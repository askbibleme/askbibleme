import dotenv from "dotenv";
dotenv.config();

import express from "express";
import fs from "fs";
import path from "path";
import dns from "node:dns/promises";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { fileURLToPath } from "url";
import crypto from "crypto";
import bcrypt from "bcryptjs";
import Database from "better-sqlite3";
import multer from "multer";
import AdmZip from "adm-zip";
import OpenAI from "openai";
import { testamentOptions } from "./src/books.js";
import {
  CHARACTER_PRESET_BY_BOOK,
  compareZhNamesByBibleRosterOrder,
} from "./src/bible-character-preset-order.js";
import {
  BUILTIN_COLOR_THEME_VARIABLES,
  BUILTIN_COLOR_THEME_VARIABLE_KEYS,
} from "./src/color-themes-builtin.js";
import {
  BIBLE_CHARACTER_PRIMARY_BOOK_BY_ZH,
  BIBLE_PRIMARY_CHARACTERS_BY_BOOK,
} from "./src/bible-primary-characters.js";
import {
  buildRelatedBookIdsByProfile,
  buildPrimaryCharacterEntriesByBook,
  resolveCharacterIdentity,
} from "./src/bible-character-identities.js";
import {
  runScenePipelineFromPublishedData,
  generateIllustrationPrompt,
  analyzeChapterForIllustration,
  buildChapterPayloadFromPublished,
  sanitizeChapterKeyPeopleArray,
  buildCharacterLockLines,
  buildCharacterLockLinesForRefSelections,
  stateStorageKey,
  defaultChapterIllustrationState,
  mergeChapterIllustrationState,
  stateFromPipelineRun,
  statureClassForSlot,
  layoutScaleHintForStature,
  resolveChapterRosterPortrait,
  sanitizeCharacterFigurePortraitSlotByZh,
} from "./src/chapter-illustration/index.js";
import {
  characterProfilesUsesSqlite,
  loadCharacterProfilesRootFromSqlite,
  saveCharacterProfilesRootToSqlite,
  migrateSeedJsonToSqliteIfEmpty,
  countCharacterProfilesInSqlite,
  getCharacterProfilesDbPathForLog,
} from "./src/character-profiles-persistence.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Render 会为所有服务设置 RENDER=true。已有 Web Service 从 Git 部署时，Blueprint（render.yaml）里的
 * env 不一定已同步到 Dashboard，会导致生产校验缺变量而 exit(1)。未配置时填入与 render.yaml
 * 及持久盘 mountPath: /var/data 一致的路径（含 DATA_ROOT，避免 admin_data 落在临时发布目录）。
 */
function applyRenderProductionEnvDefaults() {
  if (String(process.env.NODE_ENV || "").trim() !== "production") return;
  if (process.env.RENDER !== "true") return;
  const filled = [];
  if (!String(process.env.DATA_ROOT || "").trim()) {
    process.env.DATA_ROOT = "/var/data";
    filled.push("DATA_ROOT");
  }
  if (!String(process.env.CHARACTER_DATA_DIR || "").trim()) {
    process.env.CHARACTER_DATA_DIR = "/var/data/creative_runtime_data";
    filled.push("CHARACTER_DATA_DIR");
  }
  if (!String(process.env.GENERATED_ASSETS_DIR || "").trim()) {
    process.env.GENERATED_ASSETS_DIR = "/var/data/generated_png";
    filled.push("GENERATED_ASSETS_DIR");
  }
  if (filled.length) {
    console.log(
      "[render-defaults] 已补全环境变量（与 render.yaml 一致；建议在 Dashboard 显式配置）:",
      filled.join(", ")
    );
  }
}
applyRenderProductionEnvDefaults();

const DATA_ROOT = process.env.DATA_ROOT
  ? path.resolve(process.env.DATA_ROOT)
  : __dirname;

const app = express();
const SERVER_BOOT_TS = Date.now();
const SERVER_BOOT_ISO = new Date(SERVER_BOOT_TS).toISOString();
app.use(express.json({ limit: "10mb" }));

let sharpModulePromise = null;
async function getSharp() {
  if (!sharpModulePromise) {
    sharpModulePromise = import("sharp")
      .then((mod) => mod.default)
      .catch((error) => {
        sharpModulePromise = null;
        throw error;
      });
  }
  return sharpModulePromise;
}

/** 本机环回上的其它端口（如 Live Server 5500、Vite 5173）可带 Cookie/Authorization 调 Node API */
function isLoopbackBrowserOrigin(origin) {
  if (!origin || typeof origin !== "string") return false;
  try {
    const u = new URL(origin);
    const h = (u.hostname || "").toLowerCase();
    if (
      h !== "localhost" &&
      h !== "127.0.0.1" &&
      h !== "::1" &&
      h !== "[::1]"
    ) {
      return false;
    }
    if (u.protocol !== "http:" && u.protocol !== "https:") return false;
    return true;
  } catch {
    return false;
  }
}

/** 局域网 IPv4（与 listen 0.0.0.0 + 手机调试常见）：页面在 192.168.x.x:前端口、API 在 192.168.x.x:3000 时需 CORS */
function isPrivateLanBrowserOrigin(origin) {
  if (!origin || typeof origin !== "string") return false;
  try {
    const u = new URL(origin);
    if (u.protocol !== "http:" && u.protocol !== "https:") return false;
    const h = (u.hostname || "").toLowerCase();
    const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(h);
    if (!m) return false;
    const a = Number(m[1]);
    const b = Number(m[2]);
    if (![a, b, Number(m[3]), Number(m[4])].every((n) => n >= 0 && n <= 255))
      return false;
    if (a === 10) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 192 && b === 168) return true;
    return false;
  } catch {
    return false;
  }
}

function isDevCrossOriginBrowserOrigin(origin) {
  return (
    isLoopbackBrowserOrigin(origin) || isPrivateLanBrowserOrigin(origin)
  );
}

app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (isDevCrossOriginBrowserOrigin(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.append("Vary", "Origin");
    res.setHeader("Access-Control-Allow-Credentials", "true");
    res.setHeader(
      "Access-Control-Allow-Methods",
      "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS"
    );
    res.setHeader(
      "Access-Control-Allow-Headers",
      "Content-Type, Authorization, X-Device-Id"
    );
  }
  if (req.method === "OPTIONS") {
    return res.status(204).end();
  }
  next();
});

/* 仅匹配「恰好 POST /api」：常见于 nginx proxy_pass 截断子路径，子路径未到达 Node 上的具体接口 */
app.post("/api", (req, res) => {
  res.status(400).json({
    error:
      "请求路径不完整（只到了 /api）。若使用 nginx 反代 Node，请使用：location /api/ { proxy_pass http://127.0.0.1:端口/api/; }（location 与 proxy_pass 均带末尾斜杠），并 reload nginx。",
  });
});

/* APK 直链：*.apk 不进 Git；生产可把文件放到 DATA_ROOT/downloads/（Render 持久盘 /var/data） */
function resolvePublicApkPath() {
  const name = "askbible-release.apk";
  const candidates = [
    path.join(__dirname, "downloads", name),
    path.join(DATA_ROOT, "downloads", name),
  ];
  for (const p of candidates) {
    try {
      if (fs.existsSync(p) && fs.statSync(p).isFile()) return p;
    } catch {
      /* ignore */
    }
  }
  return null;
}

function sendPublicApk(req, res, next) {
  const p = resolvePublicApkPath();
  if (!p) return next();
  res.setHeader("Content-Type", "application/vnd.android.package-archive");
  res.setHeader("Content-Disposition", 'attachment; filename="AskBible.apk"');
  res.sendFile(path.resolve(p), (err) => {
    if (err) next(err);
  });
}

app.get("/downloads/askbible-release.apk", sendPublicApk);
app.head("/downloads/askbible-release.apk", sendPublicApk);

const ADMIN_DIR = path.join(DATA_ROOT, "admin_data");
const RULES_DIR = path.join(ADMIN_DIR, "rules");
const JOBS_DIR = path.join(ADMIN_DIR, "jobs");
const CONTENT_BUILDS_DIR = path.join(DATA_ROOT, "content_builds");
const CONTENT_PUBLISHED_DIR = path.join(DATA_ROOT, "content_published");
const CHAPTER_VIDEOS_DIR = path.join(DATA_ROOT, "chapter_videos");
const CHAPTER_VIDEO_UPLOAD_TMP = path.join(CHAPTER_VIDEOS_DIR, "_upload_tmp");
const MAX_CHAPTER_VIDEOS_PER_CHAPTER = 20;

const LANGUAGES_FILE = path.join(ADMIN_DIR, "languages.json");
const SCRIPTURE_VERSIONS_FILE = path.join(ADMIN_DIR, "scripture_versions.json");
const CONTENT_VERSIONS_FILE = path.join(ADMIN_DIR, "content_versions.json");
const PUBLISHED_FILE = path.join(ADMIN_DIR, "published.json");
const GLOBAL_FAVORITES_FILE = path.join(ADMIN_DIR, "global_favorites.json");
const COMMUNITY_ARTICLES_FILE = path.join(ADMIN_DIR, "community_articles.json");
const PROMO_PAGE_FILE = path.join(ADMIN_DIR, "promo_page.json");
const PROMO_PAGE_BOOTSTRAP_FILE = path.join(ADMIN_DIR, "promo_page.bootstrap.md");
const PROMO_PAGE_MAX_MARKDOWN = 400000;
const PROMO_PAGE_MAX_CUSTOM_CSS = 120000;
/** 每卷书首页介绍（Markdown），存于 content_published/<version>/<lang>/<bookId>/book_intro.json */
const BOOK_INTRO_MAX_MARKDOWN = 400000;
const SITE_CHROME_FILE = path.join(ADMIN_DIR, "site_chrome.json");
const SITE_CHROME_MAX_NAV = 16;
const SITE_CHROME_MAX_FOOTER = 8000;
/** 底栏左/中/右各栏最大字符数（总和不超过 SITE_CHROME_MAX_FOOTER 量级） */
const SITE_CHROME_MAX_FOOTER_COL = Math.floor(SITE_CHROME_MAX_FOOTER / 3);
const SITE_SEO_FILE = path.join(ADMIN_DIR, "site_seo.json");
const SITE_SEO_MAX_TITLE = 160;
const SITE_SEO_MAX_DESC = 600;
const SITE_SEO_MAX_KEYWORDS = 500;
const SITE_SEO_MAX_SHORT = 120;
const QUESTION_SUBMISSIONS_FILE = path.join(
  ADMIN_DIR,
  "question_submissions.json"
);
const QUESTION_CORRECTIONS_FILE = path.join(
  ADMIN_DIR,
  "question_text_corrections.json"
);

/**
 * 人像 + 章节仿古插画等「创作侧」运行数据根目录（持久卷，与代码发布解耦；同 question_submissions）。
 * 设置后下列 JSON 均在此目录，不再使用 admin_data 下同名文件。
 * 可选与 CHARACTER_PROFILES_DB 联用（SQLite 建议放在此目录或同卷）。
 */
const CHARACTER_DATA_DIR = process.env.CHARACTER_DATA_DIR
  ? path.resolve(process.env.CHARACTER_DATA_DIR)
  : "";

function creativeRuntimeDataPath(fileName) {
  return CHARACTER_DATA_DIR
    ? path.join(CHARACTER_DATA_DIR, fileName)
    : path.join(ADMIN_DIR, fileName);
}

const CHARACTER_ILLUSTRATION_PROFILES_FILE = creativeRuntimeDataPath(
  "character_illustration_profiles.json"
);
const CHARACTER_PROFILE_IMAGE_AUDIT_FILE = creativeRuntimeDataPath(
  "character_profile_image_audit.json"
);
const CHARACTER_STAGE_RULES_FILE = creativeRuntimeDataPath("character_stage_rules.json");
const CHAPTER_KEY_PEOPLE_FILE = creativeRuntimeDataPath("chapter_key_people.json");
const CHAPTER_ILLUSTRATION_STATE_FILE = creativeRuntimeDataPath(
  "chapter_illustration_states.json"
);
/** SQLite 空库导入用；默认同仓库内模板（与 CHARACTER_DATA_DIR 无关）。 */
const CHARACTER_PROFILES_SEED_JSON = process.env.CHARACTER_PROFILES_SEED_JSON
  ? path.resolve(process.env.CHARACTER_PROFILES_SEED_JSON)
  : path.join(__dirname, "admin_data", "character_illustration_profiles.json");

/**
 * 出图/人物立绘 PNG 与 thumbs（站点 URL 仍为 /generated/...）。
 * 设置 GENERATED_ASSETS_DIR 指向持久卷时文件不在仓库内；未设则沿用 public/generated。
 */
const CHAPTER_ILLUSTRATION_GENERATED_DIR = process.env.GENERATED_ASSETS_DIR
  ? path.resolve(process.env.GENERATED_ASSETS_DIR)
  : path.join(__dirname, "public", "generated");

const POINTS_CONFIG_FILE = path.join(ADMIN_DIR, "points_config.json");
const COLOR_THEMES_FILE = path.join(ADMIN_DIR, "color_themes.json");
const OPS_CONFIG_FILE = path.join(ADMIN_DIR, "ops_config.json");
const AUTH_DB_FILE = path.join(ADMIN_DIR, "auth.sqlite");
const ANALYTICS_DB_FILE = path.join(ADMIN_DIR, "analytics.sqlite");
/** 登录会话在服务端有效期（自登录时起算）。原 30 天；延长后减轻频繁重登。 */
const USER_SESSION_TTL_MS = 365 * 24 * 60 * 60 * 1000;
const LEGACY_USERS_FILE = path.join(ADMIN_DIR, "users.json");
const LEGACY_USER_SESSIONS_FILE = path.join(ADMIN_DIR, "user_sessions.json");
const DEPLOY_DIR = path.join(ADMIN_DIR, "deploy");
const DEPLOY_UPLOADS_DIR = path.join(DEPLOY_DIR, "uploads");
const DEPLOY_RELEASES_DIR = path.join(DEPLOY_DIR, "releases");
const DEPLOY_BACKUPS_DIR = path.join(DEPLOY_DIR, "backups");
const DEPLOY_STATE_FILE = path.join(DEPLOY_DIR, "state.json");
const DATA_BACKUPS_DIR = path.join(ADMIN_DIR, "data_backups");
const ADMIN_AUDIT_FILE = path.join(ADMIN_DIR, "admin_audit.json");
const SYSTEM_SECRETS_FILE = path.join(ADMIN_DIR, "system_secrets.json");
const DEFAULT_DATA_BACKUP_KEEP_COUNT = Math.max(1, toSafeNumber(process.env.DATA_BACKUP_KEEP_COUNT, 20));
const PUBLISH_RUN_STATE = {
  currentRunId: "",
  currentAction: "",
  cancelRequested: false,
  startedAt: "",
};
let openAiClient = null;
let openAiClientKey = "";

/* =========================================================
   基础工具
   ========================================================= */
function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function readJson(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    console.error("JSON 读取失败:", filePath, error);
    return fallback;
  }
}

function writeJson(filePath, data) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), "utf8");
}

function loadSystemSecrets() {
  const raw = readJson(SYSTEM_SECRETS_FILE, null);
  if (!raw || typeof raw !== "object") {
    return {
      openaiApiKey: "",
      remoteSyncBaseUrl: "",
      remoteSyncAdminToken: "",
    };
  }
  return {
    openaiApiKey: safeText(raw.openaiApiKey || ""),
    remoteSyncBaseUrl: safeText(raw.remoteSyncBaseUrl || ""),
    remoteSyncAdminToken: safeText(raw.remoteSyncAdminToken || ""),
  };
}

function saveSystemSecrets(next) {
  const current = loadSystemSecrets();
  const merged = {
    ...current,
    ...(next || {}),
    openaiApiKey: safeText(next?.openaiApiKey ?? current.openaiApiKey),
    remoteSyncBaseUrl: safeText(next?.remoteSyncBaseUrl ?? current.remoteSyncBaseUrl),
    remoteSyncAdminToken: safeText(next?.remoteSyncAdminToken ?? current.remoteSyncAdminToken),
  };
  writeJson(SYSTEM_SECRETS_FILE, merged);
  return merged;
}

/** 环境变量优先：线上常在 Render 等面板配置 OPENAI_API_KEY，避免后台 JSON 里过期 Key 覆盖有效配置 */
function getCurrentOpenAiApiKey() {
  const fromEnv = safeText(process.env.OPENAI_API_KEY || "");
  if (fromEnv) return fromEnv;
  return safeText(loadSystemSecrets().openaiApiKey || "");
}

function getOpenAiClient() {
  const key = getCurrentOpenAiApiKey();
  if (!key) return null;
  if (!openAiClient || openAiClientKey !== key) {
    openAiClient = new OpenAI({ apiKey: key });
    openAiClientKey = key;
  }
  return openAiClient;
}

function nextTickAsync() {
  return new Promise((resolve) => setImmediate(resolve));
}

function beginPublishRun(action) {
  const runId = `pub_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  PUBLISH_RUN_STATE.currentRunId = runId;
  PUBLISH_RUN_STATE.currentAction = safeText(action || "");
  PUBLISH_RUN_STATE.cancelRequested = false;
  PUBLISH_RUN_STATE.startedAt = nowIso();
  return runId;
}

function endPublishRun(runId) {
  if (safeText(PUBLISH_RUN_STATE.currentRunId) !== safeText(runId)) return;
  PUBLISH_RUN_STATE.currentRunId = "";
  PUBLISH_RUN_STATE.currentAction = "";
  PUBLISH_RUN_STATE.cancelRequested = false;
  PUBLISH_RUN_STATE.startedAt = "";
}

function requestCancelPublishRun() {
  if (!safeText(PUBLISH_RUN_STATE.currentRunId)) return false;
  PUBLISH_RUN_STATE.cancelRequested = true;
  return true;
}

function assertPublishRunNotCancelled(runId) {
  if (safeText(PUBLISH_RUN_STATE.currentRunId) !== safeText(runId)) {
    throw new Error("发布任务已被新的任务替换，请重试");
  }
  if (PUBLISH_RUN_STATE.cancelRequested) {
    throw new Error("发布任务已手动停止");
  }
}

function getDefaultPointsConfig() {
  return {
    naming: {
      pointName: "成长值",
      levelName: "学习等级",
      recordName: "成长记录",
      leaderboardName: "本周学习榜",
      note: "成长值用于记录学习投入，不代表属灵成熟度。",
    },
    eventLabels: {
      readChapter: "完成阅读",
      favorite: "标记重点",
      interactionClick: "学习互动",
      reply: "参与讨论",
      submitQuestion: "提出问题",
      approvedQuestion: "问题被采纳",
    },
    levels: [
      "初学者",
      "阅读者",
      "思考者",
      "提问者",
      "求索者",
      "研读者",
      "归纳者",
      "贯通者",
      "洞见者",
      "引思者",
      "学者",
      "研习导师",
    ],
  };
}

function loadPointsConfig() {
  const base = getDefaultPointsConfig();
  const loaded = readJson(POINTS_CONFIG_FILE, null);
  if (!loaded || typeof loaded !== "object") return base;
  return {
    ...base,
    ...loaded,
    naming: {
      ...base.naming,
      ...(loaded.naming || {}),
    },
    eventLabels: {
      ...base.eventLabels,
      ...(loaded.eventLabels || {}),
    },
    levels:
      Array.isArray(loaded.levels) && loaded.levels.length
        ? loaded.levels.map((x) => safeText(x)).filter(Boolean)
        : base.levels,
  };
}

function savePointsConfig(config) {
  const current = loadPointsConfig();
  const next = {
    ...current,
    ...(config || {}),
    naming: {
      ...(current.naming || {}),
      ...((config && config.naming) || {}),
    },
    eventLabels: {
      ...(current.eventLabels || {}),
      ...((config && config.eventLabels) || {}),
    },
    levels:
      Array.isArray(config?.levels) && config.levels.length
        ? config.levels.map((x) => safeText(x)).filter(Boolean)
        : current.levels,
  };
  writeJson(POINTS_CONFIG_FILE, next);
  return next;
}

function getDefaultOpsConfig() {
  return {
    dataBackupKeepCount: DEFAULT_DATA_BACKUP_KEEP_COUNT,
    autoBackupEnabled: false,
    autoBackupHour: 3,
    autoBackupMinute: 0,
    lastAutoBackupDate: "",
  };
}

function loadOpsConfig() {
  const base = getDefaultOpsConfig();
  const loaded = readJson(OPS_CONFIG_FILE, null);
  if (!loaded || typeof loaded !== "object") return base;
  return {
    ...base,
    ...loaded,
    dataBackupKeepCount: Math.max(
      1,
      Math.min(200, toSafeNumber(loaded.dataBackupKeepCount, base.dataBackupKeepCount))
    ),
    autoBackupEnabled: Boolean(loaded.autoBackupEnabled),
    autoBackupHour: Math.max(0, Math.min(23, toSafeNumber(loaded.autoBackupHour, base.autoBackupHour))),
    autoBackupMinute: Math.max(
      0,
      Math.min(59, toSafeNumber(loaded.autoBackupMinute, base.autoBackupMinute))
    ),
    lastAutoBackupDate: safeText(loaded.lastAutoBackupDate || ""),
  };
}

function saveOpsConfig(config) {
  const current = loadOpsConfig();
  const next = {
    ...current,
    ...(config || {}),
    dataBackupKeepCount: Math.max(
      1,
      Math.min(200, toSafeNumber(config?.dataBackupKeepCount, current.dataBackupKeepCount))
    ),
    autoBackupEnabled:
      typeof config?.autoBackupEnabled === "boolean"
        ? config.autoBackupEnabled
        : current.autoBackupEnabled,
    autoBackupHour: Math.max(
      0,
      Math.min(23, toSafeNumber(config?.autoBackupHour, current.autoBackupHour))
    ),
    autoBackupMinute: Math.max(
      0,
      Math.min(59, toSafeNumber(config?.autoBackupMinute, current.autoBackupMinute))
    ),
    lastAutoBackupDate: safeText(config?.lastAutoBackupDate ?? current.lastAutoBackupDate),
  };
  writeJson(OPS_CONFIG_FILE, next);
  return next;
}

function safeText(value) {
  return String(value ?? "").trim();
}

function isNonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function toSafeBool(value, fallback = false) {
  if (typeof value === "boolean") return value;
  return fallback;
}

function toSafeNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

/** 章节视频等接口用：0 表示卷首页，须与 `!chapter` 区分 */
function parseNonNegativeChapterInt(value) {
  if (value === undefined || value === null) return null;
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw === undefined || raw === null) return null;
  if (typeof raw === "string" && raw.trim() === "") return null;
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n) || n < 0) return null;
  return n;
}

/** multipart 首段 JSON，避免大文件后的文本字段被反代/解析丢弃（卷首页 chapter=0 曾因此报缺参） */
function parseUploadMetaJson(req) {
  const raw = req.body?.meta;
  if (raw == null || raw === "") return null;
  try {
    const s = typeof raw === "string" ? raw : String(raw);
    const o = JSON.parse(s);
    return o && typeof o === "object" ? o : null;
  } catch {
    return null;
  }
}

function pickStrPreferFlat(flat, fromMeta) {
  const a = safeText(flat ?? "");
  if (a) return a;
  return safeText(fromMeta ?? "");
}

function pickChapterRawForUpload(req, metaObj) {
  const b = req.body?.chapter;
  if (b !== undefined && b !== null && String(b).trim() !== "") return b;
  if (metaObj && Object.prototype.hasOwnProperty.call(metaObj, "chapter")) {
    return metaObj.chapter;
  }
  return undefined;
}

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const READ_CACHE_TTL_MS = 60 * 1000;
const readApiCache = new Map();

/** 查经 JSON 读缓存键前缀；全局章人物表等变更时请 bump，避免旧缓存章末人物错位 */
const STUDY_CONTENT_CACHE_TAG = "study:v8";
const writeRateLimitMap = new Map();
const writeDedupeMap = new Map();

function getReadCache(key) {
  const hit = readApiCache.get(key);
  if (!hit) return null;
  if (Date.now() > Number(hit.expiresAt || 0)) {
    readApiCache.delete(key);
    return null;
  }
  return hit.value;
}

function setReadCache(key, value, ttlMs = READ_CACHE_TTL_MS) {
  readApiCache.set(key, {
    value,
    expiresAt: Date.now() + Math.max(1000, Number(ttlMs || READ_CACHE_TTL_MS)),
  });
}

function clearReadCacheByPrefix(prefix) {
  for (const key of readApiCache.keys()) {
    if (String(key).startsWith(prefix)) {
      readApiCache.delete(key);
    }
  }
}

function getClientIp(req) {
  if (!req) return "system";
  const xff = String(req.headers["x-forwarded-for"] || "")
    .split(",")[0]
    .trim();
  return xff || req.ip || req.socket?.remoteAddress || "unknown";
}

function sha256Hex(raw) {
  return crypto.createHash("sha256").update(String(raw || "")).digest("hex");
}

function checkWriteRateLimit({ req, actionKey, limit, windowMs }) {
  const ip = getClientIp(req);
  const key = `${actionKey}:${ip}`;
  const now = Date.now();
  const hit = writeRateLimitMap.get(key) || { count: 0, resetAt: now + windowMs };
  if (now > Number(hit.resetAt || 0)) {
    hit.count = 0;
    hit.resetAt = now + windowMs;
  }
  hit.count += 1;
  writeRateLimitMap.set(key, hit);
  if (hit.count > limit) {
    return {
      ok: false,
      retryAfterSec: Math.max(1, Math.ceil((hit.resetAt - now) / 1000)),
    };
  }
  return { ok: true, retryAfterSec: 0 };
}

function checkWriteDedupe({ dedupeKey, ttlMs }) {
  const now = Date.now();
  const prevAt = Number(writeDedupeMap.get(dedupeKey) || 0);
  if (prevAt > 0 && now - prevAt < ttlMs) {
    return false;
  }
  writeDedupeMap.set(dedupeKey, now);
  return true;
}

ensureDir(ADMIN_DIR);
ensureDir(RULES_DIR);
ensureDir(JOBS_DIR);
ensureDir(CONTENT_BUILDS_DIR);
ensureDir(CONTENT_PUBLISHED_DIR);
ensureDir(DEPLOY_DIR);
ensureDir(DEPLOY_UPLOADS_DIR);
ensureDir(DEPLOY_RELEASES_DIR);
ensureDir(DEPLOY_BACKUPS_DIR);
ensureDir(DATA_BACKUPS_DIR);

const authDb = new Database(AUTH_DB_FILE);
authDb.pragma("journal_mode = WAL");
authDb.pragma("foreign_keys = ON");
authDb.exec(`
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS user_sessions (
  token TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT,
  ip_hash TEXT,
  user_agent TEXT,
  device_id TEXT,
  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id ON user_sessions(user_id);
`);

function ensureAuthDbColumns() {
  const userCols = authDb
    .prepare("PRAGMA table_info(users)")
    .all()
    .map((x) => String(x.name || ""));
  if (!userCols.includes("is_admin")) {
    authDb.exec("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0");
  }
  if (!userCols.includes("admin_role")) {
    authDb.exec("ALTER TABLE users ADD COLUMN admin_role TEXT NOT NULL DEFAULT ''");
  }
  if (!userCols.includes("online_seconds_total")) {
    authDb.exec("ALTER TABLE users ADD COLUMN online_seconds_total INTEGER NOT NULL DEFAULT 0");
  }
  if (!userCols.includes("color_theme_id")) {
    authDb.exec(
      "ALTER TABLE users ADD COLUMN color_theme_id TEXT NOT NULL DEFAULT ''"
    );
  }
  authDb.exec(
    "UPDATE users SET admin_role = 'qianfuzhang' WHERE is_admin = 1 AND (admin_role IS NULL OR admin_role = '')"
  );
  const cols = authDb
    .prepare("PRAGMA table_info(user_sessions)")
    .all()
    .map((x) => String(x.name || ""));
  if (!cols.includes("ip_hash")) {
    authDb.exec("ALTER TABLE user_sessions ADD COLUMN ip_hash TEXT");
  }
  if (!cols.includes("user_agent")) {
    authDb.exec("ALTER TABLE user_sessions ADD COLUMN user_agent TEXT");
  }
  if (!cols.includes("device_id")) {
    authDb.exec("ALTER TABLE user_sessions ADD COLUMN device_id TEXT");
  }
}

ensureAuthDbColumns();

const ADMIN_ROLE_LEVEL = {
  shifuzhang: 1,
  baifuzhang: 2,
  qianfuzhang: 3,
};

const PERMISSION_ROLE_MIN_LEVEL = {
  review_questions: 1,
  manage_publish: 2,
  manage_points: 2,
  manage_deploy: 3,
  manage_roles: 3,
  manage_rules: 3,
  edit_approved_question_text: 3,
};

function normalizeAdminRole(role) {
  const safe = safeText(role || "").toLowerCase();
  if (safe in ADMIN_ROLE_LEVEL) return safe;
  return "";
}

function hasPermission(authedUser, permissionKey) {
  const strictKeys = new Set([
    "review_questions",
    "manage_roles",
    "edit_approved_question_text",
  ]);
  if (!strictKeys.has(permissionKey)) {
    // Keep existing admin tools usable while RBAC is rolling out.
    return true;
  }
  if (!authedUser) return false;
  const role = normalizeAdminRole(authedUser.adminRole || "");
  if (!role) return false;
  const needLevel = Number(PERMISSION_ROLE_MIN_LEVEL[permissionKey] || 999);
  const level = Number(ADMIN_ROLE_LEVEL[role] || 0);
  return level >= needLevel;
}

function requirePermission(req, res, permissionKey) {
  const authed = getAuthedUserFromReq(req);
  const strictKeys = new Set([
    "review_questions",
    "manage_roles",
    "edit_approved_question_text",
  ]);
  if (!strictKeys.has(permissionKey)) {
    return authed || { id: "", name: "", email: "", adminRole: "" };
  }
  if (!authed) {
    res.status(401).json({ error: "请先登录" });
    return null;
  }
  if (!hasPermission(authed, permissionKey)) {
    res.status(403).json({ error: "权限不足" });
    return null;
  }
  return authed;
}

function migrateLegacyAuthJsonIfNeeded() {
  const usersCount = authDb.prepare("SELECT COUNT(1) as c FROM users").get()?.c || 0;
  if (Number(usersCount) > 0) return;
  const usersLegacy = readJson(LEGACY_USERS_FILE, { users: [] })?.users || [];
  if (!Array.isArray(usersLegacy) || !usersLegacy.length) return;

  const insertUser = authDb.prepare(
    "INSERT OR IGNORE INTO users (id, name, email, password_hash, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)"
  );
  for (const user of usersLegacy) {
    insertUser.run(
      safeText(user.id || `u_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`),
      safeText(user.name || "user"),
      safeText(user.email || "").toLowerCase(),
      safeText(user.passwordHash || ""),
      safeText(user.createdAt || nowIso()),
      safeText(user.updatedAt || "")
    );
  }

  const sessionsLegacy =
    readJson(LEGACY_USER_SESSIONS_FILE, { sessions: [] })?.sessions || [];
  if (Array.isArray(sessionsLegacy) && sessionsLegacy.length) {
    const insertSession = authDb.prepare(
      "INSERT OR IGNORE INTO user_sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)"
    );
    for (const s of sessionsLegacy) {
      insertSession.run(
        safeText(s.token || ""),
        safeText(s.userId || ""),
        safeText(s.createdAt || nowIso()),
        new Date(Date.now() + USER_SESSION_TTL_MS).toISOString()
      );
    }
  }
}

migrateLegacyAuthJsonIfNeeded();

const ANALYTICS_VISITOR_ID_MAX = 128;
const ANALYTICS_HB_SEC_ESTIMATE = 45;
const ANALYTICS_PV_SEC_BUMP = 3;

function analyticsDayKeyLocal(d = new Date()) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const da = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${da}`;
}

let analyticsDb = null;
function getAnalyticsDb() {
  if (analyticsDb) return analyticsDb;
  ensureDir(ADMIN_DIR);
  analyticsDb = new Database(ANALYTICS_DB_FILE);
  analyticsDb.pragma("journal_mode = WAL");
  analyticsDb.exec(`
    CREATE TABLE IF NOT EXISTS analytics_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      day TEXT NOT NULL,
      visitor_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      ts INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_analytics_day ON analytics_events(day);
    CREATE INDEX IF NOT EXISTS idx_analytics_day_visitor ON analytics_events(day, visitor_id);
  `);
  return analyticsDb;
}

function normalizeAnalyticsVisitorId(raw) {
  const s = safeText(raw || "").slice(0, ANALYTICS_VISITOR_ID_MAX);
  if (s.length < 4) return "";
  return s;
}

function normalizeAnalyticsKind(raw) {
  const k = safeText(raw || "").toLowerCase();
  if (k === "pv" || k === "hb") return k;
  return "";
}

/** 公开：页面浏览 / 心跳采集（无鉴权；体量由 SQLite 本地存储） */
function handleAnalyticsCollectPost(req, res) {
  try {
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const visitorId = normalizeAnalyticsVisitorId(body.visitorId);
    const kind = normalizeAnalyticsKind(body.kind);
    if (!visitorId || !kind) {
      res.status(400).json({ error: "invalid payload" });
      return;
    }
    const db = getAnalyticsDb();
    const day = analyticsDayKeyLocal();
    const ts = Date.now();
    db.prepare(
      "INSERT INTO analytics_events (day, visitor_id, kind, ts) VALUES (?,?,?,?)"
    ).run(day, visitorId, kind, ts);
    res.set("Cache-Control", "no-store");
    res.json({ ok: true });
  } catch (e) {
    console.error("[analytics/collect]", e);
    res.status(500).json({ error: "collect failed" });
  }
}

/** 管理员：访问统计汇总（与 admin-analytics.html 约定字段一致） */
function handleAdminAnalyticsOverviewGet(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const db = getAnalyticsDb();
    const now = new Date();
    const today = analyticsDayKeyLocal(now);
    const start = analyticsDayKeyLocal(
      new Date(now.getFullYear(), now.getMonth(), now.getDate() - 13)
    );
    let tz = "";
    try {
      tz = Intl.DateTimeFormat().resolvedOptions().timeZone || "";
    } catch {
      tz = "";
    }
    const rows = db
      .prepare(
        `SELECT day AS day,
          COUNT(DISTINCT visitor_id) AS uv,
          SUM(CASE WHEN kind = 'pv' THEN 1 ELSE 0 END) AS pv,
          SUM(CASE WHEN kind = 'hb' THEN 1 ELSE 0 END) AS hb
        FROM analytics_events
        WHERE day >= ?
        GROUP BY day`
      )
      .all(start);
    const byDay = new Map(
      rows.map((r) => [
        String(r.day),
        {
          uv: Number(r.uv || 0),
          pv: Number(r.pv || 0),
          hb: Number(r.hb || 0),
        },
      ])
    );
    const daily = [];
    for (let i = 13; i >= 0; i--) {
      const d = new Date(
        now.getFullYear(),
        now.getMonth(),
        now.getDate() - i
      );
      const day = analyticsDayKeyLocal(d);
      const r = byDay.get(day) || { uv: 0, pv: 0, hb: 0 };
      daily.push({
        date: day,
        uniqueVisitors: r.uv,
        pageViews: r.pv,
        onlineSeconds:
          r.hb * ANALYTICS_HB_SEC_ESTIMATE + r.pv * ANALYTICS_PV_SEC_BUMP,
      });
    }
    const t = byDay.get(today) || { uv: 0, pv: 0, hb: 0 };
    const registeredUsers =
      authDb.prepare("SELECT COUNT(1) AS c FROM users").get()?.c ?? 0;
    res.set("Cache-Control", "no-store");
    res.json({
      ok: true,
      serverTime: now.toISOString(),
      serverBootIso: SERVER_BOOT_ISO,
      timezone: tz,
      registeredUsers: Number(registeredUsers) || 0,
      today: {
        date: today,
        uniqueVisitors: t.uv,
        pageViews: t.pv,
        onlineSeconds:
          t.hb * ANALYTICS_HB_SEC_ESTIMATE + t.pv * ANALYTICS_PV_SEC_BUMP,
      },
      daily,
    });
  } catch (e) {
    console.error("[admin/analytics/overview]", e);
    res.status(500).json({ error: e.message || "统计读取失败" });
  }
}

/** 管理员：用户列表（用于统计页报告） */
function handleAdminAnalyticsUsersGet(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const rawLimit = Number(req.query?.limit || 50);
    const limit = Math.max(1, Math.min(200, Number.isFinite(rawLimit) ? Math.floor(rawLimit) : 50));
    const rows = authDb
      .prepare(
        `SELECT id, name, email, is_admin, admin_role, created_at, updated_at,
                COALESCE(online_seconds_total, 0) AS online_seconds_total
         FROM users
         ORDER BY created_at DESC
         LIMIT ?`
      )
      .all(limit);
    const users = (Array.isArray(rows) ? rows : []).map((r) => ({
      id: String(r?.id || ""),
      name: String(r?.name || ""),
      email: String(r?.email || ""),
      isAdmin: Number(r?.is_admin || 0) === 1,
      adminRole: String(r?.admin_role || ""),
      onlineSecondsTotal: Number(r?.online_seconds_total || 0),
      createdAt: String(r?.created_at || ""),
      updatedAt: String(r?.updated_at || ""),
    }));
    res.set("Cache-Control", "no-store");
    res.json({ ok: true, users });
  } catch (e) {
    console.error("[admin/analytics/users]", e);
    res.status(500).json({ error: e.message || "用户列表读取失败" });
  }
}

const upload = multer({
  dest: DEPLOY_UPLOADS_DIR,
  limits: { fileSize: 1024 * 1024 * 200 },
});

const CHAPTER_VIDEO_MAX_BYTES = 250 * 1024 * 1024;

const chapterVideoMulter = multer({
  dest: CHAPTER_VIDEO_UPLOAD_TMP,
  limits: { fileSize: CHAPTER_VIDEO_MAX_BYTES },
  fileFilter: (_req, file, cb) => {
    const ok =
      file.mimetype === "video/mp4" || file.mimetype === "video/webm";
    cb(null, ok);
  },
});

const chapterVideoPosterMulter = multer({
  dest: CHAPTER_VIDEO_UPLOAD_TMP,
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ok = /^image\/(jpeg|png|webp)$/i.test(String(file.mimetype || ""));
    cb(null, ok);
  },
});

/** 章节插图 / 人物透明参考图：归一化为 PNG 写入 CHAPTER_ILLUSTRATION_GENERATED_DIR（/generated/ URL 不变） */
const illustrationImageUploadMulter = multer({
  dest: CHAPTER_VIDEO_UPLOAD_TMP,
  limits: { fileSize: 25 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const ok = /^image\/(jpeg|png|webp|gif)$/i.test(String(file.mimetype || ""));
    cb(null, ok);
  },
});

function illustrationImageUploadMiddleware(req, res, next) {
  try {
    fs.mkdirSync(CHAPTER_VIDEO_UPLOAD_TMP, { recursive: true });
  } catch {
    /* ignore */
  }
  illustrationImageUploadMulter.single("file")(req, res, next);
}

const READER_IMAGE_MAX_EDGE = 960;
const READER_IMAGE_MAX_EDGE_LIMIT = 1280;

/* =========================================================
   配置读取
   ========================================================= */
function loadLanguages() {
  return readJson(LANGUAGES_FILE, { languages: [] });
}

function loadScriptureVersions() {
  return readJson(SCRIPTURE_VERSIONS_FILE, { scriptureVersions: [] });
}

function saveScriptureVersions(data) {
  writeJson(SCRIPTURE_VERSIONS_FILE, data);
}

function loadContentVersions() {
  return readJson(CONTENT_VERSIONS_FILE, { contentVersions: [] });
}

function saveContentVersions(data) {
  writeJson(CONTENT_VERSIONS_FILE, data);
}

/** 前台读经菜单「版本」里展示的条目：enabled 且 showInMenu 未显式为 false */
function getMenuContentVersions() {
  return (loadContentVersions().contentVersions || [])
    .filter((x) => x && x.enabled !== false && x.showInMenu !== false)
    .sort((a, b) => Number(a.order || 999) - Number(b.order || 999));
}

function loadPublished() {
  return readJson(PUBLISHED_FILE, {});
}

function savePublished(published) {
  writeJson(PUBLISHED_FILE, published);
}

function loadGlobalFavorites() {
  const data = readJson(GLOBAL_FAVORITES_FILE, null);
  if (!data || typeof data !== "object") {
    return { items: {} };
  }
  if (!data.items || typeof data.items !== "object") {
    data.items = {};
  }
  return data;
}

function saveGlobalFavorites(data) {
  writeJson(GLOBAL_FAVORITES_FILE, data);
}

const PUBLISHABLE_ARTICLE_COLUMNS = new Set(["communityArticles"]);
const MAX_COMMUNITY_ARTICLES = 800;

function loadCommunityArticles() {
  const data = readJson(COMMUNITY_ARTICLES_FILE, null);
  if (!data || typeof data !== "object") {
    return { items: [] };
  }
  if (!Array.isArray(data.items)) data.items = [];
  return data;
}

function saveCommunityArticles(data) {
  writeJson(COMMUNITY_ARTICLES_FILE, {
    items: Array.isArray(data.items) ? data.items : [],
  });
}

function loadPromoPagePayload() {
  const data = readJson(PROMO_PAGE_FILE, null);
  if (data && typeof data === "object" && typeof data.markdown === "string") {
    return {
      markdown: data.markdown,
      customCss:
        typeof data.customCss === "string" ? data.customCss : "",
      updatedAt: safeText(data.updatedAt || ""),
    };
  }
  let bootstrap = "";
  try {
    if (fs.existsSync(PROMO_PAGE_BOOTSTRAP_FILE)) {
      bootstrap = fs.readFileSync(PROMO_PAGE_BOOTSTRAP_FILE, "utf8");
    }
  } catch (error) {
    console.error("读取 promo_page.bootstrap.md 失败:", error);
  }
  return { markdown: bootstrap, customCss: "", updatedAt: "" };
}

function writePromoPageRecord(record) {
  const payload = {
    markdown: String(record.markdown ?? "").slice(0, PROMO_PAGE_MAX_MARKDOWN),
    customCss: String(record.customCss ?? "").slice(
      0,
      PROMO_PAGE_MAX_CUSTOM_CSS
    ),
    updatedAt: nowIso(),
  };
  writeJson(PROMO_PAGE_FILE, payload);
  return payload;
}

function getDefaultSiteChrome() {
  return {
    updatedAt: "",
    topbar: {
      /** 横版透明字标（SVG），与仓库 assets/brand/askbible-wordmark.svg 一致；留空则由顶栏回退为文字品牌 */
      logoUrl: "/assets/brand/askbible-wordmark.svg",
      logoHeight: 36,
      homeHref: "/",
      brandTitleAttr: "AskBible.me 首页",
      showSplitBrand: false,
      brandAsk: "Ask",
      brandBible: "Bible",
      brandMe: ".me",
      brandPlainTitle: "",
      brandSubtitle: "",
      brandSubtitleShow: true,
      brandSubtitleDismissible: false,
      brandSubtitleInline: false,
      /** 为 true 时全站顶栏 position:sticky 吸顶，正文从下方滑过 */
      topbarSticky: false,
      navLinks: [
        { href: "/", label: "读经", icon: "home", iconOnly: true },
        {
          href: "/#openBookChapter",
          label: "书卷",
          ariaLabel: "选择书卷与章节",
          icon: "book",
          iconOnly: true,
        },
        {
          href: "/#openVerseSearch",
          label: "搜索",
          icon: "search",
          iconOnly: true,
        },
        {
          href: "/#openSharePage",
          label: "分享",
          ariaLabel: "分享当前页面",
          icon: "share",
          iconOnly: true,
        },
        {
          href: "/#fontSmaller",
          label: "-",
          ariaLabel: "缩小字号",
          icon: "minus",
          iconOnly: true,
        },
        {
          href: "/#fontLarger",
          label: "+",
          ariaLabel: "放大字号",
          icon: "plus",
          iconOnly: true,
        },
        { href: "/why.html", label: "介绍", icon: "doc", iconOnly: true },
        { href: "/#openMemberHub", label: "会员", icon: "user", iconOnly: true },
      ],
    },
    footer: {
      enabled: false,
      text: "",
      left: "",
      center: "",
      right: "",
    },
  };
}

function normalizeSiteChromeFooter(foot) {
  const def = getDefaultSiteChrome().footer;
  if (!foot || typeof foot !== "object") {
    return { ...def };
  }
  const enabled = foot.enabled === true;
  const left = safeText(foot.left ?? "").slice(0, SITE_CHROME_MAX_FOOTER_COL);
  const center = safeText(foot.center ?? "").slice(0, SITE_CHROME_MAX_FOOTER_COL);
  const right = safeText(foot.right ?? "").slice(0, SITE_CHROME_MAX_FOOTER_COL);
  const legacyText = String(foot.text ?? "").trim().slice(0, SITE_CHROME_MAX_FOOTER);
  if (!left && !center && !right && legacyText) {
    return {
      enabled,
      text: legacyText,
      left: "",
      center: legacyText.slice(0, SITE_CHROME_MAX_FOOTER_COL),
      right: "",
    };
  }
  const textMirror = [left, center, right]
    .map((s) => String(s || "").trim())
    .filter(Boolean)
    .join("\n")
    .slice(0, SITE_CHROME_MAX_FOOTER);
  return {
    enabled,
    left,
    center,
    right,
    text:
      textMirror || String(foot.text ?? "").slice(0, SITE_CHROME_MAX_FOOTER),
  };
}

function isSafeSiteChromeHref(href) {
  const h = String(href || "").trim();
  if (!h || h.length > 400) return false;
  /* 同页动作（如 #openSharePage），不经由外链 */
  if (/^#[A-Za-z_][A-Za-z0-9_:.+-]*$/.test(h)) return true;
  if (h.startsWith("/") && !h.startsWith("//")) return true;
  if (/^https?:\/\//i.test(h)) {
    try {
      const u = new URL(h);
      return u.protocol === "http:" || u.protocol === "https:";
    } catch {
      return false;
    }
  }
  return false;
}

function isSafeSiteChromeLogoUrl(url) {
  const s = String(url || "").trim();
  if (!s || s.length > 600) return false;
  if (s.startsWith("/") && !s.startsWith("//")) return true;
  if (/^https?:\/\//i.test(s)) {
    try {
      const u = new URL(s);
      return u.protocol === "http:" || u.protocol === "https:";
    } catch {
      return false;
    }
  }
  return false;
}

const SITE_CHROME_NAV_ICON_IDS = new Set([
  "user",
  "home",
  "book",
  "book_open",
  "info",
  "settings",
  "search",
  "minus",
  "plus",
  "heart",
  "mail",
  "link",
  "map",
  "calendar",
  "phone",
  "doc",
  "star",
  "play",
  "share",
]);

/** 与 site-chrome.js 内 askBibleChromeNavIconHtml 别名一致，避免旧数据或误填 id 导致 icon 被清空、仅图标被压成 false */
const SITE_CHROME_NAV_ICON_ALIASES = {
  document: "doc",
  file: "doc",
  page: "doc",
  house: "home",
  person: "user",
};

function normalizeSiteChromeNavIcon(raw) {
  let s = safeText(raw || "").toLowerCase().replace(/[^a-z0-9_]/g, "");
  const mapped = SITE_CHROME_NAV_ICON_ALIASES[s];
  if (mapped) s = mapped;
  if (SITE_CHROME_NAV_ICON_IDS.has(s)) return s;
  return "";
}

function normalizeSiteChromeNavIconOnly(raw) {
  if (raw === true || raw === 1) return true;
  const s = String(raw ?? "")
    .trim()
    .toLowerCase();
  return (
    s === "true" ||
    s === "1" ||
    s === "yes" ||
    s === "on"
  );
}

/** 顶栏开关等：兼容磁盘/请求体里的字符串 "true" */
function readSiteChromeBool(v) {
  if (v === true || v === 1) return true;
  if (v === false || v === 0) return false;
  const s = String(v ?? "")
    .trim()
    .toLowerCase();
  if (s === "true" || s === "1" || s === "yes" || s === "on") return true;
  if (s === "false" || s === "0" || s === "no" || s === "off") return false;
  return false;
}

function readNavLinkIconOnlyFlag(x) {
  if (!x || typeof x !== "object") return false;
  const v = x.iconOnly != null ? x.iconOnly : x.icon_only;
  return normalizeSiteChromeNavIconOnly(v);
}

/** 磁盘上旧数据只有 href/label 时，按 href 用默认顶栏补 icon / iconOnly，避免线上一直显示纯文字 */
function navLinkHasExplicitIconOnly(x) {
  if (!x || typeof x !== "object") return false;
  return Object.prototype.hasOwnProperty.call(x, "iconOnly") ||
    Object.prototype.hasOwnProperty.call(x, "icon_only");
}

function normalizeSiteChromeNavLinks(rawList, fallback) {
  const base = fallback || getDefaultSiteChrome().topbar.navLinks;
  const defByHref = new Map(
    base.map((l) => [String(l.href || "").trim(), l])
  );
  const row = (b) => {
    const icon = normalizeSiteChromeNavIcon(b?.icon);
    const iconOnly = readNavLinkIconOnlyFlag(b) && icon !== "";
    return {
      href: b.href,
      label: b.label,
      ariaLabel: safeText(b?.ariaLabel || "").slice(0, 120),
      icon,
      iconOnly,
    };
  };
  const withIcon = (rows) => rows.map((b) => row(b));
  if (!Array.isArray(rawList)) return withIcon(base.slice());
  const out = rawList
    .slice(0, SITE_CHROME_MAX_NAV)
    .map((x) => {
      const href = isSafeSiteChromeHref(x?.href) ? String(x.href).trim() : "";
      const defRow = href ? defByHref.get(href) : undefined;
      const hasExplicitIcon =
        x &&
        typeof x === "object" &&
        x.icon != null &&
        String(x.icon).trim() !== "";
      const iconSource = hasExplicitIcon ? x.icon : defRow?.icon;
      const icon = normalizeSiteChromeNavIcon(iconSource);
      const iconOnlySource = navLinkHasExplicitIconOnly(x)
        ? x.iconOnly != null
          ? x.iconOnly
          : x.icon_only
        : defRow != null
          ? defRow.iconOnly != null
            ? defRow.iconOnly
            : defRow.icon_only
          : false;
      const iconOnly = normalizeSiteChromeNavIconOnly(iconOnlySource) && icon !== "";
      const labelFromX = safeText(x?.label || "").slice(0, 80);
      const label =
        labelFromX ||
        (defRow ? safeText(defRow.label || "").slice(0, 80) : "");
      let ariaLabel = "";
      if (
        x &&
        typeof x === "object" &&
        x.ariaLabel != null &&
        String(x.ariaLabel).trim() !== ""
      ) {
        ariaLabel = safeText(x.ariaLabel).slice(0, 120);
      } else if (
        defRow &&
        defRow.ariaLabel != null &&
        String(defRow.ariaLabel).trim() !== ""
      ) {
        ariaLabel = safeText(defRow.ariaLabel).slice(0, 120);
      }
      return {
        href,
        label,
        ariaLabel,
        icon,
        iconOnly,
      };
    })
    .filter((x) => x.href && x.label);
  return out.length ? out : withIcon(base.slice());
}

function loadSiteChrome() {
  const def = getDefaultSiteChrome();
  const data = readJson(SITE_CHROME_FILE, null);
  if (!data || typeof data !== "object") return def;
  const top = data.topbar && typeof data.topbar === "object" ? data.topbar : {};
  const footRaw = data.footer && typeof data.footer === "object" ? data.footer : {};
  const foot = normalizeSiteChromeFooter(footRaw);
  const logoUrl = isSafeSiteChromeLogoUrl(top.logoUrl)
    ? String(top.logoUrl).trim()
    : "";
  const logoHeight = Math.max(
    20,
    Math.min(80, toSafeNumber(top.logoHeight, def.topbar.logoHeight))
  );
  const homeHref = isSafeSiteChromeHref(top.homeHref)
    ? String(top.homeHref).trim()
    : def.topbar.homeHref;
  const navLinks = normalizeSiteChromeNavLinks(top.navLinks, def.topbar.navLinks);
  return {
    updatedAt: safeText(data.updatedAt || ""),
    topbar: {
      logoUrl,
      logoHeight,
      homeHref,
      brandTitleAttr: safeText(
        top.brandTitleAttr || def.topbar.brandTitleAttr
      ).slice(0, 120),
      showSplitBrand: top.showSplitBrand !== false,
      brandAsk: safeText(top.brandAsk || def.topbar.brandAsk).slice(0, 32),
      brandBible: safeText(top.brandBible || def.topbar.brandBible).slice(0, 32),
      brandMe: safeText(top.brandMe || def.topbar.brandMe).slice(0, 32),
      brandPlainTitle: safeText(top.brandPlainTitle || "").slice(0, 120),
      brandSubtitle: safeText(top.brandSubtitle || "").slice(0, 120),
      brandSubtitleShow: top.brandSubtitleShow !== false,
      brandSubtitleDismissible: top.brandSubtitleDismissible === true,
      brandSubtitleInline: top.brandSubtitleInline === true,
      topbarSticky: readSiteChromeBool(top.topbarSticky),
      navLinks,
    },
    footer: foot,
  };
}

function saveSiteChromeFromBody(body) {
  const def = getDefaultSiteChrome();
  /** 与磁盘当前配置合并：避免请求体缺字段（旧前端、缓存页、反代剥键）把副标题等清空 */
  const existing = loadSiteChrome();
  const rawBody = body && typeof body === "object" && !Array.isArray(body) ? body : {};
  const rawTop = rawBody.topbar;
  const incomingTop =
    rawTop && typeof rawTop === "object" && !Array.isArray(rawTop)
      ? { ...rawTop }
      : {};
  /** 根级备用：缓存页脚本未把开关放进 topbar 时仍能写入 */
  if ("topbarSticky" in rawBody) {
    incomingTop.topbarSticky = rawBody.topbarSticky;
  }
  const incomingFoot =
    rawBody.footer && typeof rawBody.footer === "object" && !Array.isArray(rawBody.footer)
      ? rawBody.footer
      : {};
  const top = { ...existing.topbar, ...incomingTop };
  const footMerged = { ...existing.footer, ...incomingFoot };
  const foot = normalizeSiteChromeFooter(footMerged);
  const payload = {
    updatedAt: nowIso(),
    topbar: {
      logoUrl: isSafeSiteChromeLogoUrl(top.logoUrl)
        ? String(top.logoUrl).trim()
        : "",
      logoHeight: Math.max(20, Math.min(80, toSafeNumber(top.logoHeight, 36))),
      homeHref: isSafeSiteChromeHref(top.homeHref)
        ? String(top.homeHref).trim()
        : "/",
      brandTitleAttr: safeText(
        top.brandTitleAttr || def.topbar.brandTitleAttr
      ).slice(0, 120),
      showSplitBrand: top.showSplitBrand !== false,
      brandAsk: safeText(top.brandAsk || "Ask").slice(0, 32),
      brandBible: safeText(top.brandBible || "Bible").slice(0, 32),
      brandMe: safeText(top.brandMe || ".me").slice(0, 32),
      brandPlainTitle: safeText(top.brandPlainTitle || "").slice(0, 120),
      brandSubtitle: safeText(top.brandSubtitle || "").slice(0, 120),
      brandSubtitleShow: top.brandSubtitleShow !== false,
      brandSubtitleDismissible: top.brandSubtitleDismissible === true,
      brandSubtitleInline: top.brandSubtitleInline === true,
      topbarSticky: readSiteChromeBool(top.topbarSticky),
      navLinks: normalizeSiteChromeNavLinks(top.navLinks, def.topbar.navLinks),
    },
    footer: {
      enabled: foot.enabled === true,
      left: foot.left,
      center: foot.center,
      right: foot.right,
      text: foot.text,
    },
  };
  writeJson(SITE_CHROME_FILE, payload);
  return loadSiteChrome();
}

function getDefaultSiteSeoPage(which) {
  if (which === "promo") {
    return {
      documentTitle: "AskBible.me｜宣传页",
      metaDescription: "AskBible.me 宣传页。",
      metaKeywords:
        "读经平台,查经软件,查经工具,在线圣经,圣经阅读,小组聚会,小组查经,灵修笔记,AskBible",
      ogTitle: "AskBible.me｜宣传页",
      ogDescription: "AskBible.me 宣传页。",
      twitterTitle: "AskBible.me｜宣传页",
      twitterDescription: "AskBible.me 宣传页。",
      appleMobileWebAppTitle: "AskBible",
      ogSiteName: "AskBible.me",
      jsonLdWebsiteDescription:
        "在线读经平台与查经工具，支持书页式阅读、问题式查经与小组聚会场景下的经文讨论。",
      jsonLdSoftwareDescription:
        "读经平台、查经软件与小组聚会可用的在线圣经阅读工具。",
      jsonLdWebPageDescription: "AskBible.me 宣传页。",
    };
  }
  return {
    documentTitle: "AskBible.me｜读经平台 · 在线查经 · 小组聚会与笔记",
    metaDescription:
      "AskBible.me 是在线读经与查经工具：书页式阅读、问题式查经与章节收藏，适合个人灵修、预备讲道与小组聚会共用经文与讨论。",
    metaKeywords:
      "读经平台,查经软件,查经工具,在线圣经,圣经阅读,小组聚会,小组查经,灵修笔记,AskBible",
    ogTitle: "AskBible.me｜读经平台 · 在线查经 · 小组聚会与笔记",
    ogDescription:
      "在线读经与查经工具：书页式阅读、问题式查经与社区互动，适合灵修与小组聚会。",
    twitterTitle: "AskBible.me｜读经与查经平台",
    twitterDescription: "在线读经、查经与笔记，适合灵修与小组聚会。",
    appleMobileWebAppTitle: "AskBible",
    ogSiteName: "AskBible.me",
    jsonLdWebsiteDescription:
      "在线读经平台与查经工具，支持书页式阅读、问题式查经与小组聚会场景下的经文讨论。",
    jsonLdSoftwareDescription:
      "读经平台、查经软件与小组聚会可用的在线圣经阅读与笔记工具。",
    jsonLdWebPageDescription: "",
  };
}

function getDefaultSiteSeo() {
  return {
    updatedAt: "",
    index: getDefaultSiteSeoPage("index"),
    promo: getDefaultSiteSeoPage("promo"),
  };
}

function normalizeSiteSeoPage(def, raw) {
  const r = raw && typeof raw === "object" ? raw : {};
  const t = (k, max) => safeText(r[k] ?? def[k]).slice(0, max);
  return {
    documentTitle: t("documentTitle", SITE_SEO_MAX_TITLE),
    metaDescription: t("metaDescription", SITE_SEO_MAX_DESC),
    metaKeywords: t("metaKeywords", SITE_SEO_MAX_KEYWORDS),
    ogTitle: t("ogTitle", SITE_SEO_MAX_TITLE),
    ogDescription: t("ogDescription", SITE_SEO_MAX_DESC),
    twitterTitle: t("twitterTitle", SITE_SEO_MAX_TITLE),
    twitterDescription: t("twitterDescription", SITE_SEO_MAX_DESC),
    appleMobileWebAppTitle: t("appleMobileWebAppTitle", SITE_SEO_MAX_SHORT),
    ogSiteName: t("ogSiteName", SITE_SEO_MAX_SHORT),
    jsonLdWebsiteDescription: t(
      "jsonLdWebsiteDescription",
      SITE_SEO_MAX_DESC
    ),
    jsonLdSoftwareDescription: t(
      "jsonLdSoftwareDescription",
      SITE_SEO_MAX_DESC
    ),
    jsonLdWebPageDescription: t(
      "jsonLdWebPageDescription",
      SITE_SEO_MAX_DESC
    ),
  };
}

function loadSiteSeo() {
  const def = getDefaultSiteSeo();
  const raw = readJson(SITE_SEO_FILE, null);
  if (!raw || typeof raw !== "object") {
    return def;
  }
  return {
    updatedAt: safeText(raw.updatedAt || ""),
    index: normalizeSiteSeoPage(def.index, raw.index),
    promo: normalizeSiteSeoPage(def.promo, raw.promo),
  };
}

function saveSiteSeoFromBody(body) {
  const def = getDefaultSiteSeo();
  const cur = loadSiteSeo();
  const inc = body && typeof body === "object" ? body : {};
  const payload = {
    updatedAt: nowIso(),
    index: normalizeSiteSeoPage(def.index, {
      ...def.index,
      ...cur.index,
      ...(inc.index && typeof inc.index === "object" ? inc.index : {}),
    }),
    promo: normalizeSiteSeoPage(def.promo, {
      ...def.promo,
      ...cur.promo,
      ...(inc.promo && typeof inc.promo === "object" ? inc.promo : {}),
    }),
  };
  writeJson(SITE_SEO_FILE, payload);
  return payload;
}

function formatOpenAiChatError(err) {
  const status = Number(
    err?.status ?? err?.response?.status ?? err?.statusCode ?? 0
  );
  const raw = String(
    err?.message || err?.error?.message || err?.cause?.message || ""
  );
  if (status === 401 || /invalid_api_key|incorrect api key/i.test(raw)) {
    return "OpenAI API Key 无效或缺失，请在管理后台「系统密钥」或环境变量 OPENAI_API_KEY 中配置。";
  }
  if (status === 429 || /rate limit|too many requests/i.test(raw)) {
    return "AI 服务请求过于频繁，请稍后再试。";
  }
  if (status === 503 || /overloaded|capacity|unavailable/i.test(raw)) {
    return "AI 服务暂时不可用，请稍后再试。";
  }
  if (
    /model.*not found|does not exist|invalid.*model|unknown model/i.test(raw)
  ) {
    return `当前模型不可用（${safeText(
      process.env.OPENAI_CHAT_MODEL || "gpt-4o-mini"
    )}）。请在服务器环境变量 OPENAI_CHAT_MODEL / OPENAI_CHAT_MODEL_FALLBACK 中改为可用模型（如 gpt-4o-mini）。`;
  }
  if (raw) return raw.length > 400 ? `${raw.slice(0, 400)}…` : raw;
  return "AI 对话请求失败";
}

async function openAiChatHelper({ system, messages }) {
  const client = getOpenAiClient();
  if (!client) {
    throw new Error("缺少 OPENAI_API_KEY");
  }
  const modelPrimary = safeText(process.env.OPENAI_CHAT_MODEL || "gpt-4o-mini");
  const modelFallback = safeText(process.env.OPENAI_CHAT_MODEL_FALLBACK || "gpt-4o");
  const payload = {
    model: modelPrimary,
    messages: [{ role: "system", content: system }, ...messages],
    max_tokens: 4096,
  };
  try {
    const r = await client.chat.completions.create(payload);
    return String(r.choices[0]?.message?.content || "").trim();
  } catch (err) {
    if (modelFallback && modelFallback !== modelPrimary) {
      try {
        const r2 = await client.chat.completions.create({
          ...payload,
          model: modelFallback,
        });
        return String(r2.choices[0]?.message?.content || "").trim();
      } catch (err2) {
        throw new Error(formatOpenAiChatError(err2));
      }
    }
    throw new Error(formatOpenAiChatError(err));
  }
}

/** 从首个 `{` 起按括号深度截取，避免 JSON 字符串值内含 `}` 时 lastIndexOf 截断。 */
function extractBalancedJsonObject(raw) {
  const s = String(raw || "");
  const start = s.indexOf("{");
  if (start === -1) return null;
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = start; i < s.length; i++) {
    const c = s[i];
    if (escape) {
      escape = false;
      continue;
    }
    if (inString) {
      if (c === "\\") {
        escape = true;
        continue;
      }
      if (c === '"') inString = false;
      continue;
    }
    if (c === '"') {
      inString = true;
      continue;
    }
    if (c === "{") depth++;
    else if (c === "}") {
      depth--;
      if (depth === 0) return s.slice(start, i + 1);
    }
  }
  return null;
}

/** 整段文本 → 解析后的 JSON 值（对象或数组），供多类 AI 输出共用。 */
function tryParseJsonLoose(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  const tryVal = (s) => {
    try {
      return JSON.parse(s);
    } catch (_) {
      return null;
    }
  };
  let v = tryVal(raw);
  if (v != null) return v;
  const fence = /```(?:json)?\s*([\s\S]*?)```/i.exec(raw);
  if (fence) {
    v = tryVal(fence[1].trim());
    if (v != null) return v;
  }
  const bal = extractBalancedJsonObject(raw);
  if (bal) {
    v = tryVal(bal);
    if (v != null) return v;
  }
  return null;
}

function parseJsonObjectFromAiText(text) {
  const v = tryParseJsonLoose(text);
  if (v && typeof v === "object" && !Array.isArray(v)) return v;
  return null;
}

/**
 * 人生分期接口：兼容模型把分期放在 lifeStages / phases、根级数组、或仅顶层 appearanceEn 等形态。
 */
function coerceLifeStagesRoot(v) {
  if (v == null) return null;
  if (Array.isArray(v)) {
    const arr = v
      .filter((s) => s && typeof s === "object" && !Array.isArray(s))
      .slice(0, 3);
    if (arr.length < 1) return null;
    return {
      englishName: "",
      scripturePersonalityZh: "",
      scripturePersonalityEn: "",
      lifespanZh: "",
      eraLabelZh: "",
      identityTagsZh: "",
      stages: arr,
    };
  }
  if (typeof v !== "object") return null;

  let stages = null;
  if (Array.isArray(v.stages) && v.stages.length > 0) {
    stages = v.stages;
  } else if (typeof v.stages === "string" && String(v.stages).trim()) {
    try {
      const inner = JSON.parse(v.stages);
      if (Array.isArray(inner) && inner.length > 0) stages = inner;
    } catch (_) {}
  }
  if (!stages) {
    for (const k of ["lifeStages", "life_stages", "phases", "periods"]) {
      const a = v[k];
      if (Array.isArray(a) && a.length > 0) {
        stages = a;
        break;
      }
    }
  }
  if (!stages && v.data && Array.isArray(v.data.stages) && v.data.stages.length > 0) {
    stages = v.data.stages;
  }

  if (!stages) {
    const app = safeText(v.appearanceEn || "").trim();
    if (app) {
      return {
        englishName: safeText(v.englishName || "").slice(0, 80),
        scripturePersonalityZh: safeText(v.scripturePersonalityZh || "").slice(0, 500),
        scripturePersonalityEn: safeText(v.scripturePersonalityEn || "").slice(0, 600),
        lifespanZh: safeText(v.lifespanZh || "").slice(0, 80),
        eraLabelZh: safeText(v.eraLabelZh || "").slice(0, 80),
        identityTagsZh: safeText(v.identityTagsZh || "").slice(0, 240),
        stages: [
          {
            labelZh: safeText(v.labelZh || "").slice(0, 32),
            shortSceneTagEn: safeText(v.shortSceneTagEn || "").slice(0, 160),
            appearanceEn: app.slice(0, 1200),
          },
        ],
      };
    }
    return null;
  }

  return { ...v, stages };
}

function parseLifeStagesPayloadFromAiText(text) {
  return coerceLifeStagesRoot(tryParseJsonLoose(text));
}

/** 编辑预先填写的人物气质/性格：须保留在输出最前，AI 在其后补充，而非覆盖。 */
function mergeBcdScripturePersonality(editorPref, aiOut, maxLen) {
  const u = safeText(editorPref || "").trim();
  const a = safeText(aiOut || "").trim();
  if (!u) return a.slice(0, maxLen);
  if (!a) return u.slice(0, maxLen);
  if (a.startsWith(u)) return a.slice(0, maxLen);
  return `${u}；${a}`.slice(0, maxLen);
}

const BIBLE_EXPLICIT_LIFESPAN_ZH = Object.freeze({
  adam: "活了930岁",
  seth: "活了912岁",
  enosh: "活了905岁",
  kenan: "活了910岁",
  mahalalel: "活了895岁",
  jared: "活了962岁",
  enoch: "与神同行300年；共活365岁",
  methuselah: "活了969岁",
  lamech: "活了777岁",
  noah: "活了950岁",
  shem: "活了600岁",
  arphaxad: "活了438岁",
  salah: "活了433岁",
  eber: "活了464岁",
  peleg: "活了239岁",
  reu: "活了239岁",
  serug: "活了230岁",
  nahor: "活了148岁",
  terah: "活了205岁",
  abraham: "活了175岁",
  sarah: "活了127岁",
  ishmael: "活了137岁",
  isaac: "活了180岁",
  jacob: "活了147岁",
  joseph: "活了110岁",
  levi: "活了137岁",
  kohath: "活了133岁",
  amram: "活了137岁",
  aaron: "活了123岁",
  moses: "活了120岁",
  joshua: "活了110岁",
  job: "晚年又活了140年",
  eli: "活了98岁",
  samuel: "不详",
  david: "活了70岁",
  solomon: "常见推测约60岁左右",
  rehoboam: "约活了58岁",
  abijah: "不详",
  asa: "不详",
  jehoshaphat: "约活了60岁",
  jehoram: "约活了40岁",
  ahaziah: "约活了23岁",
  joash: "约活了47岁",
  amaziah: "约活了54岁",
  azariah: "约活了68岁",
  jotham: "约活了41岁",
  ahaz: "约活了36岁",
  hezekiah: "约活了54岁",
  manasseh: "约活了67岁",
  amon: "约活了24岁",
  josiah: "约活了39岁",
  jehoiakim: "约活了36岁",
  zedekiah: "约活了53岁",
  daniel: "常见推测约80岁以上",
  john_the_baptist: "常见推测约30多岁",
  jesus: "约33岁",
});

const BIBLE_CHARACTER_ENGLISH_NAME_BY_ZH = Object.freeze({
  亚当: "Adam",
  夏娃: "Eve",
  亚伯: "Abel",
  该隐: "Cain",
  塞特: "Seth",
  以诺: "Enoch",
  挪亚: "Noah",
  闪: "Shem",
  含: "Ham",
  雅弗: "Japheth",
  亚伯拉罕: "Abraham",
  撒拉: "Sarah",
  夏甲: "Hagar",
  以实玛利: "Ishmael",
  以撒: "Isaac",
  利百加: "Rebekah",
  以扫: "Esau",
  雅各: "Jacob",
  拉结: "Rachel",
  利亚: "Leah",
  拉班: "Laban",
  约瑟: "Joseph",
  便雅悯: "Benjamin",
  犹大: "Judah",
  利未: "Levi",
  摩西: "Moses",
  亚伦: "Aaron",
  米利暗: "Miriam",
  约书亚: "Joshua",
  喇合: "Rahab",
  底波拉: "Deborah",
  基甸: "Gideon",
  路得: "Ruth",
  撒母耳: "Samuel",
  扫罗: "Saul",
  大卫: "David",
  约拿单: "Jonathan",
  所罗门: "Solomon",
  以利亚: "Elijah",
  以利沙: "Elisha",
  以赛亚: "Isaiah",
  耶利米: "Jeremiah",
  以西结: "Ezekiel",
  但以理: "Daniel",
  以斯帖: "Esther",
  末底改: "Mordecai",
  约伯: "Job",
  马利亚: "Mary",
  约瑟夫: "Joseph",
  施洗约翰: "John the Baptist",
  耶稣: "Jesus",
  彼得: "Peter",
  约翰: "John",
  雅各布: "James",
  雅各: "Jacob",
  保罗: "Paul",
  波提乏: "Potiphar",
  波提乏的妻子: "Potiphar's Wife",
});

const BIBLE_CHARACTER_ROLE_BY_ZH = Object.freeze({
  亚当: "主人物",
  夏娃: "主人物",
  亚伯拉罕: "主人物",
  撒拉: "主人物",
  以撒: "主人物",
  利百加: "主人物",
  雅各: "主人物",
  约瑟: "主人物",
  摩西: "主人物",
  大卫: "主人物",
  耶稣: "主人物",
  施洗约翰: "主人物",
  以扫: "次人物",
  拉班: "次人物",
  波提乏: "次人物",
  波提乏的妻子: "次人物",
});

const BIBLE_EXPLICIT_LIFESPAN_ZH_BY_ZH = Object.freeze({
  亚当: "活了930岁",
  塞特: "活了912岁",
  以挪士: "活了905岁",
  该南: "活了910岁",
  玛勒列: "活了895岁",
  雅列: "活了962岁",
  以诺: "与神同行300年；共活365岁",
  玛土撒拉: "活了969岁",
  拉麦: "活了777岁",
  挪亚: "活了950岁",
  闪: "活了600岁",
  亚法撒: "活了438岁",
  沙拉: "活了433岁",
  希伯: "活了464岁",
  法勒: "活了239岁",
  拉吴: "活了239岁",
  西鹿: "活了230岁",
  拿鹤: "活了148岁",
  他拉: "活了205岁",
  亚伯拉罕: "活了175岁",
  撒拉: "活了127岁",
  以实玛利: "活了137岁",
  以撒: "活了180岁",
  雅各: "活了147岁",
  约瑟: "活了110岁",
  利未: "活了137岁",
  哥辖: "活了133岁",
  暗兰: "活了137岁",
  亚伦: "活了123岁",
  摩西: "活了120岁",
  约书亚: "活了110岁",
  约伯: "晚年又活了140年",
  以利: "活了98岁",
  大卫: "活了70岁",
  所罗门: "常见推测约60岁左右",
  罗波安: "约活了58岁",
  约沙法: "约活了60岁",
  约兰: "约活了40岁",
  亚哈谢: "约活了23岁",
  约阿施: "约活了47岁",
  亚玛谢: "约活了54岁",
  乌西雅: "约活了68岁",
  约坦: "约活了41岁",
  亚哈斯: "约活了36岁",
  希西家: "约活了54岁",
  玛拿西: "约活了67岁",
  亚们: "约活了24岁",
  约西亚: "约活了39岁",
  约雅敬: "约活了36岁",
  西底家: "约活了53岁",
  但以理: "常见推测约80岁以上",
  施洗约翰: "常见推测约30多岁",
  耶稣: "约33岁",
});

function buildPrimaryCharacterDirectorySummary() {
  const primaryEntriesByBook = buildPrimaryCharacterEntriesByBook(
    BIBLE_PRIMARY_CHARACTERS_BY_BOOK
  );
  const oldBookIds = new Set(
    (Array.isArray(testamentOptions?.[0]?.books) ? testamentOptions[0].books : [])
      .map((book) => String(book?.usfx || "").trim())
      .filter((bookId) => bookId && !String(bookId).startsWith("_"))
  );
  const newBookIds = new Set(
    (Array.isArray(testamentOptions?.[1]?.books) ? testamentOptions[1].books : [])
      .map((book) => String(book?.usfx || "").trim())
      .filter((bookId) => bookId && !String(bookId).startsWith("_"))
  );
  const labelById = Object.create(null);
  testamentOptions.forEach((group) => {
    (Array.isArray(group?.books) ? group.books : []).forEach((book) => {
      const bookId = String(book?.usfx || "").trim();
      if (!bookId || bookId.startsWith("_")) return;
      labelById[bookId] = String(book?.cn || book?.en || bookId).trim();
    });
  });
  const oldRows = [];
  const newRows = [];
  const oldUnique = new Set();
  const newUnique = new Set();
  Object.keys(primaryEntriesByBook).forEach((bookId) => {
    const names = Array.isArray(primaryEntriesByBook[bookId])
      ? primaryEntriesByBook[bookId]
      : [];
    const row = {
      bookId,
      bookNameZh: labelById[bookId] || bookId,
      primaryCount: names.length,
    };
    if (oldBookIds.has(bookId)) {
      oldRows.push(row);
      names.forEach((name) => oldUnique.add(String(name?.profileKey || "").trim()));
      return;
    }
    if (newBookIds.has(bookId)) {
      newRows.push(row);
      names.forEach((name) => newUnique.add(String(name?.profileKey || "").trim()));
    }
  });
  return {
    bookLabelById: labelById,
    primaryEntriesByBook,
    summary: {
      oldTestamentUniquePrimary: oldUnique.size,
      newTestamentUniquePrimary: newUnique.size,
      oldTestamentBooks: oldRows,
      newTestamentBooks: newRows,
    },
  };
}

function normalizeBibleCharacterKey(raw) {
  return safeText(raw || "")
    .trim()
    .toLowerCase()
    .replace(/[`'’".,;:()[\]{}]/g, " ")
    .replace(/\b(the|saint)\b/g, " ")
    .replace(/\s+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function toBibleEnglishDisplayName(raw) {
  return safeText(raw || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .map((part) =>
      part
        .split("-")
        .map((seg) =>
          seg ? seg.charAt(0).toUpperCase() + seg.slice(1).toLowerCase() : seg
        )
        .join("-")
    )
    .join(" ")
    .slice(0, 80);
}

function resolveCharacterEnglishName(chineseName, englishName) {
  const identity = resolveCharacterIdentity("", chineseName);
  const zh = safeText(identity.displayNameZh || chineseName || "").trim();
  const current = safeText(englishName || "").trim();
  if (zh && BIBLE_CHARACTER_ENGLISH_NAME_BY_ZH[zh]) {
    return BIBLE_CHARACTER_ENGLISH_NAME_BY_ZH[zh];
  }
  if (current) return toBibleEnglishDisplayName(current);
  return "";
}

function resolveCharacterSourceBookId(chineseName, currentBookId) {
  const current = safeText(currentBookId || "").trim().toUpperCase();
  if (current) return current;
  const identity = resolveCharacterIdentity("", chineseName);
  const zh = safeText(identity.displayNameZh || chineseName || "").trim();
  if (identity.sourceBookId) return safeText(identity.sourceBookId).trim().toUpperCase();
  if (!zh) return "";
  const presetBook = CHARACTER_PRESET_BY_BOOK.find((book) =>
    Array.isArray(book?.names) && book.names.some((name) => String(name || "").trim() === zh)
  );
  if (presetBook?.bookId) return String(presetBook.bookId).trim().toUpperCase();
  if (BIBLE_CHARACTER_PRIMARY_BOOK_BY_ZH[zh]) {
    return String(BIBLE_CHARACTER_PRIMARY_BOOK_BY_ZH[zh]).trim().toUpperCase();
  }
  return "";
}

function normalizeCharacterRoleZh(raw) {
  const s = safeText(raw || "").trim();
  if (s === "主人物" || s === "主要人物" || s === "关键人物") return "主人物";
  if (s === "次人物" || s === "辅助人物" || s === "配角") return "次人物";
  return "";
}

function resolveCharacterRoleZh(chineseName, currentRole, sourceBookId) {
  const normalized = normalizeCharacterRoleZh(currentRole);
  if (normalized) return normalized;
  const identity = resolveCharacterIdentity(sourceBookId, chineseName);
  const zh = safeText(identity.displayNameZh || chineseName || "").trim();
  const bookId = resolveCharacterSourceBookId(zh, sourceBookId);
  const primaryNames = bookId ? BIBLE_PRIMARY_CHARACTERS_BY_BOOK[bookId] : null;
  if (zh && Array.isArray(primaryNames)) {
    return primaryNames.includes(zh) ? "主人物" : "次人物";
  }
  if (zh && BIBLE_CHARACTER_ROLE_BY_ZH[zh]) {
    return BIBLE_CHARACTER_ROLE_BY_ZH[zh];
  }
  return "次人物";
}

function resolveCharacterLifespanZh(chineseName, englishName) {
  const identity = resolveCharacterIdentity("", chineseName);
  const zh = safeText(identity.displayNameZh || chineseName || "").trim();
  const enKey = normalizeBibleCharacterKey(englishName);
  if (zh && BIBLE_EXPLICIT_LIFESPAN_ZH_BY_ZH[zh]) {
    return BIBLE_EXPLICIT_LIFESPAN_ZH_BY_ZH[zh];
  }
  if (enKey && BIBLE_EXPLICIT_LIFESPAN_ZH[enKey]) {
    return BIBLE_EXPLICIT_LIFESPAN_ZH[enKey];
  }
  return "不详";
}

function applyCharacterProfileLifespanDefaults(profilesRoot) {
  const root =
    profilesRoot && typeof profilesRoot === "object" ? profilesRoot : { characters: {} };
  const chars =
    root.characters && typeof root.characters === "object" ? root.characters : {};
  let changed = false;
  for (const [zhName, row] of Object.entries(chars)) {
    if (!row || typeof row !== "object") continue;
    const current = safeText(row.lifespanZh || "").trim();
    if (current) continue;
    row.lifespanZh = resolveCharacterLifespanZh(zhName, row.englishName || "");
    changed = true;
  }
  return { root, changed };
}

function applyCharacterProfileEnglishNameDefaults(profilesRoot) {
  const root =
    profilesRoot && typeof profilesRoot === "object" ? profilesRoot : { characters: {} };
  const chars =
    root.characters && typeof root.characters === "object" ? root.characters : {};
  let changed = false;
  for (const [zhName, row] of Object.entries(chars)) {
    if (!row || typeof row !== "object") continue;
    const next = resolveCharacterEnglishName(zhName, row.englishName || "");
    if (!next || next === safeText(row.englishName || "").trim()) continue;
    row.englishName = next;
    changed = true;
  }
  return { root, changed };
}

function applyCharacterProfileSourceBookDefaults(profilesRoot) {
  const root =
    profilesRoot && typeof profilesRoot === "object" ? profilesRoot : { characters: {} };
  const chars =
    root.characters && typeof root.characters === "object" ? root.characters : {};
  let changed = false;
  for (const [zhName, row] of Object.entries(chars)) {
    if (!row || typeof row !== "object") continue;
    const next = resolveCharacterSourceBookId(zhName, row.sourceBookId || "");
    if (!next || next === safeText(row.sourceBookId || "").trim().toUpperCase()) continue;
    row.sourceBookId = next;
    changed = true;
  }
  return { root, changed };
}

function applyCharacterProfileIdentityDefaults(profilesRoot) {
  const root =
    profilesRoot && typeof profilesRoot === "object" ? profilesRoot : { characters: {} };
  const chars =
    root.characters && typeof root.characters === "object" ? root.characters : {};
  const relatedBookIdsByProfile = buildRelatedBookIdsByProfile(BIBLE_PRIMARY_CHARACTERS_BY_BOOK);
  let changed = false;
  for (const [profileKey, row] of Object.entries(chars)) {
    if (!row || typeof row !== "object") continue;
    const identity = resolveCharacterIdentity(row.sourceBookId || "", profileKey);
    const displayNameZh = safeText(identity.displayNameZh || profileKey).trim();
    if (displayNameZh && displayNameZh !== safeText(row.displayNameZh || "").trim()) {
      row.displayNameZh = displayNameZh;
      changed = true;
    }
    const related = Array.isArray(relatedBookIdsByProfile[profileKey])
      ? relatedBookIdsByProfile[profileKey]
      : [];
    if (related.length) {
      const current = Array.isArray(row.bookIds)
        ? row.bookIds.map((x) => safeText(x).trim().toUpperCase()).filter(Boolean)
        : [];
      const merged = [...new Set([...current, ...related])];
      if (JSON.stringify(merged) !== JSON.stringify(current)) {
        row.bookIds = merged;
        changed = true;
      }
    }
  }
  return { root, changed };
}

function applyCharacterProfileRoleDefaults(profilesRoot) {
  const root =
    profilesRoot && typeof profilesRoot === "object" ? profilesRoot : { characters: {} };
  const chars =
    root.characters && typeof root.characters === "object" ? root.characters : {};
  let changed = false;
  for (const [zhName, row] of Object.entries(chars)) {
    if (!row || typeof row !== "object") continue;
    const next = resolveCharacterRoleZh(
      zhName,
      row.characterRoleZh || "",
      row.sourceBookId || ""
    );
    if (!next || next === safeText(row.characterRoleZh || "").trim()) continue;
    row.characterRoleZh = next;
    changed = true;
  }
  return { root, changed };
}

async function handleCharacterProfileGenerate(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const chineseName = safeText(body.chineseName || "").slice(0, 32);
    if (!chineseName) {
      return res.status(400).json({ error: "请填写中文名（chineseName）。" });
    }
    const notes = safeText(body.notes || "").slice(0, 500);
    const prefZh = safeText(body.scripturePersonalityZh || "").slice(0, 500);
    const prefEn = safeText(body.scripturePersonalityEn || "").slice(0, 600);
    if (!getCurrentOpenAiApiKey()) {
      return res.status(503).json({
        error:
          "未配置 OpenAI API Key。请在管理后台「系统密钥」或环境变量 OPENAI_API_KEY 中配置。",
      });
    }
    const system = [
      "You are a Bible reference assistant for illustration workflows.",
      "Given a biblical figure's Chinese name, output a single JSON object with exactly these string keys:",
      "englishName: common English name as used in Bible translations.",
      "scripturePersonalityZh: concise Chinese — how Scripture portrays this person's character, virtues, and role (e.g. 被称为信心之父、信而顺服). Not physical appearance.",
      "scripturePersonalityEn: one or two English sentences — inner character, faith posture, demeanor for illustrators (e.g. 'father of faith', steadfast obedience); not clothing or face shape.",
      "lifespanZh: concise Chinese lifespan note. Priority: (1) if Scripture explicitly records lifespan, use that; (2) otherwise only use a very common Bible-study / theologian estimate if broadly recognized; (3) otherwise return 不详.",
      "eraLabelZh: concise Chinese era/dynasty/story-period label, e.g. 列王时代 / 士师时代 / 族长时代 / 出埃及年代. Keep it short.",
      "identityTagsZh: concise Chinese tags for this person's biblical identity, separated by Chinese full-width parentheses groups or semicolons, e.g. （信心之父）（蒙召离乡） or （第一个王）（便雅悯支派）. Prefer short, memorable Bible-study labels rather than long prose.",
      "If the user message includes EDITOR_PRIORITY lines for scripturePersonalityZh and/or scripturePersonalityEn, those strings are authoritative: each corresponding JSON field MUST begin with that exact text verbatim, then a Chinese semicolon ；, then your own complementary biblical traits. Never remove or contradict the editor text. If no EDITOR_PRIORITY for a field, generate that field normally.",
      "shortSceneTagEn: one short English phrase (about one sentence) for scene context beside Chinese scene text (e.g. at the well, before Pharaoh). Put story location or moment HERE — NOT inside appearanceEn.",
      "appearanceEn: detailed English visual description for image generation. Include stable facial identity cues (face shape, eye spacing, nose, distinctive traits) so the same figure can be recognized if more life stages are added later. For roster, article, or museum-style use, describe expression and gaze so the figure can engage the reader: calm dignified eye contact toward the viewer when posture allows (soft direct or gentle three-quarter), warm and intentional — not an aggressive stare; this is AI-assisted interpretive illustration, not documentary photography. Ancient Near Eastern or period-appropriate styling; no modern items.",
      "Transparent cutout rule (mandatory for appearanceEn): Renders are full-length character sprites on a TRUE transparent PNG — NO painted background. Describe ONLY what is ON the figure: face, hair, skin, build, posture, hands, garments, footwear, and small handheld props. Do NOT describe any environment: no garden, trees, sky, horizon, architecture, ground plane, landscape, indoor room, \"setting is\", \"surrounded by\", \"lush flora\", directional sunlight tied to a place, or backdrop of any kind. Convey era or story ONLY through clothing, age, and body — put named locations or beats in shortSceneTagEn, not as scenery in appearanceEn.",
      "Stature for lineup (mandatory in appearanceEn when relevant): These sprites are shown bottom-aligned beside others. For an adult woman, note she is only slightly shorter than a typical adult man of the same narrative — about a 5% standing-height difference (≈95% of his crown-to-heel), subtle eye level, NOT a large or cartoonish gap. For an adult man, typical tall adult proportions. For minors, age-appropriate shorter stature versus adults. Never imply every character should be scaled to identical height in the frame.",
      "Cross-cast distinctiveness (mandatory): This person will be shown in a roster beside many other named biblical figures. The face and build must be clearly UNIQUE — not interchangeable with a generic handsome-bearded patriarch or stock template. Deliberately vary face shape, nose bridge and tip, eye shape and spacing, brows, jaw width, cheek volume, ears, hairline, beard density and pattern, stature, and age-appropriate details within believable ancient Levant / broader MENA diversity. A viewer comparing lineup images must not confuse this character with a different named person.",
      "Costume chronology and office (mandatory for appearanceEn): (1) PRIMEVAL — BEFORE Cain in the Genesis story order (creation through Genesis 3: Adam and Eve as the first humans, Eden and immediate expulsion): garments MUST be simple tanned animal hides, fur, or minimal primitive skin wraps only — NOT woven priestly vestments, NOT royal court layered textiles, NOT crown or palace insignia, NOT fine dyed linens of later eras. (2) FROM CAIN ONWARD through all later Scripture: do NOT default the roster to primitive skins. Match clothing to Scripture-informed identity, wealth, social rank, and historical layer — HIGH PRIEST / priests at worship: plausible biblical-era priestly dress (linen layers, ephod-related elements, prescribed colors where fitting); KINGS / QUEENS / high court officials: dignified layered robes, quality weave, tasteful ornament or signs of rule when the narrative warrants; wealthy patriarchs, chiefs, merchants: well-made tunics, mantles, period-plausible dyes; poor, captives, mourners, or deliberate humility: simpler or rougher garb when the text signals. Differentiate tabernacle vs temple vs exile vs return vs Second Temple vs Gospel-era Palestine in plausible cut and textile when the figure’s story sits in that layer.",
      "Clothing must match biblical social standing (apply ONLY outside the primeval Adam/Eve animal-skin rule above): figures Scripture shows as wealthy or high-status — patriarchs with large herds and households (e.g. Abraham, Isaac), chiefs, kings, courtiers, priests — wear well-made ancient Near Eastern dress appropriate to their office and era — layered robes, quality textiles, tasteful dye or trim, dignified draping; do NOT default to drab sackcloth or generic impoverished peasant garb unless the narrative clearly demands poverty, mourning, or deliberate humility. Infer from story cues (flocks, wells, servants, gifts, throne, altar service).",
      "Garment variety within era (mandatory for appearanceEn): Stay strictly ancient Near Eastern / eastern Mediterranean biblical-era dress — wool, linen, tunics, mantles, cloaks, sashes, veils where fitting; NO medieval European, NO Renaissance, NO modern or fantasy costume. Avoid generic roster sameness: do NOT default every male to the same undyed tunic + identical brown outer wrap. Name specific, plausible variety in cut, layering, drape, weave, trim, and head covering for THIS person. Period dyes only (indigo/blue, madder/rust red, purple for elite, undyed cream/gray, olive, terracotta, subtle stripes) — distinct palette vs a monochrome beige cast, but never neon or anachronistic fashion colors.",
      "Respond with ONLY valid JSON, no markdown fences, no commentary.",
    ].join(" ");
    const userParts = [`Chinese name: ${chineseName}`];
    if (notes) userParts.push(`Additional notes from editor: ${notes}`);
    if (prefZh || prefEn) {
      userParts.push("EDITOR_PRIORITY (must lead the JSON personality fields, verbatim, then ； then your additions):");
      if (prefZh) userParts.push(`scripturePersonalityZh: ${prefZh}`);
      if (prefEn) userParts.push(`scripturePersonalityEn: ${prefEn}`);
    }
    let text;
    try {
      text = await openAiChatHelper({
        system,
        messages: [{ role: "user", content: userParts.join("\n") }],
      });
    } catch (err) {
      const msg = err && err.message ? String(err.message) : "生成失败";
      return res.status(500).json({ error: msg });
    }
    const parsed = parseJsonObjectFromAiText(text);
    if (!parsed) {
      return res
        .status(500)
        .json({ error: "模型未返回可解析的 JSON，请重试或手写。" });
    }
    const englishName = resolveCharacterEnglishName(
      chineseName,
      safeText(parsed.englishName || "")
    );
    const scripturePersonalityZh = mergeBcdScripturePersonality(
      prefZh,
      safeText(parsed.scripturePersonalityZh || ""),
      500
    );
    const scripturePersonalityEn = mergeBcdScripturePersonality(
      prefEn,
      safeText(parsed.scripturePersonalityEn || ""),
      600
    );
    const lifespanZh = resolveCharacterLifespanZh(chineseName, englishName).slice(0, 80);
    const eraLabelZh = safeText(parsed.eraLabelZh || "").slice(0, 80);
    const identityTagsZh = safeText(parsed.identityTagsZh || "").slice(0, 240);
    const shortSceneTagEn = safeText(parsed.shortSceneTagEn || "");
    const appearanceEn = safeText(parsed.appearanceEn || "");
    if (!englishName && !appearanceEn) {
      return res.status(500).json({ error: "模型返回内容为空，请重试。" });
    }
    res.json({
      ok: true,
      englishName: englishName.slice(0, 80),
      sourceBookId: resolveCharacterSourceBookId(chineseName, ""),
      characterRoleZh: resolveCharacterRoleZh(
        chineseName,
        "",
        resolveCharacterSourceBookId(chineseName, "")
      ),
      scripturePersonalityZh,
      scripturePersonalityEn,
      lifespanZh,
      eraLabelZh,
      identityTagsZh,
      shortSceneTagEn: shortSceneTagEn.slice(0, 160),
      appearanceEn: appearanceEn.slice(0, 1200),
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "生成失败，请稍后重试。" });
  }
}

/** Editor notes explicitly ask for 2–3 life stages (otherwise genlifestages defaults to 1 adult). */
function bcdEditorNotesRequestMultiStage(raw) {
  const s = safeText(raw || "");
  if (!s) return false;
  const lower = s.toLowerCase();
  if (
    /两期|二期|三期|2期|3期|多期|多个时期|分期|两个时期|三个时期|两阶段|三阶段|两套时期|三套时期/.test(
      s
    )
  ) {
    return true;
  }
  if (/\b(two|three)\s+stages?\b/i.test(lower)) return true;
  if (/\b(2|3)\s*stages?\b/i.test(lower)) return true;
  if (/\bmulti[-\s]?stages?\b/i.test(lower)) return true;
  if (/\bmultiple\s+life\s+stages?\b/i.test(lower)) return true;
  return false;
}

/**
 * If the model returned multiple stages without an explicit editor multi-stage request,
 * keep one canonical adult-oriented stage (middle of three, first of two).
 */
function bcdCoerceLifeStagesToDefaultSingle(stages, notes) {
  const arr = Array.isArray(stages)
    ? stages.filter((s) => s && typeof s === "object").slice(0, 3)
    : [];
  if (bcdEditorNotesRequestMultiStage(notes) || arr.length <= 1) return arr;
  const pick = arr.length === 2 ? 0 : 1;
  return [arr[pick]];
}

async function handleCharacterProfileGenerateLifeStages(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const chineseName = safeText(body.chineseName || "").slice(0, 32);
    if (!chineseName) {
      return res.status(400).json({ error: "请填写中文名（chineseName）。" });
    }
    const notes = safeText(body.notes || "").slice(0, 500);
    const prefZh = safeText(body.scripturePersonalityZh || "").slice(0, 500);
    const prefEn = safeText(body.scripturePersonalityEn || "").slice(0, 600);
    if (!getCurrentOpenAiApiKey()) {
      return res.status(503).json({
        error:
          "未配置 OpenAI API Key。请在管理后台「系统密钥」或环境变量 OPENAI_API_KEY 中配置。",
      });
    }
    const system = [
      "You are a Bible scholar assistant for illustration consistency workflows.",
      "Given a biblical figure's Chinese name, output ONE JSON object with:",
      "englishName: string — common English name in major Bible translations.",
      "scripturePersonalityZh: concise Chinese — character, virtues, and biblical reputation as Scripture presents them (e.g. 信心之父、信而顺服、柔和谦卑). Not physical appearance.",
      "scripturePersonalityEn: 1–3 English sentences — temperament, inner life, and narrative role for illustrators (e.g. 'known as the father of faith', 'courageous before giants'); do NOT repeat hair, face, or clothing (those go in appearanceEn per stage).",
      "lifespanZh: concise Chinese lifespan note. Priority: (1) if Scripture explicitly records lifespan, use that; (2) otherwise only use a very common Bible-study / theologian estimate if broadly recognized; (3) otherwise return 不详.",
      "eraLabelZh: concise Chinese era/story-period label, e.g. 族长时代 / 士师时代 / 联合王国 / 被掳归回后.",
      "identityTagsZh: concise memorable Chinese identity tags, preferably wrapped like （信心之父）（以色列王） or separated with semicolons if needed. Keep tags short and recognizable.",
      "If the user message includes EDITOR_PRIORITY lines for scripturePersonalityZh and/or scripturePersonalityEn, those strings are authoritative: each corresponding JSON field MUST begin with that exact text verbatim, then a Chinese semicolon ；, then your own complementary biblical traits. Never remove or contradict the editor text. If no EDITOR_PRIORITY for a field, generate that field normally.",
      "stages: array — DEFAULT is ONE adult canonical reference (see rules below).",
      "- DEFAULT (when Editor notes do NOT explicitly request multiple life stages): Output exactly 1 stage. That stage MUST be a physically mature adult in prime years — the canonical face-and-body template for roster and downstream scene generation — NOT an infant or child, NOT extreme end-of-life frailty as the only output. labelZh may cue 成年 / 壮年 / 标准参考. shortSceneTagEn: one representative story beat. appearanceEn: lock clear, stable facial identity traits (bone structure, eyes, nose, jaw, skin-tone family) plus prime-adult hair, build, and costume that match Scripture office/wealth.",
      "- MULTI-STAGE (2 or 3 stages) ONLY when Editor notes explicitly request it — e.g. Chinese: 两期、二期、三期、2期、3期、多期、多个时期、分期、两个时期、三个时期、两阶段、三阶段、两套时期、三套时期; English: two stages, three stages, multi-stage, 2 stages, 3 stages, multiple life stages. If there is NO such explicit request in the user message, you MUST output exactly 1 stage — never 2 or 3.",
      "- When multi-stage IS requested: output 2 or 3 stages (chronological order; never more than 3; never padded). Each stage must differ in age and/or narrative role. COSTUMES must differ MEANINGFULLY between stages — never copy-paste the same garment description. When Scripture shows growing prosperity, covenant blessing, large household, or exalted office, LATER / OLDER stages must wear VISIBLY RICHER, higher-status dress than earlier humbler phases (more layered robes, finer weave, quality dyes or trim, dignified mantle) — the wealth/status jump must be obvious at a glance, not a near duplicate outfit with wrinkles added.",
      "Order stages chronologically by the biblical story. Each stage object must have:",
      "labelZh: concise Chinese label (e.g. 唯一出场 / 早期 / 中期 / 后期 plus a short biblical cue when helpful).",
      "shortSceneTagEn: one short English phrase for scene context for THAT stage (location or story beat). Put environment or place names HERE — NOT as background scenery inside appearanceEn.",
      "appearanceEn: detailed English visual description for THAT stage. For each stage, when the face will be visible in portraits, note calm dignified gaze toward the viewer where posture allows (soft direct or gentle three-quarter) — intentional engagement for article or display; AI-assisted interpretive art, not a historical photograph.",
      "Transparent cutout rule (mandatory for every appearanceEn): Outputs are full-length transparent-PNG character references — NO painted background. Describe ONLY the figure: face, hair, skin, build, posture, hands, clothing, footwear, small handheld props. NEVER describe gardens, skies, trees, buildings, ground, horizons, \"lush setting\", flora, rooms, or any backdrop; NEVER write \"the setting is\". Narrative place or moment belongs in shortSceneTagEn only.",
      "Stature for lineup (mandatory in each appearanceEn when relevant): Sprites are composited bottom-aligned. For adult women, include only slightly shorter standing height than typical adult men in the same era (~5% / ≈95% of male crown-to-heel, subtle — not a dramatic gap). For adult men, full adult height class. For children or elders, age-appropriate height versus prime adults. Multi-stage same person: younger stages must be shorter than older adult stages — never equalize all stages to one height.",
      "CRITICAL — ONE PERSON, NOT SEPARATE CAST: Every stage describes the SAME biological individual. You must NOT write descriptions that imply different unrelated faces or different ethnicities between stages.",
      "Facial identity lock (mandatory for multi-stage outputs): Pick ONE stable facial blueprint for this figure — face shape, eye spacing and shape, nose and brow, jaw, ear shape, baseline skin tone family, any distinctive mark. Copy this SAME blueprint into EVERY stage's appearanceEn as an opening clause (use identical wording for the shared traits). After that clause, describe ONLY what changes in THIS stage: age, wrinkles, hair/beard length and color, body build, clothing, and pose. Later stages show aging or costume change on the SAME face, like one actor in makeup, not three different actors. Secondary-stage generation must never invent a new face — only age the same blueprint. Wealth progression: when later stages are materially richer in Scripture, spell out clearly upgraded textiles and layering vs earlier stages. Do not add environmental \"setting\" in appearanceEn (transparent sprite).",
      "If stages.length === 1, the single appearanceEn should still name clear facial identity traits for future consistency.",
      "Cross-cast distinctiveness (mandatory): The facial blueprint you lock for this figure must be visually UNIQUE in the project's character roster — not the same generic face as other biblical portraits. When inventing the shared blueprint, deliberately differentiate face shape, nose, eyes, brows, jaw, ears, hairline, beard pattern, and stature from a stock patriarch template, while staying coherent across this person's own stages.",
      "Ancient Near Eastern or period-appropriate styling; no modern items.",
      "Costume chronology and office (mandatory for each appearanceEn): Same rules as single-profile generation — PRIMEVAL BEFORE Cain (Adam, Eve in Genesis 1–3 / immediate expulsion): animal hides or primitive skin wraps only, no priestly or royal woven finery. FROM CAIN ONWARD: dress MUST reflect office (priest, king, prophet, soldier, slave, farmer, etc.), wealth, rank, and narrative era — priests at service in plausible biblical-era cultic dress; royalty and high court in layered quality robes and fitting insignia when warranted; wealthy households in good tunics and mantles; poverty, exile, mourning in humbler cloth when the text says so. Each stage’s clothing must evolve plausibly with that stage’s story moment.",
      "Clothing per stage must match biblical social standing (outside primeval animal-skin-only cases): wealthy patriarchs, tribal leaders, kings, officials, and priests should look appropriately prosperous and role-specific — layered robes, quality woven garments, priestly or royal detail where Scripture places them; avoid a generic drab peasant default when the person is rich, honored, or holds sacred or royal office. Use humbler dress only when the text clearly indicates poverty, exile, mourning, or similar.",
      "Garment variety within era (mandatory for each appearanceEn): Strictly biblical-era Near Eastern / eastern Mediterranean garments only; forbidden medieval, Renaissance, modern, or fantasy dress. Across the project many figures exist — do NOT reuse one stock costume description for every patriarch. For this figure, specify concrete variety (layering, mantle vs wrap, sleeve, sash, trim, head covering) and period-plausible colors (undyed wool, indigo, madder red, purple accents if elite, olive, terracotta, etc.) so they are not interchangeable with a generic beige-brown template, without breaking historical plausibility.",
      "Respond with ONLY valid JSON. No markdown fences, no commentary.",
      "Required root shape (mandatory): a single JSON object with keys englishName, scripturePersonalityZh, scripturePersonalityEn, lifespanZh, eraLabelZh, identityTagsZh, and stages. The stages value MUST be a JSON array: default length 1 (single adult); length 2–3 ONLY when Editor notes explicitly request multi-stage. Each object MUST have labelZh, shortSceneTagEn, appearanceEn. Never omit the key \"stages\"; do not rename it (e.g. not lifeStages only); do not return the stages list as the root array without wrapping it in that object.",
    ].join(" ");
    const userParts = [`Chinese name: ${chineseName}`];
    userParts.push(
      bcdEditorNotesRequestMultiStage(notes)
        ? "Remember: all stages are the same person; facial identity must be unified; costumes must differ stage to stage, with later wealthy phases visibly richer dress than earlier ones."
        : "Remember: output exactly ONE adult prime-life stage unless Editor notes explicitly requested multiple stages; that single stage is the canonical face template."
    );
    if (notes) userParts.push(`Editor notes: ${notes}`);
    if (prefZh || prefEn) {
      userParts.push("EDITOR_PRIORITY (must lead the JSON personality fields, verbatim, then ； then your additions):");
      if (prefZh) userParts.push(`scripturePersonalityZh: ${prefZh}`);
      if (prefEn) userParts.push(`scripturePersonalityEn: ${prefEn}`);
    }
    let text = "";
    let parsed = null;
    for (let attempt = 0; attempt < 2; attempt++) {
      const userContent =
        attempt === 0
          ? userParts.join("\n")
          : `${userParts.join("\n")}\n\nVALIDATION FAILED: Your last reply was not usable. Reply with ONLY one JSON object (no markdown, no prose). It MUST include top-level keys englishName, scripturePersonalityZh, scripturePersonalityEn, lifespanZh, eraLabelZh, identityTagsZh, and stages. The value of stages MUST be a JSON array of 1 to 3 objects; each object MUST have labelZh, shortSceneTagEn, appearanceEn.`;
      try {
        text = await openAiChatHelper({
          system,
          messages: [{ role: "user", content: userContent }],
        });
      } catch (err) {
        const msg = err && err.message ? String(err.message) : "生成失败";
        return res.status(500).json({ error: msg });
      }
      parsed = parseLifeStagesPayloadFromAiText(text);
      if (parsed && Array.isArray(parsed.stages) && parsed.stages.length > 0) break;
      console.warn(
        "[genlifestages] parse/coerce miss attempt",
        attempt + 1,
        String(text || "").slice(0, 500)
      );
    }
    if (!parsed || !Array.isArray(parsed.stages) || parsed.stages.length < 1) {
      return res
        .status(500)
        .json({ error: "模型未返回含 stages 数组的 JSON，请重试。" });
    }
    let rawStages = bcdCoerceLifeStagesToDefaultSingle(parsed.stages, notes);
    if (rawStages.length < 1 || rawStages.length > 3) {
      return res
        .status(500)
        .json({ error: "模型应返回 1～3 个时期，请重试。" });
    }
    const stages = [];
    for (let i = 0; i < rawStages.length; i++) {
      const s = rawStages[i];
      stages.push({
        labelZh: safeText(s.labelZh || "").slice(0, 32),
        shortSceneTagEn: safeText(s.shortSceneTagEn || "").slice(0, 160),
        appearanceEn: safeText(s.appearanceEn || "").slice(0, 1200),
      });
    }
    const englishName = resolveCharacterEnglishName(
      chineseName,
      safeText(parsed.englishName || "")
    ).slice(0, 80);
    const scripturePersonalityZh = mergeBcdScripturePersonality(
      prefZh,
      safeText(parsed.scripturePersonalityZh || ""),
      500
    );
    const scripturePersonalityEn = mergeBcdScripturePersonality(
      prefEn,
      safeText(parsed.scripturePersonalityEn || ""),
      600
    );
    const lifespanZh = resolveCharacterLifespanZh(chineseName, englishName).slice(0, 80);
    const eraLabelZh = safeText(parsed.eraLabelZh || "").slice(0, 80);
    const identityTagsZh = safeText(parsed.identityTagsZh || "").slice(0, 240);
    if (!englishName && !stages.some((x) => String(x.appearanceEn || "").trim())) {
      return res.status(500).json({ error: "模型返回内容为空，请重试。" });
    }
    const gptUserEn = userParts.join("\n");
    res.json({
      ok: true,
      englishName,
      sourceBookId: resolveCharacterSourceBookId(chineseName, ""),
      characterRoleZh: resolveCharacterRoleZh(
        chineseName,
        "",
        resolveCharacterSourceBookId(chineseName, "")
      ),
      scripturePersonalityZh,
      scripturePersonalityEn,
      lifespanZh,
      eraLabelZh,
      identityTagsZh,
      stages,
      promptLog: {
        gptSystemEn: system,
        gptUserEn,
      },
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "生成失败，请稍后重试。" });
  }
}

async function handleCharacterProfilePromptArchiveTranslate(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const rawParts = body.parts;
    if (!Array.isArray(rawParts) || rawParts.length < 1) {
      return res.status(400).json({ error: "parts 须为非空数组。" });
    }
    if (rawParts.length > 8) {
      return res.status(400).json({ error: "parts 最多 8 段。" });
    }
    if (!getCurrentOpenAiApiKey()) {
      return res.status(503).json({
        error:
          "未配置 OpenAI API Key。请在管理后台「系统密钥」或环境变量 OPENAI_API_KEY 中配置。",
      });
    }
    const parts = [];
    let totalChars = 0;
    for (let i = 0; i < rawParts.length; i++) {
      const p = rawParts[i];
      const id = safeText(p.id || "").slice(0, 64) || `p${i}`;
      const label = safeText(p.label || "").slice(0, 120);
      const text = safeText(p.text || "").slice(0, 12000);
      totalChars += text.length;
      if (totalChars > 48000) {
        return res.status(400).json({ error: "各段文本总长度过大。" });
      }
      parts.push({ id, label, text });
    }
    const system = [
      "You translate English LLM and image-generation prompt text into Simplified Chinese for Bible illustration tool admins.",
      'Output ONLY valid JSON, no markdown fences: {"items":[{"id":"...","zh":"..."}]} — same ids as input, same order, one item per block.',
      "Translate faithfully. Use familiar Chinese for biblical names when obvious. Keep untranslatable tokens like PNG, alpha, JSON.",
    ].join(" ");
    const blocks = parts
      .map(
        (p) =>
          `[BLOCK id=${JSON.stringify(p.id)}]\n${p.text}`
      )
      .join("\n\n---\n\n");
    const user = `Translate each BLOCK's text to Chinese in "zh". Return JSON items in the same order as the blocks.\n\n${blocks}`;
    let text;
    try {
      text = await openAiChatHelper({
        system,
        messages: [{ role: "user", content: user }],
      });
    } catch (err) {
      const msg = err && err.message ? String(err.message) : "翻译失败";
      return res.status(500).json({ ok: false, error: msg });
    }
    const parsed = parseJsonObjectFromAiText(text);
    const items =
      parsed && Array.isArray(parsed.items) ? parsed.items : null;
    if (!items || items.length === 0) {
      return res
        .status(500)
        .json({ ok: false, error: "模型未返回可解析的翻译 JSON。" });
    }
    const out = items.map((it) => ({
      id: safeText(it.id || "").slice(0, 64),
      zh: safeText(it.zh || "").slice(0, 16000),
    }));
    res.json({ ok: true, items: out });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "翻译失败，请稍后重试。" });
  }
}

function loadQuestionSubmissions() {
  const data = readJson(QUESTION_SUBMISSIONS_FILE, null);
  if (!data || typeof data !== "object") {
    return { items: [] };
  }
  if (!Array.isArray(data.items)) data.items = [];
  data.items = data.items.map((item) => ({
    ...item,
    status: safeText(item?.status || "pending") || "pending",
    moderationReasons: Array.isArray(item?.moderationReasons)
      ? item.moderationReasons.map((x) => safeText(x || "")).filter(Boolean).slice(0, 8)
      : [],
    reviewedAt: safeText(item?.reviewedAt || ""),
    replies: Array.isArray(item?.replies)
      ? item.replies
          .map((reply) => ({
            id: safeText(reply?.id || ""),
            questionId: safeText(reply?.questionId || item?.id || ""),
            replyText: safeText(reply?.replyText || ""),
            userId: safeText(reply?.userId || ""),
            userName: safeText(reply?.userName || ""),
            userEmail: safeText(reply?.userEmail || ""),
            createdAt: safeText(reply?.createdAt || ""),
          }))
          .filter((reply) => reply.id && reply.replyText)
      : [],
  }));
  return data;
}

function saveQuestionSubmissions(data) {
  writeJson(QUESTION_SUBMISSIONS_FILE, data);
}

function normalizeQuestionModerationText(raw) {
  return String(raw || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function assessQuestionSubmissionRisk({
  questionText,
  authed,
  req,
  db,
  bookId,
  chapter,
}) {
  const text = String(questionText || "").trim();
  const normalized = normalizeQuestionModerationText(text);
  const reasons = [];
  let reject = false;
  let pending = false;

  const adPatterns = [
    /(?:https?:\/\/|www\.|\.com\b|\.cn\b|\.net\b|\.cc\b|\.top\b|t\.me\/|wa\.me\/)/i,
    /(?:微信|vx|v信|加微|加v|qq|q群|电报|telegram|whatsapp|联系我|私聊|公众号|扫码)/i,
    /(?:代写|兼职|赚钱|推广|引流|返利|优惠|加群|课程咨询|办理|出售|购买|代理)/i,
  ];
  if (adPatterns.some((re) => re.test(text))) {
    reject = true;
    reasons.push("疑似广告或引流");
  }

  if (/(.)\1{7,}/.test(text) || /^[\W_]+$/.test(text)) {
    pending = true;
    reasons.push("疑似灌水字符");
  }

  if (text.length > 220) {
    pending = true;
    reasons.push("内容过长");
  }

  const tokenCount = normalized
    ? normalized.split(/[\s,.;:!?，。！？；：、/\\|()[\]{}"'`~+-]+/).filter(Boolean).length
    : 0;
  if (tokenCount > 0 && tokenCount <= 2 && text.length >= 24) {
    pending = true;
    reasons.push("疑似重复刷词");
  }

  const items = Array.isArray(db?.items) ? db.items : [];
  const userKey = safeText(authed?.id || authed?.email || "");
  const ipHash = sha256Hex(getClientIp(req));
  const nowMs = Date.now();
  const recentByActor = items.filter((item) => {
    const createdAtMs = Date.parse(String(item?.createdAt || ""));
    if (!Number.isFinite(createdAtMs) || nowMs - createdAtMs > 10 * 60 * 1000) {
      return false;
    }
    const sameUser = userKey && safeText(item?.userId || item?.userEmail || "") === userKey;
    const sameIp = safeText(item?.ipHash || "") === ipHash;
    return sameUser || sameIp;
  });
  if (recentByActor.length >= 5) {
    pending = true;
    reasons.push("短时间连续提问过多");
  }

  const sameQuestionRecent = recentByActor.filter((item) => {
    const prev = normalizeQuestionModerationText(item?.questionText || "");
    return prev && prev === normalized;
  });
  if (sameQuestionRecent.length >= 1) {
    pending = true;
    reasons.push("短时间重复提问");
  }

  const sameChapterBurst = recentByActor.filter(
    (item) =>
      safeText(item?.bookId || "") === safeText(bookId || "") &&
      toSafeNumber(item?.chapter, 0) === toSafeNumber(chapter, 0)
  );
  if (sameChapterBurst.length >= 3) {
    pending = true;
    reasons.push("同章短时密集提问");
  }

  if (reject) {
    return {
      status: "rejected",
      reasons,
      userMessage: "内容疑似广告、引流或联系方式，未予发布。",
      autoReviewed: true,
    };
  }
  if (pending) {
    return {
      status: "pending",
      reasons,
      userMessage: "已收到你的问题；系统检测到异常特征，已转入人工复核。",
      autoReviewed: false,
    };
  }
  return {
    status: "approved",
    reasons: ["自动通过"],
    userMessage: "已提交，感谢你的好问题",
    autoReviewed: true,
  };
}

function loadQuestionCorrections() {
  const data = readJson(QUESTION_CORRECTIONS_FILE, null);
  if (!data || typeof data !== "object") return { items: [] };
  if (!Array.isArray(data.items)) data.items = [];
  return data;
}

function saveQuestionCorrections(data) {
  writeJson(QUESTION_CORRECTIONS_FILE, data);
}

function stablePresetCorrectionKey(
  bookId,
  chapter,
  contentVersion,
  contentLang,
  rangeStart,
  rangeEnd,
  segmentTitle,
  questionIndex
) {
  const seed = [
    safeText(bookId),
    String(toSafeNumber(chapter, 0)),
    safeText(contentVersion),
    safeText(contentLang),
    String(toSafeNumber(rangeStart, 0)),
    String(toSafeNumber(rangeEnd, 0)),
    safeText(segmentTitle),
    String(toSafeNumber(questionIndex, 0)),
  ].join("|");
  let hash = 0;
  for (let i = 0; i < seed.length; i += 1) {
    hash = (hash * 31 + seed.charCodeAt(i)) >>> 0;
  }
  return `qcorr_${hash.toString(16)}`;
}

function readCurrentPresetQuestionTextFromPublished(fields) {
  const bookId = safeText(fields.bookId || "");
  const chapter = toSafeNumber(fields.chapter, 0);
  const contentVersion = safeText(fields.contentVersion || "");
  const contentLang = safeText(fields.contentLang || "");
  const rangeStart = toSafeNumber(fields.rangeStart, 0);
  const rangeEnd = toSafeNumber(fields.rangeEnd, 0);
  const segmentTitle = safeText(fields.segmentTitle || "");
  const questionIndex = toSafeNumber(fields.questionIndex, 0);
  if (!bookId || !chapter || !contentVersion || !contentLang) {
    return { error: "缺少书籍或版本信息" };
  }
  const data = readPublishedContent({
    versionId: contentVersion,
    lang: contentLang,
    bookId,
    chapter,
  });
  if (!data) return { error: "未找到已发布内容" };
  const seg = (data.segments || []).find(
    (s) =>
      toSafeNumber(s.rangeStart, 0) === rangeStart &&
      toSafeNumber(s.rangeEnd, 0) === rangeEnd &&
      safeText(s.title || "") === segmentTitle
  );
  if (!seg || !Array.isArray(seg.questions)) {
    return { error: "未找到对应段落或问题" };
  }
  const q = seg.questions[questionIndex];
  if (typeof q !== "string") return { error: "问题索引无效" };
  return { text: q };
}

function buildApprovedPresetCorrectionTextMap(
  db,
  bookId,
  chapter,
  contentVersion,
  contentLang
) {
  const map = new Map();
  const items = (db.items || []).filter(
    (x) =>
      safeText(x.status) === "approved" &&
      safeText(x.targetType) === "preset" &&
      safeText(x.bookId) === safeText(bookId) &&
      toSafeNumber(x.chapter, 0) === toSafeNumber(chapter, 0) &&
      safeText(x.contentVersion) === safeText(contentVersion) &&
      safeText(x.contentLang) === safeText(contentLang)
  );
  items.sort((a, b) =>
    String(b.reviewedAt || b.createdAt || "").localeCompare(
      String(a.reviewedAt || a.createdAt || "")
    )
  );
  for (const it of items) {
    const sk = safeText(it.stableKey || "");
    if (sk && !map.has(sk)) map.set(sk, safeText(it.proposedText || ""));
  }
  return map;
}

function applyPresetQuestionCorrectionsToStudyPayload(
  data,
  bookId,
  chapter,
  contentVersion,
  contentLang
) {
  if (!data || !Array.isArray(data.segments)) return data;
  const db = loadQuestionCorrections();
  const keyToText = buildApprovedPresetCorrectionTextMap(
    db,
    bookId,
    chapter,
    contentVersion,
    contentLang
  );
  if (!keyToText.size) return data;
  const next = {
    ...data,
    segments: data.segments.map((seg) => ({
      ...seg,
      questions: Array.isArray(seg.questions) ? [...seg.questions] : [],
    })),
  };
  for (const seg of next.segments) {
    const qs = seg.questions;
    for (let i = 0; i < qs.length; i += 1) {
      const sk = stablePresetCorrectionKey(
        bookId,
        chapter,
        contentVersion,
        contentLang,
        seg.rangeStart,
        seg.rangeEnd,
        seg.title,
        i
      );
      const replacement = keyToText.get(sk);
      if (replacement != null && replacement !== "" && typeof qs[i] === "string") {
        qs[i] = replacement;
      }
    }
  }
  return next;
}

function invalidateStudyContentCache(version, lang, bookId, chapter) {
  const key = `${STUDY_CONTENT_CACHE_TAG}:${String(version)}:${String(lang)}:${String(
    bookId
  )}:${Number(chapter)}`;
  readApiCache.delete(key);
}

/** 与 /api/questions/approved 中 userLevel 计算方式一致（按已通过审核的贡献数） */
function getCommunityUserLevelFromSubmissions(userId, userEmail) {
  const db = loadQuestionSubmissions();
  const approvedAll = (db.items || []).filter(
    (x) => safeText(x.status) === "approved"
  );
  const approvedCountByUser = new Map();
  approvedAll.forEach((x) => {
    const uid = safeText(x.userId || x.userEmail || "");
    if (!uid) return;
    approvedCountByUser.set(uid, (approvedCountByUser.get(uid) || 0) + 1);
  });
  const uid = safeText(userId || "");
  const email = safeText(userEmail || "");
  let approvedCount = 0;
  if (uid && approvedCountByUser.has(uid)) {
    approvedCount = Number(approvedCountByUser.get(uid) || 0);
  } else if (email && approvedCountByUser.has(email)) {
    approvedCount = Number(approvedCountByUser.get(email) || 0);
  }
  if (approvedCount <= 0) return 0;
  return Math.max(1, Math.min(12, Math.floor((approvedCount - 1) / 3) + 1));
}

function loadDeployState() {
  const data = readJson(DEPLOY_STATE_FILE, null);
  if (!data || typeof data !== "object") {
    return { currentVersion: "", uploads: [], history: [] };
  }
  if (!Array.isArray(data.uploads)) data.uploads = [];
  if (!Array.isArray(data.history)) data.history = [];
  data.currentVersion = safeText(data.currentVersion || "");
  return data;
}

function saveDeployState(state) {
  writeJson(DEPLOY_STATE_FILE, state);
}

function loadAdminAudit() {
  const data = readJson(ADMIN_AUDIT_FILE, null);
  if (!data || typeof data !== "object") return { items: [] };
  if (!Array.isArray(data.items)) data.items = [];
  return data;
}

function saveAdminAudit(data) {
  writeJson(ADMIN_AUDIT_FILE, data);
}

function appendAdminAudit(req, actor, action, detail = {}) {
  const db = loadAdminAudit();
  const item = {
    id: `audit_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    at: nowIso(),
    action: safeText(action || ""),
    actorId: safeText(actor?.id || ""),
    actorName: safeText(actor?.name || ""),
    actorEmail: safeText(actor?.email || ""),
    actorRole: safeText(actor?.adminRole || ""),
    ipHash: sha256Hex(getClientIp(req)),
    detail,
  };
  db.items = [item, ...(db.items || [])].slice(0, 2000);
  saveAdminAudit(db);
}

function listDataBackups() {
  ensureDir(DATA_BACKUPS_DIR);
  return fs
    .readdirSync(DATA_BACKUPS_DIR)
    .filter((x) => x.endsWith(".json"))
    .map((x) => readJson(path.join(DATA_BACKUPS_DIR, x), null))
    .filter(Boolean)
    .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
}

function pruneDataBackups(keepCount) {
  const keep = Math.max(1, Math.min(200, toSafeNumber(keepCount, DEFAULT_DATA_BACKUP_KEEP_COUNT)));
  const all = listDataBackups();
  const removeItems = all.slice(keep);
  const removed = [];
  for (const item of removeItems) {
    const id = safeText(item?.id || "");
    if (!id) continue;
    const dir = path.join(DATA_BACKUPS_DIR, id);
    const metaFile = path.join(DATA_BACKUPS_DIR, `${id}.json`);
    try {
      if (fs.existsSync(dir)) fs.rmSync(dir, { recursive: true, force: true });
      if (fs.existsSync(metaFile)) fs.unlinkSync(metaFile);
      removed.push(id);
    } catch (_err) {
      // ignore one-off cleanup error and continue others
    }
  }
  return { keepCount: keep, removed, removedCount: removed.length, totalBefore: all.length };
}

function runAutoDataBackupTick() {
  const ops = loadOpsConfig();
  if (!ops.autoBackupEnabled) return;
  const now = new Date();
  const hh = now.getHours();
  const mm = now.getMinutes();
  if (hh !== Number(ops.autoBackupHour) || mm !== Number(ops.autoBackupMinute)) return;
  const today = now.toISOString().slice(0, 10);
  if (safeText(ops.lastAutoBackupDate) === today) return;
  const backup = createDataBackup();
  const prune = pruneDataBackups(ops.dataBackupKeepCount);
  saveOpsConfig({ lastAutoBackupDate: today });
  appendAdminAudit(null, null, "data_backup_auto", {
    backupId: backup.id,
    keepCount: ops.dataBackupKeepCount,
    removedCount: prune.removedCount,
    scheduledAt: `${String(ops.autoBackupHour).padStart(2, "0")}:${String(
      ops.autoBackupMinute
    ).padStart(2, "0")}`,
  });
}

function runAutoDataBackupNowByUser(authed, req) {
  const ops = loadOpsConfig();
  const backup = createDataBackup();
  const prune = pruneDataBackups(ops.dataBackupKeepCount);
  appendAdminAudit(req, authed, "data_backup_auto_manual", {
    backupId: backup.id,
    keepCount: ops.dataBackupKeepCount,
    removedCount: prune.removedCount,
    scheduledAt: `${String(ops.autoBackupHour).padStart(2, "0")}:${String(
      ops.autoBackupMinute
    ).padStart(2, "0")}`,
  });
  return {
    backup,
    prune,
    keepCount: ops.dataBackupKeepCount,
    autoBackupEnabled: ops.autoBackupEnabled,
    autoBackupHour: ops.autoBackupHour,
    autoBackupMinute: ops.autoBackupMinute,
  };
}

function createDataBackup() {
  const id = `dbk_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const dir = path.join(DATA_BACKUPS_DIR, id);
  ensureDir(dir);
  const copied = [];
  const targets = [
    { abs: ADMIN_DIR, rel: "admin_data" },
    { abs: CONTENT_PUBLISHED_DIR, rel: "content_published" },
    { abs: CONTENT_BUILDS_DIR, rel: "content_builds" },
    { abs: CHAPTER_ILLUSTRATION_GENERATED_DIR, rel: "public/generated" },
  ];
  for (const t of targets) {
    if (!fs.existsSync(t.abs)) continue;
    const dest = path.join(dir, t.rel);
    fs.cpSync(t.abs, dest, { recursive: true, force: true });
    copied.push(t.rel);
  }
  const meta = {
    id,
    createdAt: nowIso(),
    copied,
  };
  writeJson(path.join(DATA_BACKUPS_DIR, `${id}.json`), meta);
  return meta;
}

function restoreDataBackup(backupId) {
  const safeId = safeText(backupId || "");
  if (!safeId) throw new Error("缺少 backupId");
  const dir = path.join(DATA_BACKUPS_DIR, safeId);
  if (!fs.existsSync(dir)) throw new Error("备份不存在");
  const pairs = [
    { src: path.join(dir, "admin_data"), dest: ADMIN_DIR },
    { src: path.join(dir, "content_published"), dest: CONTENT_PUBLISHED_DIR },
    { src: path.join(dir, "content_builds"), dest: CONTENT_BUILDS_DIR },
    {
      src: path.join(dir, "public", "generated"),
      dest: CHAPTER_ILLUSTRATION_GENERATED_DIR,
    },
  ];
  const restored = [];
  for (const p of pairs) {
    if (!fs.existsSync(p.src)) continue;
    ensureDir(path.dirname(p.dest));
    fs.cpSync(p.src, p.dest, { recursive: true, force: true });
    restored.push(path.basename(p.dest));
  }
  return {
    backupId: safeId,
    restored,
    at: nowIso(),
  };
}

function buildDataBackupZip(backupId) {
  const safeId = safeText(backupId || "");
  if (!safeId) throw new Error("缺少 backupId");
  const backupDir = path.join(DATA_BACKUPS_DIR, safeId);
  if (!fs.existsSync(backupDir)) throw new Error("备份不存在");
  const zipId = `data_backup_${safeId}_${Date.now()}`;
  const zipPath = path.join(DEPLOY_UPLOADS_DIR, `${zipId}.zip`);
  const zip = new AdmZip();
  const files = walkFiles(backupDir);
  let addedCount = 0;
  for (const abs of files) {
    const rel = path.relative(backupDir, abs).replaceAll("\\", "/");
    if (!rel) continue;
    zip.addLocalFile(abs, safeId, rel);
    addedCount += 1;
  }
  zip.addFile(
    `${safeId}/version.json`,
    Buffer.from(JSON.stringify({ backupId: safeId, generatedAt: nowIso() }, null, 2), "utf8")
  );
  zip.writeZip(zipPath);
  return { zipPath, backupId: safeId, addedCount };
}

function walkFiles(baseDir) {
  const out = [];
  if (!fs.existsSync(baseDir)) return out;
  const stack = [baseDir];
  while (stack.length) {
    const cur = stack.pop();
    const entries = fs.readdirSync(cur, { withFileTypes: true });
    for (const e of entries) {
      const p = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(p);
      else out.push(p);
    }
  }
  return out;
}

function normalizeDeployPackageKind(kind) {
  const k = safeText(kind || "")
    .trim()
    .toLowerCase()
    .replace(/_/g, "-");
  if (k === "full") return "full";
  if (k === "full-slim") return "full-slim";
  return "upgrade";
}

function addLocalFileToZipIfExists(zip, absPath, relPath, seenRelPaths = null) {
  const safeRel = String(relPath || "").replaceAll("\\", "/");
  if (!safeRel || (seenRelPaths && seenRelPaths.has(safeRel))) return false;
  if (!absPath || !fs.existsSync(absPath)) return false;
  try {
    if (!fs.statSync(absPath).isFile()) return false;
  } catch {
    return false;
  }
  zip.addLocalFile(absPath, path.dirname(safeRel), path.basename(safeRel));
  if (seenRelPaths) seenRelPaths.add(safeRel);
  return true;
}

function collectGeneratedImageRelativePathsFromValue(input, out = new Set()) {
  if (typeof input === "string") {
    const raw = input.trim();
    if (raw) {
      const normalized = normalizeIllustrationImageUrlForPublication(raw);
      if (normalized && normalized.startsWith("/generated/")) {
        const resolved = resolveSafeGeneratedPngPath(normalized);
        if (resolved) {
          const rel = path.relative(__dirname, resolved).replaceAll("\\", "/");
          if (rel) out.add(rel);
        }
      }
    }
    return out;
  }
  if (Array.isArray(input)) {
    for (const item of input) {
      collectGeneratedImageRelativePathsFromValue(item, out);
    }
    return out;
  }
  if (input && typeof input === "object") {
    for (const value of Object.values(input)) {
      collectGeneratedImageRelativePathsFromValue(value, out);
    }
  }
  return out;
}

function isAdminPathInsideRepo(absPath) {
  const abs = path.resolve(absPath);
  const root = path.resolve(__dirname);
  const sep = path.sep;
  return abs === root || abs.startsWith(root + sep);
}

function listDeploySyncAdminRelativePaths() {
  const paths = [
    !characterProfilesUsesSqlite() && CHARACTER_ILLUSTRATION_PROFILES_FILE,
    CHAPTER_ILLUSTRATION_STATE_FILE,
    CHARACTER_STAGE_RULES_FILE,
    CHAPTER_KEY_PEOPLE_FILE,
  ]
    .filter(Boolean)
    .filter((abs) => fs.existsSync(abs))
    .filter((abs) => isAdminPathInsideRepo(abs))
    .map((abs) => path.relative(__dirname, abs).replaceAll("\\", "/"));
  return paths;
}

function normalizeRemoteSyncBaseUrl(input) {
  const raw = String(input || "").trim().replace(/\/+$/, "");
  if (!raw) return "";
  try {
    const u = new URL(raw);
    if (u.protocol !== "http:" && u.protocol !== "https:") return "";
    return `${u.protocol}//${u.host}${u.pathname.replace(/\/+$/, "")}`;
  } catch {
    return "";
  }
}

function getRemoteSyncConfig() {
  const secrets = loadSystemSecrets();
  return {
    baseUrl: normalizeRemoteSyncBaseUrl(secrets.remoteSyncBaseUrl || ""),
    adminToken: safeText(secrets.remoteSyncAdminToken || ""),
  };
}

function maskRemoteSyncToken(token) {
  const t = safeText(token || "");
  if (!t) return "";
  return `${"*".repeat(Math.max(0, t.length - 4))}${t.slice(-4)}`;
}

function isAllowedSyncRelPath(relPath) {
  const rel = String(relPath || "").replaceAll("\\", "/").replace(/^\/+/, "");
  if (!rel || rel.includes("../")) return false;
  if (rel.startsWith("content_published/")) return rel.endsWith(".json");
  if (listDeploySyncAdminRelativePaths().includes(rel)) return true;
  if (rel.startsWith("public/generated/")) return true;
  return false;
}

function resolveGeneratedSyncRelToAbs(relPath) {
  const rel = String(relPath || "").replaceAll("\\", "/").replace(/^\/+/, "");
  if (!rel.startsWith("public/generated/")) return "";
  const tail = rel.slice("public/generated/".length);
  if (!tail || tail.includes("..")) return "";
  const full = path.resolve(path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, tail));
  const root = path.resolve(CHAPTER_ILLUSTRATION_GENERATED_DIR) + path.sep;
  if (!full.startsWith(root)) return "";
  return full;
}

function resolveSyncRelPathToAbs(relPath) {
  const rel = String(relPath || "").replaceAll("\\", "/").replace(/^\/+/, "");
  if (!isAllowedSyncRelPath(rel)) return "";
  if (rel.startsWith("public/generated/")) {
    return resolveGeneratedSyncRelToAbs(rel);
  }
  const abs = path.resolve(path.join(__dirname, rel));
  const root = path.resolve(__dirname) + path.sep;
  if (!abs.startsWith(root)) return "";
  return abs;
}

function computeFileDigest(absPath) {
  return crypto.createHash("sha256").update(fs.readFileSync(absPath)).digest("hex");
}

function buildSyncSnapshot() {
  const files = [];
  const adminRelPaths = listDeploySyncAdminRelativePaths();
  for (const rel of adminRelPaths) {
    const abs = resolveSyncRelPathToAbs(rel);
    if (!abs || !fs.existsSync(abs)) continue;
    const stat = fs.statSync(abs);
    files.push({
      rel,
      size: stat.size,
      mtimeMs: Math.round(stat.mtimeMs),
      sha256: computeFileDigest(abs),
    });
  }
  const contentFiles = walkFiles(CONTENT_PUBLISHED_DIR)
    .filter((abs) => abs.endsWith(".json"))
    .sort((a, b) => a.localeCompare(b));
  for (const abs of contentFiles) {
    const rel = path.relative(__dirname, abs).replaceAll("\\", "/");
    if (!isAllowedSyncRelPath(rel)) continue;
    const stat = fs.statSync(abs);
    files.push({
      rel,
      size: stat.size,
      mtimeMs: Math.round(stat.mtimeMs),
      sha256: computeFileDigest(abs),
    });
  }
  const generatedFiles = walkFiles(CHAPTER_ILLUSTRATION_GENERATED_DIR).sort((a, b) =>
    a.localeCompare(b)
  );
  const genRootResolved = path.resolve(CHAPTER_ILLUSTRATION_GENERATED_DIR) + path.sep;
  for (const abs of generatedFiles) {
    const resolved = path.resolve(abs);
    if (!resolved.startsWith(genRootResolved)) continue;
    const tail = path
      .relative(CHAPTER_ILLUSTRATION_GENERATED_DIR, resolved)
      .replaceAll("\\", "/");
    if (!tail || tail.startsWith("..")) continue;
    const rel = `public/generated/${tail}`;
    if (!isAllowedSyncRelPath(rel)) continue;
    const stat = fs.statSync(abs);
    files.push({
      rel,
      size: stat.size,
      mtimeMs: Math.round(stat.mtimeMs),
      sha256: computeFileDigest(abs),
    });
  }
  return {
    generatedAt: nowIso(),
    fileCount: files.length,
    files,
  };
}

function summarizeSyncRelPath(rel) {
  const pathText = String(rel || "");
  if (pathText.startsWith("content_published/")) return "已发布内容";
  if (pathText.startsWith("public/generated/thumbs/")) return "缩略图";
  if (pathText.startsWith("public/generated/")) return "插画与人物图";
  if (pathText.startsWith("admin_data/")) return "人物与插画配置";
  return "其它";
}

function compareSyncSnapshots(localSnapshot, remoteSnapshot) {
  const localFiles = Array.isArray(localSnapshot?.files) ? localSnapshot.files : [];
  const remoteFiles = Array.isArray(remoteSnapshot?.files) ? remoteSnapshot.files : [];
  const localMap = new Map(localFiles.map((x) => [String(x.rel || ""), x]));
  const remoteMap = new Map(remoteFiles.map((x) => [String(x.rel || ""), x]));
  const allRels = new Set([...localMap.keys(), ...remoteMap.keys()]);
  const onlyRemote = [];
  const onlyLocal = [];
  const different = [];
  const groupCounts = {
    remoteOnly: {},
    localOnly: {},
    different: {},
  };
  const bump = (bucket, rel) => {
    const key = summarizeSyncRelPath(rel);
    bucket[key] = (bucket[key] || 0) + 1;
  };
  for (const rel of [...allRels].sort()) {
    const localItem = localMap.get(rel);
    const remoteItem = remoteMap.get(rel);
    if (localItem && !remoteItem) {
      onlyLocal.push({ rel, local: localItem });
      bump(groupCounts.localOnly, rel);
      continue;
    }
    if (!localItem && remoteItem) {
      onlyRemote.push({ rel, remote: remoteItem });
      bump(groupCounts.remoteOnly, rel);
      continue;
    }
    if (!localItem || !remoteItem) continue;
    if (String(localItem.sha256 || "") !== String(remoteItem.sha256 || "")) {
      different.push({ rel, local: localItem, remote: remoteItem });
      bump(groupCounts.different, rel);
    }
  }
  return {
    localFileCount: localFiles.length,
    remoteFileCount: remoteFiles.length,
    onlyRemote,
    onlyLocal,
    different,
    groupCounts,
    pullPaths: [...onlyRemote.map((x) => x.rel), ...different.map((x) => x.rel)],
    pushPaths: [...onlyLocal.map((x) => x.rel), ...different.map((x) => x.rel)],
  };
}

function buildSyncPackageZipFromRelPaths(relPaths, label = "sync") {
  const normalized = Array.from(
    new Set(
      (Array.isArray(relPaths) ? relPaths : [])
        .map((x) => String(x || "").replaceAll("\\", "/").replace(/^\/+/, ""))
        .filter((x) => x && isAllowedSyncRelPath(x))
    )
  ).sort();
  const zipId = `sync_${label}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const zipPath = path.join(DEPLOY_UPLOADS_DIR, `${zipId}.zip`);
  const zip = new AdmZip();
  let addedCount = 0;
  for (const rel of normalized) {
    const abs = resolveSyncRelPathToAbs(rel);
    if (!abs || !fs.existsSync(abs)) continue;
    zip.addLocalFile(abs, path.dirname(rel), path.basename(rel));
    addedCount += 1;
  }
  zip.addFile(
    "version.json",
    Buffer.from(
      JSON.stringify(
        {
          label,
          generatedAt: nowIso(),
          fileCount: addedCount,
          scope: "content-character-illustration-sync",
        },
        null,
        2
      ),
      "utf8"
    )
  );
  zip.writeZip(zipPath);
  return { zipPath, addedCount, relPaths: normalized };
}

function applySyncZipBuffer(buffer, sourceLabel, req, authed) {
  const zip = new AdmZip(buffer);
  const entries = zip.getEntries().filter((entry) => !entry.isDirectory);
  const allowedEntries = [];
  for (const entry of entries) {
    const rel = String(entry.entryName || "").replaceAll("\\", "/").replace(/^\/+/, "");
    if (!isAllowedSyncRelPath(rel)) continue;
    allowedEntries.push({ rel, entry });
  }
  if (!allowedEntries.length) {
    throw new Error("同步包中没有可导入的内容/人物/插画文件");
  }
  const backupId = `syncbk_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const backupDir = path.join(DEPLOY_BACKUPS_DIR, backupId);
  ensureDir(backupDir);
  const manifest = [];
  let appliedCount = 0;
  for (const item of allowedEntries) {
    const dest = resolveSyncRelPathToAbs(item.rel);
    if (!dest) continue;
    if (fs.existsSync(dest) && fs.statSync(dest).isFile()) {
      const backupPath = path.join(backupDir, item.rel);
      ensureDir(path.dirname(backupPath));
      fs.copyFileSync(dest, backupPath);
      manifest.push({ rel: item.rel, hadOriginal: true });
    } else {
      manifest.push({ rel: item.rel, hadOriginal: false });
    }
    ensureDir(path.dirname(dest));
    fs.writeFileSync(dest, item.entry.getData());
    appliedCount += 1;
  }
  writeJson(path.join(backupDir, "manifest.json"), manifest);
  appendAdminAudit(req, authed, "remote_sync_import", {
    source: safeText(sourceLabel || ""),
    appliedCount,
    backupId,
  });
  return { appliedCount, backupId, relPaths: allowedEntries.map((x) => x.rel) };
}

async function fetchRemoteSyncJson(relativePath, init = {}) {
  const remote = getRemoteSyncConfig();
  if (!remote.baseUrl || !remote.adminToken) {
    throw new Error("请先配置远端站点地址与管理员 Token");
  }
  const url = `${remote.baseUrl}${relativePath.startsWith("/") ? relativePath : `/${relativePath}`}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      ...(init.headers || {}),
      Authorization: `Bearer ${remote.adminToken}`,
    },
  });
  const data = await res.json().catch(() => null);
  if (!res.ok) {
    throw new Error((data && data.error) || `远端请求失败 HTTP ${res.status}`);
  }
  return data;
}

async function fetchRemoteSyncZip(relativePath, body) {
  const remote = getRemoteSyncConfig();
  if (!remote.baseUrl || !remote.adminToken) {
    throw new Error("请先配置远端站点地址与管理员 Token");
  }
  const url = `${remote.baseUrl}${relativePath.startsWith("/") ? relativePath : `/${relativePath}`}`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${remote.adminToken}`,
    },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => null);
    throw new Error((data && data.error) || `远端下载同步包失败 HTTP ${res.status}`);
  }
  return Buffer.from(await res.arrayBuffer());
}

function shouldSkipPackageRelPath(rel, kind = "upgrade") {
  const normalized = String(rel || "").replaceAll("\\", "/");
  if (!normalized) return true;
  const commonSkips = [".git/", ".cursor/", ".DS_Store", "node_modules/"];
  if (commonSkips.some((p) => normalized.startsWith(p))) return true;
  /* 部署包永不打入密钥与本地环境（避免 zip 外流或误传仓库） */
  if (normalized === "admin_data/system_secrets.json") return true;
  if (normalized === ".env" || normalized.startsWith(".env.")) return true;
  /** 整站精简包：不含已发布经文 JSON、源码圣经数据、任务生成目录与登录/统计库 */
  if (kind === "full-slim") {
    const slimSkips = [
      "content_published/",
      "content_builds/",
      "data/",
      "admin_data/jobs/",
      "deploy-builds/",
      "admin_data/deploy/",
    ];
    if (slimSkips.some((p) => normalized.startsWith(p))) return true;
    if (
      normalized.startsWith("admin_data/auth.sqlite") ||
      normalized.startsWith("admin_data/analytics.sqlite")
    ) {
      return true;
    }
    if (normalized === "admin_data/auth.db" || normalized.startsWith("admin_data/auth/")) {
      return true;
    }
  }
  if (kind === "upgrade") {
    const upgradeSkips = [
      "admin_data/deploy/",
      "admin_data/auth.db",
      "admin_data/auth/",
      "admin_data/global_favorites.json",
      "admin_data/community_articles.json",
      "admin_data/promo_page.json",
      "admin_data/question_submissions.json",
      /* 热更新网站：不重复打包大体量目录（apply 只覆盖 zip 内文件，线上已有内容保留） */
      "content_published/",
      "content_builds/",
      "data/",
      "chapter_videos/",
      "dist-capacitor/",
      "admin_data/jobs/",
      "deploy-builds/",
    ];
    if (upgradeSkips.some((p) => normalized.startsWith(p))) return true;
    if (
      normalized.startsWith("admin_data/auth.sqlite") ||
      normalized.startsWith("admin_data/analytics.sqlite")
    ) {
      return true;
    }
  }
  return false;
}

function buildPackageZip({ kind, version }) {
  const safeKind = normalizeDeployPackageKind(kind);
  const safeVersion =
    safeText(version || "").replace(/[^\w.-]+/g, "_") || `v${Date.now()}`;
  const packageId = `pkg_${safeKind}_${Date.now()}_${Math.random()
    .toString(36)
    .slice(2, 8)}`;
  const zipPath = path.join(DEPLOY_UPLOADS_DIR, `${packageId}.zip`);
  const zip = new AdmZip();
  const rootFiles = walkFiles(__dirname);
  let addedCount = 0;

  for (const absPath of rootFiles) {
    const rel = path.relative(__dirname, absPath).replaceAll("\\", "/");
    if (shouldSkipPackageRelPath(rel, safeKind)) continue;
    if (rel.startsWith("admin_data/deploy/uploads/")) continue;
    zip.addLocalFile(absPath, path.dirname(rel), path.basename(rel));
    addedCount += 1;
  }

  zip.addFile(
    "version.json",
    Buffer.from(
      JSON.stringify(
        {
          version: safeVersion,
          packageKind: safeKind,
          generatedAt: nowIso(),
        },
        null,
        2
      ),
      "utf8"
    )
  );
  zip.writeZip(zipPath);
  return {
    zipPath,
    packageId,
    packageKind: safeKind,
    version: safeVersion,
    addedCount,
  };
}

function buildChangedChaptersPackageZip({ version, changes }) {
  const safeVersion =
    safeText(version || "").replace(/[^\w.-]+/g, "_") || `v${Date.now()}`;
  const packageId = `pkg_changed_${Date.now()}_${Math.random()
    .toString(36)
    .slice(2, 8)}`;
  const zipPath = path.join(DEPLOY_UPLOADS_DIR, `${packageId}.zip`);
  const zip = new AdmZip();
  const normalizedChanges = Array.isArray(changes)
    ? changes
        .map((x) => ({
          version: safeText(x?.version || ""),
          lang: safeText(x?.lang || ""),
          bookId: safeText(x?.bookId || ""),
          chapter: toSafeNumber(x?.chapter, 0),
        }))
        .filter((x) => x.version && x.lang && x.bookId && x.chapter > 0)
    : [];
  let addedCount = 0;
  const included = [];
  const missing = [];
  const unique = new Set();
  const seenRelPaths = new Set();
  const includedGeneratedAssets = [];
  const includedAdminFiles = [];
  const generatedAssetSet = new Set();

  for (const item of normalizedChanges) {
    const key = `${item.version}|${item.lang}|${item.bookId}|${item.chapter}`;
    if (unique.has(key)) continue;
    unique.add(key);
    const srcPath = getPublishedContentFilePath({
      versionId: item.version,
      lang: item.lang,
      bookId: item.bookId,
      chapter: item.chapter,
    });
    if (!fs.existsSync(srcPath)) {
      missing.push(item);
      continue;
    }
    const rel = path
      .relative(__dirname, srcPath)
      .replaceAll("\\", "/");
    addLocalFileToZipIfExists(zip, srcPath, rel, seenRelPaths);
    included.push(item);
    addedCount += 1;
    const data = readJson(srcPath, null);
    collectGeneratedImageRelativePathsFromValue(data, generatedAssetSet);
  }

  for (const rel of listDeploySyncAdminRelativePaths()) {
    const abs = path.join(__dirname, rel);
    if (addLocalFileToZipIfExists(zip, abs, rel, seenRelPaths)) {
      includedAdminFiles.push(rel);
      addedCount += 1;
      const data = readJson(abs, null);
      collectGeneratedImageRelativePathsFromValue(data, generatedAssetSet);
    }
  }

  for (const rel of [...generatedAssetSet].sort((a, b) => a.localeCompare(b, "en"))) {
    const abs = path.join(__dirname, rel);
    if (addLocalFileToZipIfExists(zip, abs, rel, seenRelPaths)) {
      includedGeneratedAssets.push(rel);
      addedCount += 1;
    }
  }

  zip.addFile(
    "version.json",
    Buffer.from(
      JSON.stringify(
        {
          version: safeVersion,
          packageKind: "changed",
          generatedAt: nowIso(),
          changeCount: included.length,
          generatedAssetCount: includedGeneratedAssets.length,
          adminSyncFileCount: includedAdminFiles.length,
        },
        null,
        2
      ),
      "utf8"
    )
  );
  zip.addFile(
    "changed_chapters.json",
    Buffer.from(
      JSON.stringify(
        {
          included,
          missing,
          includedAdminFiles,
          includedGeneratedAssets,
        },
        null,
        2
      ),
      "utf8"
    )
  );
  zip.writeZip(zipPath);
  return {
    zipPath,
    packageId,
    packageKind: "changed",
    version: safeVersion,
    addedCount,
    missingCount: missing.length,
    generatedAssetCount: includedGeneratedAssets.length,
    adminSyncFileCount: includedAdminFiles.length,
  };
}

function hashPassword(raw) {
  return bcrypt.hashSync(String(raw || ""), 10);
}

function verifyPassword(raw, hash) {
  const plain = String(raw || "");
  const stored = String(hash || "");
  if (!stored) return false;
  if (stored.startsWith("$2a$") || stored.startsWith("$2b$") || stored.startsWith("$2y$")) {
    return bcrypt.compareSync(plain, stored);
  }
  const legacySha = crypto.createHash("sha256").update(plain).digest("hex");
  return stored === legacySha;
}

function createUserToken() {
  return crypto.randomBytes(24).toString("hex");
}

function getAuthTokenFromReq(req) {
  const auth = safeText(req.headers.authorization || "");
  if (!auth.toLowerCase().startsWith("bearer ")) return "";
  return auth.slice(7).trim();
}

function getAuthedUserFromReq(req) {
  const token = getAuthTokenFromReq(req) || safeText(req.query?.token || "");
  if (!token) return null;
  const hit = authDb
    .prepare(
      "SELECT token, user_id, expires_at FROM user_sessions WHERE token = ? LIMIT 1"
    )
    .get(token);
  if (!hit) return null;
  if (safeText(hit.expires_at) && Date.parse(hit.expires_at) <= Date.now()) {
    authDb.prepare("DELETE FROM user_sessions WHERE token = ?").run(token);
    return null;
  }
  const user = authDb
    .prepare(
      "SELECT id, name, email, is_admin, admin_role, COALESCE(online_seconds_total, 0) AS online_seconds_total, COALESCE(color_theme_id, '') AS color_theme_id FROM users WHERE id = ? LIMIT 1"
    )
    .get(safeText(hit.user_id || ""));
  if (!user) return null;
  const adminRole = normalizeAdminRole(user.admin_role || "");
  const isAdmin =
    Number(user.is_admin || 0) === 1 || Boolean(adminRole);
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    adminRole,
    isAdmin: Boolean(isAdmin),
    totalOnlineSeconds: Number(user.online_seconds_total || 0),
    colorThemeId: safeText(user.color_theme_id || ""),
    token,
  };
}

function authedUserHasAdminAccess(authed) {
  if (!authed) return false;
  if (authed.isAdmin === true) return true;
  return Boolean(normalizeAdminRole(authed.adminRole || ""));
}

function requireAdminUser(req, res) {
  const authed = getAuthedUserFromReq(req);
  if (!authed) {
    res.status(401).json({ error: "请先登录" });
    return null;
  }
  if (!authedUserHasAdminAccess(authed)) {
    res.status(403).json({ error: "需要管理员权限" });
    return null;
  }
  return authed;
}

/**
 * 管理后台单页 HTML：直接 GET 即可返回静态文件（200），便于收藏链接与分享路径。
 * 写操作仍走 /api/admin/*，由接口层校验登录、管理员或千夫长权限；勿在 HTML 层挡 401，否则易被误认为「页面未部署」。
 */
function sendAdminToolHtmlPage(req, res, absolutePath, _qianfuzhangOnly) {
  void _qianfuzhangOnly;
  void req;
  /** 避免浏览器/CDN/SW 旁路缓存内联脚本，导致管理页 UI 与保存逻辑长期过期 */
  res.set("Cache-Control", "private, no-store, no-cache, must-revalidate, max-age=0");
  res.set("Pragma", "no-cache");
  res.sendFile(path.resolve(absolutePath), (err) => {
    if (err) {
      console.warn("[admin-tool-html]", err.message);
      res.status(404).type("text/plain; charset=utf-8").send("页面不存在");
    }
  });
}

function readClientMeta(req) {
  const userAgent = safeText(req.headers["user-agent"] || "");
  const deviceId = safeText(req.headers["x-device-id"] || "");
  const ip = getClientIp(req);
  return {
    userAgent,
    deviceId,
    ipHash: sha256Hex(ip),
  };
}

function loadRuleConfig(versionId) {
  const filePath = path.join(RULES_DIR, `${versionId}.json`);
  return readJson(filePath, null);
}

function getEnabledLanguages() {
  return (loadLanguages().languages || []).filter((x) => x.enabled);
}

function getEnabledContentVersions() {
  return (loadContentVersions().contentVersions || [])
    .filter((x) => x.enabled)
    .sort((a, b) => Number(a.order || 999) - Number(b.order || 999));
}

/* =========================================================
   书卷
   ========================================================= */
function flattenBooks() {
  return testamentOptions.flatMap((testament) =>
    testament.books.map((book) => ({
      testamentName: testament.name,
      bookId: book.usfx,
      bookCn: book.cn,
      bookCnAbbr: book.cnAbbr || book.cn,
      bookEnAbbr: book.enAbbr || book.usfx,
      bookEsAbbr: book.esAbbr || book.usfx,
      bookEn: book.en || book.cn,
      chapters: book.chapters,
      overviewOnly: book.overviewOnly === true,
    }))
  );
}

function getBookById(bookId) {
  return flattenBooks().find((b) => b.bookId === bookId) || null;
}

function getBooksByTestament(testamentName) {
  return flattenBooks().filter((b) => b.testamentName === testamentName);
}

/* =========================================================
   圣经版本管理
   ========================================================= */
function normalizeScriptureVersion(input) {
  return {
    id: safeText(input.id),
    label: safeText(input.label),
    lang: safeText(input.lang),
    enabled: toSafeBool(input.enabled, true),
    uiEnabled: toSafeBool(input.uiEnabled, true),
    contentEnabled: toSafeBool(input.contentEnabled, true),
    scriptureEnabled: toSafeBool(input.scriptureEnabled, true),
    contentMode: safeText(input.contentMode) || "native",
    sourceType: safeText(input.sourceType) || "usfx",
    sourceFile: safeText(input.sourceFile),
    description: safeText(input.description),
    sortOrder: toSafeNumber(input.sortOrder, 999),
    updatedAt: nowIso(),
  };
}

function validateScriptureVersion(version) {
  if (!isNonEmptyString(version.id)) {
    throw new Error("圣经版本缺少 id");
  }
  if (!isNonEmptyString(version.label)) {
    throw new Error("圣经版本缺少 label");
  }
  if (!isNonEmptyString(version.lang)) {
    throw new Error("圣经版本缺少 lang");
  }
  if (!isNonEmptyString(version.sourceType)) {
    throw new Error("圣经版本缺少 sourceType");
  }
  if (!isNonEmptyString(version.sourceFile)) {
    throw new Error("圣经版本缺少 sourceFile");
  }
}

function getAllScriptureVersions() {
  return (loadScriptureVersions().scriptureVersions || []).sort(
    (a, b) => Number(a.sortOrder || 999) - Number(b.sortOrder || 999)
  );
}

function getEnabledScriptureVersions() {
  return getAllScriptureVersions().filter((x) => x.enabled);
}

function getScriptureVersionConfig(versionId) {
  return getAllScriptureVersions().find((v) => v.id === versionId) || null;
}

function getPrimaryScriptureVersionByLang(lang) {
  const fixedMap = {
    zh: "cuvs_zh",
    en: "web_en",
    es: "rv1909_es",
    he: "hebrew_wlc",
  };

  const preferredId = fixedMap[String(lang || "").trim()];
  if (preferredId) {
    const hit = getEnabledScriptureVersions().find(
      (x) => x.id === preferredId && x.scriptureEnabled !== false
    );
    if (hit) return hit;
  }

  return (
    getEnabledScriptureVersions().find(
      (x) => x.lang === lang && x.scriptureEnabled !== false
    ) || null
  );
}

function resolveGenerationPrimaryScriptureVersion({ lang, preferredVersionId }) {
  const safeLang = safeText(lang || "");
  const preferredId = safeText(preferredVersionId || "");
  // Requirement: when BBE is selected as main version, still map generation scripture to WEB.
  if (safeLang === "en" && preferredId === "bbe_en") {
    const web = getScriptureVersionConfig("web_en");
    if (web) return web;
  }
  if (preferredId) {
    const preferred = getScriptureVersionConfig(preferredId);
    if (preferred && preferred.scriptureEnabled !== false) return preferred;
  }
  return getPrimaryScriptureVersionByLang(safeLang);
}

function upsertScriptureVersion(versionInput) {
  const normalized = normalizeScriptureVersion(versionInput);
  validateScriptureVersion(normalized);

  const current = loadScriptureVersions();
  const items = current.scriptureVersions || [];
  const idx = items.findIndex((x) => x.id === normalized.id);

  if (idx >= 0) {
    items[idx] = {
      ...items[idx],
      ...normalized,
    };
  } else {
    items.push({
      ...normalized,
      createdAt: nowIso(),
    });
  }

  saveScriptureVersions({ scriptureVersions: items });
  return normalized;
}

function deleteScriptureVersion(versionId) {
  const current = loadScriptureVersions();
  const items = current.scriptureVersions || [];
  const next = items.filter((x) => x.id !== versionId);

  if (next.length === items.length) {
    throw new Error("未找到要删除的圣经版本");
  }

  saveScriptureVersions({ scriptureVersions: next });
  return { deleted: true, id: versionId };
}

/* =========================================================
   USFX XML 缓存
   ========================================================= */
const xmlCache = new Map();

function loadXmlFileByPath(relativeOrAbsolutePath) {
  const filePath = path.isAbsolute(relativeOrAbsolutePath)
    ? relativeOrAbsolutePath
    : path.join(__dirname, relativeOrAbsolutePath);

  if (xmlCache.has(filePath)) return xmlCache.get(filePath);

  const xml = fs.readFileSync(filePath, "utf8");
  xmlCache.set(filePath, xml);
  return xml;
}

/* =========================================================
   XML 解析
   ========================================================= */
function stripXml(text) {
  return String(text || "")
    .replace(/<f\b[^>]*>[\s\S]*?<\/f>/g, " ")
    .replace(/<x\b[^>]*>[\s\S]*?<\/x>/g, " ")
    .replace(/<fig\b[^>]*>[\s\S]*?<\/fig>/g, " ")
    .replace(/<table\b[^>]*>[\s\S]*?<\/table>/g, " ")
    .replace(/<ref\b[^>]*>[\s\S]*?<\/ref>/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractChapter(xml, bookCode, chapter) {
  // Some USFX files close books with </book>, some rely on the next <book> tag.
  // Match current book body until next book (or file end) to support both formats.
  const bookRe = new RegExp(
    `<book\\b[^>]*id="${bookCode}"[^>]*>([\\s\\S]*?)(?=<book\\b[^>]*id="[^"]+"[^>]*>|<\\/usfx>|$)`,
    "i"
  );
  const bookMatch = xml.match(bookRe);
  if (!bookMatch) return [];

  const bookBody = bookMatch[1];
  const chapterRe = new RegExp(
    `<c\\b[^>]*id="${chapter}"[^>]*\\/>[\\s\\S]*?(?=<c\\b[^>]*id="\\d+"[^>]*\\/>|$)`,
    "i"
  );
  const chapterMatch = bookBody.match(chapterRe);
  if (!chapterMatch) return [];

  const chunk = chapterMatch[0];
  const verseRe1 = /<v\b[^>]*id="(\d+)"[^>]*\/>([\s\S]*?)<ve\/>/g;
  const verseRe2 =
    /<v\b[^>]*id="(\d+)"[^>]*\/>([\s\S]*?)(?=<v\b[^>]*id="\d+"[^>]*\/>|$)/g;

  const verses = [];
  let m;

  while ((m = verseRe1.exec(chunk)) !== null) {
    const verseNo = Number(m[1]);
    const verseText = stripXml(m[2]);
    if (verseText) verses.push({ verse: verseNo, text: verseText });
  }

  if (!verses.length) {
    while ((m = verseRe2.exec(chunk)) !== null) {
      const verseNo = Number(m[1]);
      const verseText = stripXml(m[2]);
      if (verseText) verses.push({ verse: verseNo, text: verseText });
    }
  }

  return verses;
}

/* =========================================================
   经文读取
   ========================================================= */
function getScriptureRowsForVersion({ scriptureVersionId, bookId, chapter }) {
  const scriptureConfig = getScriptureVersionConfig(scriptureVersionId);
  if (!scriptureConfig || scriptureConfig.enabled === false) {
    throw new Error(`未找到经文版本: ${scriptureVersionId}`);
  }

  const book = getBookById(bookId);
  if (!book) {
    throw new Error(`未识别书卷: ${bookId}`);
  }

  const chapterNum = Number(chapter);
  if (
    !Number.isInteger(chapterNum) ||
    chapterNum < 1 ||
    chapterNum > Number(book.chapters || 0)
  ) {
    throw new Error("章节范围不正确");
  }

  if (scriptureConfig.sourceType !== "usfx") {
    throw new Error(`暂不支持的经文源类型: ${scriptureConfig.sourceType}`);
  }

  const xml = loadXmlFileByPath(scriptureConfig.sourceFile);
  return extractChapter(xml, book.bookId, chapterNum);
}

function getMultiVersionScriptureRows({
  scriptureVersionIds,
  bookId,
  chapter,
}) {
  const ids = Array.isArray(scriptureVersionIds)
    ? scriptureVersionIds.filter(Boolean)
    : [];

  if (!ids.length) {
    throw new Error("缺少 scriptureVersionIds");
  }

  const allVersesMap = new Map();

  ids.forEach((versionId) => {
    const rows = getScriptureRowsForVersion({
      scriptureVersionId: versionId,
      bookId,
      chapter,
    });

    rows.forEach((row) => {
      const existing = allVersesMap.get(row.verse) || {
        verse: row.verse,
        texts: {},
      };
      existing.texts[versionId] = row.text;
      allVersesMap.set(row.verse, existing);
    });
  });

  return Array.from(allVersesMap.values()).sort((a, b) => a.verse - b.verse);
}

/* =========================================================
   经文全文搜索（单译本，内存索引）
   ========================================================= */
const scriptureSearchIndexCache = new Map();

function listSearchableScriptureVersionIds() {
  return getEnabledScriptureVersions()
    .filter((x) => x.uiEnabled !== false && x.scriptureEnabled !== false)
    .map((x) => x.id);
}

function verseSearchTextMatches(haystack, needle) {
  const h = String(haystack || "");
  const n = String(needle || "");
  if (!n) return false;
  if (/[^\x00-\x7F]/.test(n)) return h.includes(n);
  return h.toLowerCase().includes(n.toLowerCase());
}

function buildScriptureSearchIndex(versionId) {
  const config = getScriptureVersionConfig(versionId);
  if (!config || config.enabled === false || config.scriptureEnabled === false) {
    throw new Error("无效的经文版本");
  }
  if (config.sourceType !== "usfx") {
    throw new Error("该译本暂不支持全文搜索");
  }
  const xml = loadXmlFileByPath(config.sourceFile);
  const rows = [];
  for (const book of flattenBooks()) {
    const maxCh = Number(book.chapters || 0);
    for (let ch = 1; ch <= maxCh; ch++) {
      const verses = extractChapter(xml, book.bookId, ch);
      for (const v of verses) {
        rows.push({
          bookId: book.bookId,
          bookCn: book.bookCn,
          testamentName: book.testamentName,
          chapter: ch,
          verse: v.verse,
          text: v.text,
        });
      }
    }
  }
  return rows;
}

function getScriptureSearchIndex(versionId) {
  if (scriptureSearchIndexCache.has(versionId)) {
    return scriptureSearchIndexCache.get(versionId);
  }
  const index = buildScriptureSearchIndex(versionId);
  scriptureSearchIndexCache.set(versionId, index);
  return index;
}

function scriptureSearchSnippet(text, needle, maxLen) {
  const t = String(text || "");
  const n = String(needle || "");
  const len = Math.min(120, Math.max(24, Number(maxLen) || 56));
  if (!n) return t.length > len ? `${t.slice(0, len)}…` : t;
  let i = -1;
  if (/[^\x00-\x7F]/.test(n)) {
    i = t.indexOf(n);
  } else {
    const low = t.toLowerCase();
    const sub = n.toLowerCase();
    i = low.indexOf(sub);
  }
  if (i < 0) return t.length > len ? `${t.slice(0, len)}…` : t;
  const half = Math.floor(len / 2);
  let start = Math.max(0, i - half);
  let end = Math.min(t.length, start + len);
  if (end - start < len) start = Math.max(0, end - len);
  let s = t.slice(start, end);
  if (start > 0) s = `…${s}`;
  if (end < t.length) s = `${s}…`;
  return s;
}

function searchScriptureVersesInIndex(index, needle, scope, limit) {
  const n = safeText(needle);
  if (!n) return [];
  const scopeFilter = (row) => {
    if (scope === "ot") return row.testamentName === "旧约";
    if (scope === "nt") return row.testamentName === "新约";
    return true;
  };
  const cap = Math.min(120, Math.max(1, Number(limit) || 40));
  const out = [];
  for (const row of index) {
    if (!scopeFilter(row)) continue;
    if (!verseSearchTextMatches(row.text, n)) continue;
    out.push(row);
    if (out.length >= cap) break;
  }
  return out;
}

/* =========================================================
   内容路径
   ========================================================= */
function getBuildContentFilePath({
  buildId,
  versionId,
  lang,
  bookId,
  chapter,
}) {
  return path.join(
    CONTENT_BUILDS_DIR,
    buildId,
    versionId,
    lang,
    bookId,
    `${chapter}.json`
  );
}

function getPublishedContentFilePath({ versionId, lang, bookId, chapter }) {
  return path.join(
    CONTENT_PUBLISHED_DIR,
    versionId,
    lang,
    bookId,
    `${chapter}.json`
  );
}

function getPublishedBookIntroFilePath({ versionId, lang, bookId }) {
  return path.join(
    CONTENT_PUBLISHED_DIR,
    versionId,
    lang,
    bookId,
    "book_intro.json"
  );
}

function readPublishedBookIntro({ versionId, lang, bookId }) {
  const filePath = getPublishedBookIntroFilePath({ versionId, lang, bookId });
  const data = readJson(filePath, null);
  if (!data || typeof data !== "object") return null;
  return {
    markdown: String(data.markdown ?? "").slice(0, BOOK_INTRO_MAX_MARKDOWN),
    updatedAt: safeText(data.updatedAt || ""),
  };
}

function readPublishedContent({ versionId, lang, bookId, chapter }) {
  const filePath = getPublishedContentFilePath({
    versionId,
    lang,
    bookId,
    chapter,
  });
  return readJson(filePath, null);
}

/**
 * 列出 content_published 下所有已存在的某书某章 JSON（任意内容版本 × 输出语言）。
 */
function listPublishedChapterFilesForBookChapter(bookId, chapter) {
  const out = [];
  const vids = listPublishedContentVersionIds();
  for (const vid of vids) {
    const base = path.join(CONTENT_PUBLISHED_DIR, vid);
    let langs = [];
    try {
      langs = fs
        .readdirSync(base, { withFileTypes: true })
        .filter((d) => d.isDirectory())
        .map((d) => d.name);
    } catch {
      continue;
    }
    for (const langId of langs) {
      const p = getPublishedContentFilePath({
        versionId: vid,
        lang: langId,
        bookId,
        chapter,
      });
      if (fs.existsSync(p)) {
        out.push({ path: p, versionId: vid, lang: langId });
      }
    }
  }
  return out;
}

/**
 * 章节插图全站共用：当前 JSON 无插图时，从同书同章任意已发布文件按优先级借用 chapterIllustration。
 * 优先级：default/zh → default/当前语言 → 其余 default/* → 其他版本 zh → 其余。
 */
function mergeChapterIllustrationFromCanonicalIfMissing(
  data,
  versionId,
  lang,
  bookId,
  chapter
) {
  if (!data || typeof data !== "object") return data;
  const ill = normalizeChapterIllustrationForSave(data.chapterIllustration);
  if (ill) return data;

  /* 1) 管理端插图几乎总写在 default/zh：优先直读，避免下方循环里「跳过当前请求版本」误伤唯一有图文件 */
  const zhCanon = readPublishedContent({
    versionId: "default",
    lang: "zh",
    bookId,
    chapter,
  });
  const zhCanonIll = normalizeChapterIllustrationForSave(zhCanon?.chapterIllustration);
  if (zhCanonIll) {
    return { ...data, chapterIllustration: zhCanonIll };
  }

  const files = listPublishedChapterFilesForBookChapter(bookId, chapter);
  const reqLang = String(lang);
  const priority = (t) => {
    const v = String(t.versionId);
    const l = String(t.lang);
    if (v === "default" && l === "zh") return 0;
    if (v === "default" && l === reqLang) return 1;
    if (v === "default") return 2;
    if (l === "zh") return 3;
    return 4;
  };
  files.sort((a, b) => {
    const d = priority(a) - priority(b);
    if (d !== 0) return d;
    const cv = String(a.versionId).localeCompare(String(b.versionId));
    if (cv !== 0) return cv;
    return String(a.lang).localeCompare(String(b.lang));
  });

  /* 不再 skip「当前 version/lang」：若该文件是唯一带图的 JSON，skip 会导致永远借不到图 */
  for (const t of files) {
    const fallback = readPublishedContent({
      versionId: t.versionId,
      lang: t.lang,
      bookId,
      chapter,
    });
    const fbIll = normalizeChapterIllustrationForSave(fallback?.chapterIllustration);
    if (fbIll) return { ...data, chapterIllustration: fbIll };
  }
  return data;
}

function listPublishedContentVersionIds() {
  try {
    return fs
      .readdirSync(CONTENT_PUBLISHED_DIR, { withFileTypes: true })
      .filter((d) => d.isDirectory())
      .map((d) => d.name);
  } catch {
    return [];
  }
}

function getBookLabelById(bookId) {
  const target = String(bookId || "").trim();
  if (!target) return "";
  for (const testament of testamentOptions) {
    const books = Array.isArray(testament?.books) ? testament.books : [];
    for (const book of books) {
      if (String(book?.usfx || "").trim() === target) {
        return String(book?.cn || book?.en || target).trim();
      }
    }
  }
  return target;
}

function isDivineSpeechVerseText(rawText) {
  const text = safeText(rawText);
  if (!text) return false;
  const patterns = [
    /耶和华说/u,
    /主说/u,
    /神说/u,
    /耶稣说/u,
    /圣灵说/u,
    /耶和华如此说/u,
    /主如此说/u,
    /神如此说/u,
    /耶和华晓谕/u,
    /主晓谕/u,
    /神晓谕/u,
    /耶和华吩咐/u,
    /主吩咐/u,
    /神吩咐/u,
    /耶和华向.*显现.*说/u,
    /主向.*显现.*说/u,
    /神向.*显现.*说/u,
    /耶和华向.*显现/u,
    /主向.*显现/u,
    /神向.*显现/u,
    /耶和华临到.*说/u,
    /主临到.*说/u,
    /神临到.*说/u,
    /耶和华应许.*说/u,
    /主应许.*说/u,
    /神应许.*说/u,
    /耶和华对.*说/u,
    /主对.*说/u,
    /神对.*说/u,
    /耶稣对.*说/u,
    /耶和华曰/u,
    /主曰/u,
    /神曰/u,
    /耶和华云/u,
    /主云/u,
    /神云/u,
    /\bthe\s+lord\s+said\b/i,
    /\bgod\s+said\b/i,
    /\bjesus\s+said\b/i,
    /\bthus\s+says?\s+the\s+lord\b/i,
    /\bthe\s+lord\s+spoke\b/i,
    /\bgod\s+spoke\b/i,
    /\bthe\s+lord\s+spoke\s+to\b/i,
    /\bgod\s+spoke\s+to\b/i,
    /\bjesus\s+spoke\s+to\b/i,
    /\bel\s+senor\s+dijo\b/i,
    /\bdios\s+dijo\b/i,
    /\bjesus\s+dijo\b/i,
    /\bas[ií]\s+dice\s+el\s+se[nñ]or\b/i,
    /\bel\s+se[nñ]or\s+habl[oó]\b/i,
    /\bdios\s+habl[oó]\b/i,
    /\bel\s+se[nñ]or\s+habl[oó]\s+a\b/i,
    /\bdios\s+habl[oó]\s+a\b/i,
  ];
  return patterns.some((re) => re.test(text));
}

function getQuoteDelta(rawText) {
  const text = safeText(rawText);
  const openCount = (text.match(/[「『“‘«﹁〝]/g) || []).length;
  const closeCount = (text.match(/[」』”’»﹂〞]/g) || []).length;
  return openCount - closeCount;
}

function hasClosingQuote(rawText) {
  return /[」』”’»﹂〞]/.test(safeText(rawText));
}

function hasOpeningQuote(rawText) {
  return /[「『“‘«﹁〝]/.test(safeText(rawText));
}

function buildDivineSpeechVerseSetFromRows(rows) {
  const sorted = [...(rows || [])].sort(
    (a, b) => Number(a?.verse || 0) - Number(b?.verse || 0)
  );
  const set = new Set();
  let inDivineQuote = false;
  let quoteBalance = 0;
  let pendingTailCarry = 0;

  for (const row of sorted) {
    const verseNo = Number(row?.verse || 0);
    const text = safeText(row?.text || "");
    if (!verseNo || !text) continue;

    const trigger = isDivineSpeechVerseText(text);
    if (trigger || inDivineQuote || pendingTailCarry > 0) {
      set.add(verseNo);
    }

    const delta = getQuoteDelta(text);
    if (trigger && delta > 0) {
      inDivineQuote = true;
      quoteBalance = delta;
      pendingTailCarry = 0;
      continue;
    }
    if (trigger && !hasClosingQuote(text)) {
      inDivineQuote = true;
      quoteBalance = hasOpeningQuote(text) ? Math.max(1, delta) : 1;
      pendingTailCarry = 0;
      continue;
    }
    if (inDivineQuote) {
      quoteBalance += delta;
      if (quoteBalance <= 0 && hasClosingQuote(text)) {
        inDivineQuote = false;
        quoteBalance = 0;
        pendingTailCarry = 0;
      }
      continue;
    }
    if (trigger && hasClosingQuote(text)) {
      pendingTailCarry = 0;
      continue;
    }
    if (pendingTailCarry > 0) {
      pendingTailCarry -= 1;
      continue;
    }
    if (trigger) {
      pendingTailCarry = 1;
    }
  }

  return set;
}

/* =========================================================
   内容保存 / 发布
   ========================================================= */
function isSafeChapterVideoId(id) {
  return /^[a-f0-9]{8,64}$/i.test(String(id || ""));
}

function normalizeChapterVideosForSave(raw) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const v of raw) {
    if (!v || typeof v !== "object") continue;
    const id = safeText(v.id || "");
    if (!isSafeChapterVideoId(id)) continue;
    const ext = safeText(v.ext || "").toLowerCase();
    if (ext !== "mp4" && ext !== "webm") continue;
    const mime = ext === "webm" ? "video/webm" : "video/mp4";
    const posterUrl = safeText(v.posterUrl || v.poster || "").slice(0, 800);
    const posterUpdatedAt = safeText(v.posterUpdatedAt || "").slice(0, 80);
    const row = {
      id,
      title: safeText(v.title || "").slice(0, 200),
      mime,
      ext,
      updatedAt: safeText(v.updatedAt || "") || nowIso(),
    };
    if (posterUrl) {
      row.posterUrl = posterUrl;
      if (posterUpdatedAt) row.posterUpdatedAt = posterUpdatedAt;
    }
    out.push(row);
  }
  return out;
}

function getChapterVideosDir(versionId, lang, bookId, chapter) {
  return path.join(
    CHAPTER_VIDEOS_DIR,
    safeText(versionId),
    safeText(lang),
    safeText(bookId),
    String(Number(chapter))
  );
}

/**
 * 卷首页视频写入 `0.json`；若尚无文件则创建最小占位（与读经页 chapter=0 时拉取 chapterVideos 一致）。
 */
function ensurePublishedJsonForBookLandingVideos(versionId, lang, bookId) {
  const publishedPath = getPublishedContentFilePath({
    versionId,
    lang,
    bookId,
    chapter: 0,
  });
  if (fs.existsSync(publishedPath)) {
    const data = readJson(publishedPath, null);
    return data && typeof data === "object" ? data : null;
  }
  const bookMeta = getBookById(bookId);
  const stub = normalizeStudyContentForSave({
    version: versionId,
    versionLabel: "",
    contentLang: lang,
    bookId,
    bookLabel: safeText(bookMeta?.bookCn || bookId),
    chapter: 0,
    theme: "",
    repeatedWords: [],
    segments: [],
    chapterLeaderHint: [],
    chapterVideos: [],
    title: "",
    closing: "",
  });
  writeJson(publishedPath, stub);
  return readJson(publishedPath, null);
}

function resolveChapterVideoFilePath(versionId, lang, bookId, chapter, id) {
  if (!isSafeChapterVideoId(id)) return null;
  const resolvedDir = path.resolve(
    getChapterVideosDir(versionId, lang, bookId, chapter)
  );
  const root = path.resolve(CHAPTER_VIDEOS_DIR);
  if (resolvedDir !== root && !resolvedDir.startsWith(root + path.sep)) {
    return null;
  }
  const base = String(id).toLowerCase();
  for (const ext of ["mp4", "webm"]) {
    const p = path.resolve(path.join(resolvedDir, `${base}.${ext}`));
    if (p !== resolvedDir && !p.startsWith(resolvedDir + path.sep)) continue;
    try {
      if (fs.existsSync(p) && fs.statSync(p).isFile()) {
        return { filePath: p, ext };
      }
    } catch {
      /* ignore */
    }
  }
  return null;
}

function moveUploadedFileToFinal(src, dest) {
  try {
    fs.renameSync(src, dest);
  } catch (e) {
    if (e && e.code === "EXDEV") {
      fs.copyFileSync(src, dest);
      try {
        fs.unlinkSync(src);
      } catch {
        /* ignore */
      }
    } else {
      throw e;
    }
  }
}

function assertValidPublishedBookChapter(bookId, chapter) {
  const bm = getBookById(bookId);
  if (!bm) return "无效书卷";
  const n = Number(bm.chapters);
  const ch = Number(chapter);
  if (!Number.isFinite(ch) || ch < 0 || !Number.isInteger(ch)) {
    return "无效章节";
  }
  if (n === 0) {
    if (ch !== 0) return "该卷仅有卷首页（章 0）";
    return null;
  }
  if (ch > n) return `章节不能超过 ${n}`;
  return null;
}

function moveChapterVideoPosterFilesBetweenDirs(fromDir, toDir, id) {
  const base = String(id).toLowerCase();
  const prefix = `${base}.poster.`;
  let names;
  try {
    names = fs.readdirSync(fromDir);
  } catch {
    return;
  }
  fs.mkdirSync(toDir, { recursive: true });
  for (const name of names) {
    if (!name.startsWith(prefix)) continue;
    const fp = path.join(fromDir, name);
    const tp = path.join(toDir, name);
    try {
      if (fs.existsSync(tp)) {
        try {
          fs.unlinkSync(tp);
        } catch {
          /* ignore */
        }
      }
      moveUploadedFileToFinal(fp, tp);
    } catch {
      /* ignore single file */
    }
  }
}

function chapterVideoPosterApiPath(versionId, lang, bookId, chapter, id) {
  const q = new URLSearchParams({
    version: safeText(versionId),
    lang: safeText(lang),
    bookId: safeText(bookId),
    chapter: String(Number(chapter)),
    id: safeText(id),
  });
  return `/api/published/chapter-video-poster?${q.toString()}`;
}

function resolveChapterVideoPosterFilePath(versionId, lang, bookId, chapter, id) {
  if (!isSafeChapterVideoId(id)) return null;
  const resolvedDir = path.resolve(
    getChapterVideosDir(versionId, lang, bookId, chapter)
  );
  const root = path.resolve(CHAPTER_VIDEOS_DIR);
  if (resolvedDir !== root && !resolvedDir.startsWith(root + path.sep)) {
    return null;
  }
  const base = String(id).toLowerCase();
  for (const ext of ["jpg", "jpeg", "png", "webp"]) {
    const p = path.resolve(path.join(resolvedDir, `${base}.poster.${ext}`));
    if (p !== resolvedDir && !p.startsWith(resolvedDir + path.sep)) continue;
    try {
      if (fs.existsSync(p) && fs.statSync(p).isFile()) return p;
    } catch {
      /* ignore */
    }
  }
  return null;
}

function unlinkChapterVideoPosterFiles(versionId, lang, bookId, chapter, id) {
  const resolvedDir = path.resolve(
    getChapterVideosDir(versionId, lang, bookId, chapter)
  );
  const root = path.resolve(CHAPTER_VIDEOS_DIR);
  if (resolvedDir !== root && !resolvedDir.startsWith(root + path.sep)) return;
  const base = String(id).toLowerCase();
  for (const ext of ["jpg", "jpeg", "png", "webp"]) {
    const p = path.join(resolvedDir, `${base}.poster.${ext}`);
    try {
      if (fs.existsSync(p)) fs.unlinkSync(p);
    } catch {
      /* ignore */
    }
  }
}

function listChapterVideosOverview(versionId, lang) {
  const v = safeText(versionId);
  const l = safeText(lang);
  const base = path.join(CONTENT_PUBLISHED_DIR, v, l);
  if (!fs.existsSync(base)) return [];
  const out = [];
  let bookEntries;
  try {
    bookEntries = fs.readdirSync(base, { withFileTypes: true });
  } catch {
    return [];
  }
  for (const d of bookEntries) {
    if (!d.isDirectory()) continue;
    const bookId = d.name;
    /* 概论等书卷目录名为 _OT_OVERVIEW、_BIBLE_INTRO 等，须参与扫描；仅跳过隐藏目录 */
    if (bookId.startsWith(".")) continue;
    const bookPath = path.join(base, bookId);
    let files;
    try {
      files = fs.readdirSync(bookPath);
    } catch {
      continue;
    }
    for (const f of files) {
      const m = /^(\d+)\.json$/i.exec(f);
      if (!m) continue;
      const chapter = Number(m[1]);
      if (!Number.isFinite(chapter) || chapter < 0) continue;
      const fp = path.join(bookPath, f);
      const data = readJson(fp, null);
      if (!data || typeof data !== "object") continue;
      if (!Array.isArray(data.chapterVideos) || !data.chapterVideos.length) {
        continue;
      }
      const bookLabel = safeText(data.bookLabel || "");
      const bookMeta = getBookById(bookId);
      const bookCn = bookLabel || bookMeta?.bookCn || bookId;
      for (const raw of data.chapterVideos) {
        const norm = normalizeChapterVideosForSave([raw])[0];
        if (!norm) continue;
        out.push({
          version: v,
          lang: l,
          bookId,
          bookCn,
          chapter,
          ...norm,
        });
      }
    }
  }
  out.sort((a, b) => {
    const bc = String(a.bookId).localeCompare(String(b.bookId));
    if (bc !== 0) return bc;
    if (a.chapter !== b.chapter) return a.chapter - b.chapter;
    return String(a.title || "").localeCompare(String(b.title || ""));
  });
  return out;
}

/**
 * 插画地址存成站点相对路径 `/generated/...`，避免本地管理页写入
 * `http://127.0.0.1:3000/generated/...` 后线上读经页无法加载。
 */
function normalizeIllustrationImageUrlForPublication(raw) {
  const s = safeText(raw);
  if (!s) return "";
  if (!/^https?:\/\//i.test(s)) {
    return s.startsWith("/") ? s : "/" + s.replace(/^\/+/, "/");
  }
  try {
    const u = new URL(s);
    const host = u.hostname.toLowerCase();
    if (host === "localhost" || host === "127.0.0.1" || host === "::1") {
      return u.pathname + u.search;
    }
    if (u.pathname.startsWith("/generated/")) {
      return u.pathname + u.search;
    }
  } catch (_) {}
  return s;
}

function normalizeChapterIllustrationForSave(raw) {
  if (!raw || typeof raw !== "object") return null;
  const imageUrl = normalizeIllustrationImageUrlForPublication(raw.imageUrl || "");
  if (!imageUrl) return null;
  return {
    imageUrl,
    updatedAt: safeText(raw.updatedAt || ""),
  };
}

function normalizeChapterIllustrationInPublishedReadPayload(data) {
  if (!data || typeof data !== "object") return data;
  const ill = normalizeChapterIllustrationForSave(data.chapterIllustration);
  if (ill) {
    return { ...data, chapterIllustration: ill };
  }
  if (data.chapterIllustration != null) {
    const { chapterIllustration, ...rest } = data;
    return rest;
  }
  return data;
}

function normalizeStudyContentForSave(input) {
  const out = {
    version: safeText(input.version),
    versionLabel: safeText(input.versionLabel),
    contentLang: safeText(input.contentLang),
    bookId: safeText(input.bookId),
    bookLabel: safeText(input.bookLabel),
    chapter: Number(input.chapter || 0),
    theme: safeText(input.theme),
    repeatedWords: Array.isArray(input.repeatedWords)
      ? input.repeatedWords
      : [],
    segments: Array.isArray(input.segments) ? input.segments : [],
    chapterLeaderHint: Array.isArray(input.chapterLeaderHint)
      ? input.chapterLeaderHint
      : [],
    closing: safeText(input.closing),
    title: safeText(input.title),
    generatedAt: safeText(input.generatedAt) || nowIso(),
    savedAt: nowIso(),
    chapterVideos: normalizeChapterVideosForSave(input.chapterVideos),
  };
  const slotZh = sanitizeCharacterFigurePortraitSlotByZh(
    input.characterFigurePortraitSlotByZh
  );
  if (slotZh && Object.keys(slotZh).length > 0) {
    out.characterFigurePortraitSlotByZh = slotZh;
  }
  const chapterKeyPeople = sanitizeChapterKeyPeopleArray(input.chapterKeyPeople);
  if (chapterKeyPeople.length > 0) {
    out.chapterKeyPeople = chapterKeyPeople;
  }
  const ill = normalizeChapterIllustrationForSave(input.chapterIllustration);
  if (ill) out.chapterIllustration = ill;
  return out;
}

function saveStudyContentToBuild(studyContent, buildId) {
  const normalized = normalizeStudyContentForSave(studyContent);

  if (!isNonEmptyString(normalized.version)) {
    throw new Error("保存失败：缺少 version");
  }
  if (!isNonEmptyString(normalized.contentLang)) {
    throw new Error("保存失败：缺少 contentLang");
  }
  if (!isNonEmptyString(normalized.bookId)) {
    throw new Error("保存失败：缺少 bookId");
  }
  if (!Number.isInteger(normalized.chapter) || normalized.chapter < 1) {
    throw new Error("保存失败：chapter 不正确");
  }

  const filePath = getBuildContentFilePath({
    buildId,
    versionId: normalized.version,
    lang: normalized.contentLang,
    bookId: normalized.bookId,
    chapter: normalized.chapter,
  });

  writeJson(filePath, normalized);
  return { filePath, savedContent: normalized };
}

function mergePublishOneChapter(studyContent) {
  const hadChapterVideosKey =
    studyContent &&
    typeof studyContent === "object" &&
    Object.prototype.hasOwnProperty.call(studyContent, "chapterVideos");
  const hadChapterIllustrationKey =
    studyContent &&
    typeof studyContent === "object" &&
    Object.prototype.hasOwnProperty.call(studyContent, "chapterIllustration");
  const hadCharacterFigureSlotKey =
    studyContent &&
    typeof studyContent === "object" &&
    Object.prototype.hasOwnProperty.call(
      studyContent,
      "characterFigurePortraitSlotByZh"
    );
  const hadChapterKeyPeopleKey =
    studyContent &&
    typeof studyContent === "object" &&
    Object.prototype.hasOwnProperty.call(studyContent, "chapterKeyPeople");
  const normalized = normalizeStudyContentForSave(studyContent);

  const filePath = getPublishedContentFilePath({
    versionId: normalized.version,
    lang: normalized.contentLang,
    bookId: normalized.bookId,
    chapter: normalized.chapter,
  });

  const existing = readJson(filePath, null);
  if (
    !hadChapterVideosKey &&
    existing &&
    Array.isArray(existing.chapterVideos) &&
    existing.chapterVideos.length > 0
  ) {
    normalized.chapterVideos = normalizeChapterVideosForSave(
      existing.chapterVideos
    );
  }
  if (
    !hadChapterIllustrationKey &&
    existing &&
    normalizeChapterIllustrationForSave(existing.chapterIllustration)
  ) {
    normalized.chapterIllustration = normalizeChapterIllustrationForSave(
      existing.chapterIllustration
    );
  }
  if (
    !hadCharacterFigureSlotKey &&
    existing &&
    existing.characterFigurePortraitSlotByZh &&
    typeof existing.characterFigurePortraitSlotByZh === "object"
  ) {
    const kept = sanitizeCharacterFigurePortraitSlotByZh(
      existing.characterFigurePortraitSlotByZh
    );
    if (Object.keys(kept).length > 0) {
      normalized.characterFigurePortraitSlotByZh = kept;
    }
  }
  if (
    !hadChapterKeyPeopleKey &&
    existing &&
    Array.isArray(existing.chapterKeyPeople) &&
    existing.chapterKeyPeople.length > 0
  ) {
    const kept = sanitizeChapterKeyPeopleArray(existing.chapterKeyPeople);
    if (kept.length > 0) {
      normalized.chapterKeyPeople = kept;
    }
  }

  writeJson(filePath, normalized);
  return filePath;
}

/** 批量任务逐章发布时更新 published.json（不整批重算） */
function bumpPublishedMergeMetaForOneChapter(versionId, lang, buildId) {
  const published = loadPublished();
  if (!published[versionId]) published[versionId] = {};
  if (!published[versionId][lang]) published[versionId][lang] = {};
  const row = published[versionId][lang];
  row.publishMode = "merge";
  row.lastMergedBuildId = buildId;
  row.publishedAt = nowIso();
  row.publishedCount = (row.publishedCount || 0) + 1;
  savePublished(published);
}

function getStudyContentHash(studyContent) {
  const normalized = normalizeStudyContentForSave(studyContent);
  return sha256Hex(JSON.stringify(normalized));
}

function compareBuildChapterWithPublished({
  buildId,
  versionId,
  lang,
  bookId,
  chapter,
}) {
  const sourcePath = getBuildContentFilePath({
    buildId,
    versionId,
    lang,
    bookId,
    chapter,
  });
  if (!fs.existsSync(sourcePath)) {
    return { existsInBuild: false, changed: false, sourcePath };
  }
  const buildContent = readJson(sourcePath, null);
  if (!buildContent) {
    return { existsInBuild: false, changed: false, sourcePath };
  }
  const publishedPath = getPublishedContentFilePath({
    versionId,
    lang,
    bookId,
    chapter,
  });
  const publishedExists = fs.existsSync(publishedPath);
  const publishedContent = readJson(publishedPath, null);
  const buildHash = getStudyContentHash(buildContent);
  const publishedHash = publishedContent ? getStudyContentHash(publishedContent) : "";
  const changed = !publishedHash || buildHash !== publishedHash;
  return {
    existsInBuild: true,
    changed,
    publishedExists,
    buildHash,
    publishedHash,
    sourcePath,
    publishedPath,
    buildContent,
  };
}

/** 比较内存中的章节与读者端已发布文件（用于批量生成后逐章发布） */
function compareStudyContentWithPublished(studyContent) {
  const normalized = normalizeStudyContentForSave(studyContent);
  const publishedPath = getPublishedContentFilePath({
    versionId: normalized.version,
    lang: normalized.contentLang,
    bookId: normalized.bookId,
    chapter: normalized.chapter,
  });
  const publishedExists = fs.existsSync(publishedPath);
  const publishedContent = readJson(publishedPath, null);
  const newHash = getStudyContentHash(normalized);
  const publishedHash = publishedContent ? getStudyContentHash(publishedContent) : "";
  const changed = !publishedHash || newHash !== publishedHash;
  return {
    publishedExists,
    changed,
    newHash,
    publishedHash,
    publishedPath,
  };
}

async function mergePublishFromBuild({
  buildId,
  versionId,
  lang,
  targets,
  onlyChanged = false,
  dryRun = false,
  runId = "",
  skipReplacesExisting = false,
}) {
  let publishedCount = 0;
  let skippedCount = 0;
  let skippedWouldReplaceCount = 0;
  const changedTargets = [];
  const skippedTargets = [];
  let stepCount = 0;

  for (const target of targets) {
    if (runId) {
      assertPublishRunNotCancelled(runId);
      stepCount += 1;
      if (stepCount % 25 === 0) {
        await nextTickAsync();
      }
    }
    if (target.versionId !== versionId || target.lang !== lang) continue;
    const compare = compareBuildChapterWithPublished({
      buildId,
      versionId,
      lang,
      bookId: target.bookId,
      chapter: target.chapter,
    });
    if (!compare.existsInBuild) continue;

    const isChanged = Boolean(compare.changed);
    if (onlyChanged && !isChanged) {
      skippedCount += 1;
      skippedTargets.push({
        bookId: target.bookId,
        chapter: target.chapter,
        reason: "unchanged",
      });
      continue;
    }
    const replacesExisting = Boolean(compare.publishedExists && isChanged);
    if (skipReplacesExisting && replacesExisting) {
      skippedCount += 1;
      skippedWouldReplaceCount += 1;
      skippedTargets.push({
        bookId: target.bookId,
        chapter: target.chapter,
        reason: "would_replace",
      });
      continue;
    }
    changedTargets.push({
      bookId: target.bookId,
      chapter: target.chapter,
      changed: isChanged,
      replacesExisting,
    });
    if (!dryRun) {
      mergePublishOneChapter(compare.buildContent);
    }
    publishedCount += 1;
  }

  if (dryRun) {
    return {
      published: loadPublished(),
      publishedCount,
      skippedCount,
      skippedWouldReplaceCount,
      changedTargets,
      skippedTargets,
      dryRun: true,
      skipReplacesExisting: Boolean(skipReplacesExisting),
    };
  }

  const published = loadPublished();
  if (!published[versionId]) published[versionId] = {};
  if (!published[versionId][lang]) published[versionId][lang] = {};

  published[versionId][lang].publishMode = "merge";
  published[versionId][lang].lastMergedBuildId = buildId;
  published[versionId][lang].publishedAt = nowIso();
  published[versionId][lang].publishedCount =
    (published[versionId][lang].publishedCount || 0) + publishedCount;

  savePublished(published);

  return {
    published,
    publishedCount,
    skippedCount,
    skippedWouldReplaceCount,
    changedTargets,
    skippedTargets,
    skipReplacesExisting: Boolean(skipReplacesExisting),
  };
}

/** 将某次批量任务已写入 content_builds 的章节合并进 content_published（用于取消/未完成且未自动发布） */
async function mergePublishFromJobBuild(
  job,
  { onlyChanged = true, dryRun = false, skipReplacesExisting = false } = {}
) {
  if (!job || !isNonEmptyString(job.buildId)) {
    throw new Error("任务缺少 buildId");
  }
  if (!Array.isArray(job.targets) || job.targets.length === 0) {
    throw new Error("任务没有目标列表");
  }
  const done = Math.max(0, Number(job.done || 0));
  if (done < 1) {
    throw new Error("该任务尚无已生成章节（进度为 0），无可发布内容");
  }
  const touchedPairs = new Set(
    job.targets.map((x) => `${safeText(x.versionId)}__${safeText(x.lang)}`)
  );
  const details = [];
  let totalPublishedCount = 0;
  let totalSkippedCount = 0;
  let totalSkippedWouldReplaceCount = 0;
  for (const pair of touchedPairs) {
    const [versionId, lang] = pair.split("__");
    if (!versionId || !lang) continue;
    const result = await mergePublishFromBuild({
      buildId: job.buildId,
      versionId,
      lang,
      targets: job.targets,
      onlyChanged,
      dryRun,
      skipReplacesExisting,
    });
    totalPublishedCount += Number(result.publishedCount || 0);
    totalSkippedCount += Number(result.skippedCount || 0);
    totalSkippedWouldReplaceCount += Number(result.skippedWouldReplaceCount || 0);
    details.push({
      versionId,
      lang,
      publishedCount: result.publishedCount,
      skippedCount: result.skippedCount,
      skippedWouldReplaceCount: result.skippedWouldReplaceCount || 0,
      changedTargets: result.changedTargets || [],
      skippedTargets: result.skippedTargets || [],
    });
  }
  return {
    jobId: job.id,
    buildId: job.buildId,
    totalPublishedCount,
    totalSkippedCount,
    totalSkippedWouldReplaceCount,
    details,
    onlyChanged: Boolean(onlyChanged),
    dryRun: Boolean(dryRun),
    skipReplacesExisting: Boolean(skipReplacesExisting),
  };
}

function listJobsEligibleForPartialMergePublish() {
  return listAllJobsNewestFirst()
    .filter((job) => {
      if (!isNonEmptyString(job.buildId)) return false;
      if (!Array.isArray(job.targets) || job.targets.length === 0) return false;
      const done = Math.max(0, Number(job.done || 0));
      if (done < 1) return false;
      if (job.status === "cancelled") return true;
      if (job.status === "completed" && job.autoPublish !== true) return true;
      return false;
    })
    .sort((a, b) =>
      String(a.createdAt || "").localeCompare(String(b.createdAt || ""))
    );
}

/** 按任务创建时间从旧到新依次合并，同一章多任务时以后处理的为准 */
async function mergePublishAllPartialJobBuilds({
  onlyChanged = true,
  dryRun = false,
  skipReplacesExisting = false,
} = {}) {
  const jobs = listJobsEligibleForPartialMergePublish();
  const results = [];
  let totalPublishedCount = 0;
  let totalSkippedCount = 0;
  let totalSkippedWouldReplaceCount = 0;
  for (const job of jobs) {
    const r = await mergePublishFromJobBuild(job, {
      onlyChanged,
      dryRun,
      skipReplacesExisting,
    });
    totalPublishedCount += r.totalPublishedCount;
    totalSkippedCount += r.totalSkippedCount;
    totalSkippedWouldReplaceCount += Number(r.totalSkippedWouldReplaceCount || 0);
    results.push(r);
  }
  return {
    jobCount: jobs.length,
    totalPublishedCount,
    totalSkippedCount,
    totalSkippedWouldReplaceCount,
    results,
    onlyChanged: Boolean(onlyChanged),
    dryRun: Boolean(dryRun),
    skipReplacesExisting: Boolean(skipReplacesExisting),
  };
}

function saveStudyContentAndPublish(studyContent) {
  const normalized = normalizeStudyContentForSave(studyContent);
  const buildId = `manual_${normalized.version}_${normalized.contentLang}`;

  const result = saveStudyContentToBuild(normalized, buildId);
  const mergeResult = mergePublishOneChapter(normalized);

  const published = loadPublished();
  if (!published[normalized.version]) published[normalized.version] = {};
  if (!published[normalized.version][normalized.contentLang]) {
    published[normalized.version][normalized.contentLang] = {};
  }

  published[normalized.version][normalized.contentLang].publishMode = "merge";
  published[normalized.version][normalized.contentLang].lastMergedBuildId =
    buildId;
  published[normalized.version][normalized.contentLang].publishedAt = nowIso();
  published[normalized.version][normalized.contentLang].publishedCount =
    (published[normalized.version][normalized.contentLang].publishedCount ||
      0) + 1;

  savePublished(published);

  return {
    buildId,
    filePath: result.filePath,
    publishedFilePath: mergeResult,
    published,
    savedContent: result.savedContent,
  };
}

/* =========================================================
   Prompt
   ========================================================= */
function buildLanguageInstruction(lang) {
  if (lang === "zh") {
    return "请使用自然、清晰、简洁的简体中文输出。";
  }
  if (lang === "en") {
    return "Output entirely in natural, clear, readable English.";
  }
  if (lang === "es") {
    return "Escribe completamente en español natural, claro y fácil de leer.";
  }
  return "Output in a clear and natural language matching the requested language.";
}

function buildRuleTextFromConfig(ruleConfig, lang) {
  const baseRules = ruleConfig?.baseRules || {};
  const languageProfile = ruleConfig?.languageProfiles?.[lang] || {};
  const styleTags = ruleConfig?.styleTags || [];
  const scene = ruleConfig?.scene || "小组查经";
  const template = ruleConfig?.template || "讨论版";

  const systemPromptOverride = safeText(ruleConfig?.systemPromptOverride);
  if (systemPromptOverride) return systemPromptOverride;

  return `
你是一个圣经查经内容生成助手。

你的任务：
根据给定经文，为用户生成适合“${scene}”场景使用的查经内容。

固定要求：
1. 按经文自然分段，不要平均机械切段。
2. 每一段只给 ${baseRules.minQuestionsPerSegment || 2} 到 ${
    baseRules.maxQuestionsPerSegment || 4
  } 个问题。
3. 问题要顺着经文推进，像从开始到结束一段一段查考。
4. 问题必须紧贴该段经文，不要泛泛而谈。
5. 风格模板：${template}。
6. 风格标签：${styleTags.join("、") || "简洁、生活化"}。
7. ${
    baseRules.leaderHint === false
      ? "不要输出整章带领提示。"
      : "可以输出整章带领提示。"
  }
8. ${
    baseRules.avoidRepeat === false
      ? "允许适度重复句式。"
      : "尽量避免不同段落重复使用同样句式。"
  }
9. 全章总问题数尽量控制在 ${baseRules.chapterQuestionMin || 15} 到 ${
    baseRules.chapterQuestionMax || 20
  } 个左右。
10. ${
    baseRules.allowLightApplication === false
      ? "不要强调应用延伸，只聚焦文本观察与理解。"
      : "可以有轻度应用，但不能脱离经文文本。"
  }
11. ${
    baseRules.allowGospelEmphasis
      ? "可以适度强调福音线索、人的光景、神的拯救与恩典。"
      : "不要强行拉向福音主题，先尊重文本本身。"
  }
12. ${
    baseRules.allowChildrenTone ? "语言要更简单、具体、短句化，适合儿童。" : ""
  }
13. ${
    baseRules.allowYouthTone
      ? "可以更多关注成长、身份、选择、同伴压力与真实生活。"
      : ""
  }
14. ${
    baseRules.allowCoupleTone
      ? "可以更多关注关系、沟通、信任、同心与家庭中的属灵同行。"
      : ""
  }
15. ${
    baseRules.allowWorkplaceTone
      ? "可以更多关注品格、决策、忠心、诚信、压力与职场处境。"
      : ""
  }
16. 输出必须是合法 JSON。
17. 不要输出 markdown 代码块，不要在 JSON 外说话。

语言要求：
${buildLanguageInstruction(lang)}

语言补充要求：
${languageProfile.customPrompt || "无"}
`.trim();
}

function buildUserTextForGeneration({
  bookId,
  chapter,
  scriptureRows,
  lang,
  primaryScriptureVersionId,
}) {
  const book = getBookById(bookId);
  const bookLabel =
    lang === "en" ? book?.bookEn || bookId : book?.bookCn || bookId;

  const scriptureText = scriptureRows
    .map((row) => {
      const verseText = row.texts?.[primaryScriptureVersionId] || "";
      return `${row.verse}. ${verseText}`;
    })
    .join("\n");

  return `
Please generate Bible study content for:

Book: ${bookLabel}
Chapter: ${chapter}
Language: ${lang}

Scripture:
${scriptureText}

Return strict JSON in this format:
{
  "title": "Title",
  "theme": "Theme",
  "repeatedWords": [
    { "word": "word1", "count": 3 }
  ],
  "segments": [
    {
      "title": "Segment title",
      "rangeStart": 1,
      "rangeEnd": 5,
      "questions": [
        "Question 1",
        "Question 2"
      ]
    }
  ],
  "chapterLeaderHint": [],
  "closing": ""
}
`.trim();
}

async function generateStudyWithRuleConfig({
  versionId,
  lang,
  bookId,
  chapter,
  primaryScriptureVersionId = "",
}) {
  const aiClient = getOpenAiClient();
  if (!aiClient) {
    throw new Error("缺少 OPENAI_API_KEY");
  }

  const ruleConfig = loadRuleConfig(versionId);
  if (!ruleConfig) {
    throw new Error(`未找到规则配置: ${versionId}`);
  }

  const languages = loadLanguages().languages || [];
  const langConfig = languages.find((x) => x.id === lang && x.enabled);
  if (!langConfig) {
    throw new Error(`未启用的语言: ${lang}`);
  }

  const primaryScriptureVersion = resolveGenerationPrimaryScriptureVersion({
    lang,
    preferredVersionId: primaryScriptureVersionId,
  });
  if (!primaryScriptureVersion) {
    throw new Error(`未找到该语言的圣经版本: ${lang}`);
  }

  const scriptureRows = getMultiVersionScriptureRows({
    scriptureVersionIds: [primaryScriptureVersion.id],
    bookId,
    chapter,
  });

  const systemText = buildRuleTextFromConfig(ruleConfig, lang);
  const userText = buildUserTextForGeneration({
    bookId,
    chapter,
    scriptureRows,
    lang,
    primaryScriptureVersionId: primaryScriptureVersion.id,
  });

  const modelCandidates = Array.from(
    new Set(
      [
        process.env.OPENAI_MODEL,
        "gpt-5.4",
        "gpt-4.1-mini",
      ].filter((x) => isNonEmptyString(x))
    )
  );

  let response = null;
  let lastErr = null;

  for (const model of modelCandidates) {
    try {
      response = await aiClient.responses.create({
        model,
        input: [
          {
            role: "system",
            content: [{ type: "input_text", text: systemText }],
          },
          {
            role: "user",
            content: [{ type: "input_text", text: userText }],
          },
        ],
      });
      break;
    } catch (error) {
      lastErr = error;
      const status = Number(error?.status || 0);
      const msg = String(error?.message || "");
      const modelUnavailable =
        status === 400 ||
        status === 401 ||
        status === 403 ||
        status === 404 ||
        /model|not found|access|permission|unauthorized|unsupported/i.test(msg);
      if (!modelUnavailable) {
        break;
      }
    }
  }

  if (!response) {
    const status = Number(lastErr?.status || 0);
    const code = safeText(lastErr?.code);
    const message = safeText(lastErr?.message) || "模型请求失败";
    const causeCode = safeText(lastErr?.cause?.code || lastErr?.cause?.cause?.code);
    const hostname = safeText(
      lastErr?.cause?.hostname || lastErr?.cause?.cause?.hostname
    );
    if (causeCode === "ENOTFOUND" && hostname) {
      throw new Error(
        `网络/DNS 异常：无法解析 ${hostname}，请检查本机网络、DNS 或代理设置`
      );
    }
    const detail = [status ? `status=${status}` : "", code ? `code=${code}` : ""]
      .filter(Boolean)
      .join(", ");
    throw new Error(detail ? `${message}（${detail}）` : message);
  }

  const rawText = safeText(response.output_text);
  if (!rawText) {
    throw new Error("模型返回为空，请检查模型权限或配额");
  }
  let parsed;

  try {
    parsed = JSON.parse(rawText);
  } catch (error) {
    console.error("模型原始输出：", rawText);
    throw new Error("模型返回的不是合法 JSON");
  }

  const book = getBookById(bookId);

  return {
    version: versionId,
    versionLabel:
      getEnabledContentVersions().find((x) => x.id === versionId)?.label ||
      versionId,
    contentLang: lang,
    bookId,
    bookLabel: lang === "en" ? book?.bookEn || bookId : book?.bookCn || bookId,
    chapter: Number(chapter),
    theme: parsed.theme || "",
    repeatedWords: parsed.repeatedWords || [],
    segments: parsed.segments || [],
    chapterLeaderHint: parsed.chapterLeaderHint || [],
    closing: parsed.closing || "",
    title: parsed.title || "",
    generatedAt: nowIso(),
  };
}

/* =========================================================
   批量任务
   ========================================================= */
function getAllEnabledVersionIds() {
  return getEnabledContentVersions().map((x) => x.id);
}

function getAllEnabledLanguageIds() {
  return getEnabledLanguages().map((x) => x.id);
}

function resolveVersionIds(versionMode, version) {
  if (versionMode === "all_enabled") {
    return getAllEnabledVersionIds();
  }
  return isNonEmptyString(version) ? [version] : [];
}

function resolveLanguageIds(langMode, lang) {
  if (langMode === "all_enabled") {
    return getAllEnabledLanguageIds();
  }
  return isNonEmptyString(lang) ? [lang] : [];
}

function createBookRangeTargets(bookId, startChapter, endChapter) {
  const book = getBookById(bookId);
  if (!book) throw new Error("未找到 bookId");

  const start = Number(startChapter || 1);
  const end = Number(endChapter || book.chapters);

  if (!Number.isInteger(start) || !Number.isInteger(end)) {
    throw new Error("章节范围必须是整数");
  }
  if (start < 1 || end < 1 || start > end || end > Number(book.chapters)) {
    throw new Error("章节范围不正确");
  }

  return Array.from({ length: end - start + 1 }, (_, i) => ({
    bookId,
    chapter: start + i,
  }));
}

function buildChapterTargetsForScope(scope, payload) {
  if (scope === "chapter") {
    const bookId = safeText(payload.bookId);
    const chapter = Number(payload.chapter || 0);
    if (!bookId || !chapter) {
      throw new Error("chapter 任务缺少 bookId 或 chapter");
    }
    return [{ bookId, chapter }];
  }

  if (scope === "book") {
    const bookId = safeText(payload.bookId);
    const hasRange =
      payload.startChapter !== undefined || payload.endChapter !== undefined;

    if (hasRange) {
      return createBookRangeTargets(
        bookId,
        payload.startChapter,
        payload.endChapter
      );
    }

    const book = getBookById(bookId);
    if (!book) throw new Error("未找到 bookId");
    return Array.from({ length: Number(book.chapters) }, (_, i) => ({
      bookId,
      chapter: i + 1,
    }));
  }

  if (scope === "old_testament") {
    return getBooksByTestament("旧约").flatMap((book) =>
      Array.from({ length: Number(book.chapters) }, (_, i) => ({
        bookId: book.bookId,
        chapter: i + 1,
      }))
    );
  }

  if (scope === "new_testament") {
    return getBooksByTestament("新约").flatMap((book) =>
      Array.from({ length: Number(book.chapters) }, (_, i) => ({
        bookId: book.bookId,
        chapter: i + 1,
      }))
    );
  }

  if (scope === "bible") {
    return flattenBooks().flatMap((book) =>
      Array.from({ length: Number(book.chapters) }, (_, i) => ({
        bookId: book.bookId,
        chapter: i + 1,
      }))
    );
  }

  throw new Error(`不支持的 scope: ${scope}`);
}

function resolveTargetsFromPayload(payload) {
  const scope = safeText(payload.scope || "chapter");
  const versionIds = resolveVersionIds(payload.versionMode, payload.version);
  const langIds = resolveLanguageIds(payload.langMode, payload.lang);

  if (!versionIds.length) {
    throw new Error("没有可用的内容版本");
  }
  if (!langIds.length) {
    throw new Error("没有可用的语言");
  }

  const chapters = buildChapterTargetsForScope(scope, payload);
  const targets = [];

  for (const versionId of versionIds) {
    for (const lang of langIds) {
      for (const item of chapters) {
        targets.push({
          versionId,
          lang,
          bookId: item.bookId,
          chapter: item.chapter,
        });
      }
    }
  }

  return targets;
}

function createBuildIdForJob(payload) {
  const scope = safeText(payload.scope || "chapter");
  const versionPart =
    payload.versionMode === "all_enabled"
      ? "allv"
      : safeText(payload.version || "noversion");
  const langPart =
    payload.langMode === "all_enabled"
      ? "alll"
      : safeText(payload.lang || "nolang");
  return `build_${Date.now()}_${scope}_${versionPart}_${langPart}`;
}

function getJobFilePath(jobId) {
  return path.join(JOBS_DIR, `${jobId}.json`);
}

function readJob(jobId) {
  return readJson(getJobFilePath(jobId), null);
}

function writeJob(job) {
  writeJson(getJobFilePath(job.id), job);
}

function listAllJobsNewestFirst() {
  ensureDir(JOBS_DIR);
  return fs
    .readdirSync(JOBS_DIR)
    .filter((f) => f.endsWith(".json"))
    .map((f) => readJson(path.join(JOBS_DIR, f), null))
    .filter(Boolean)
    .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
}

function createBulkJob(payload) {
  const kind = safeText(payload.kind || "study");

  const targets = resolveTargetsFromPayload(payload);
  const jobId = `job_${Date.now()}`;
  const buildId = createBuildIdForJob(payload);

  const job = {
    id: jobId,
    kind,
    type: "bulk_generate",
    scriptureVersionId: safeText(payload.scriptureVersionId || ""),
    scope: safeText(payload.scope || "chapter"),
    versionMode: safeText(payload.versionMode || "single"),
    version: safeText(payload.version || ""),
    langMode: safeText(payload.langMode || "single"),
    lang: safeText(payload.lang || ""),
    bookId: safeText(payload.bookId || ""),
    chapter: payload.chapter ? Number(payload.chapter) : null,
    startChapter:
      payload.startChapter !== undefined ? Number(payload.startChapter) : null,
    endChapter:
      payload.endChapter !== undefined ? Number(payload.endChapter) : null,
    autoPublish: payload.autoPublish === true,
    skipPublishOverwrite:
      payload.skipPublishOverwrite === true ||
      payload.skipPublishWhenPublishedExists === true,
    buildId,
    status: "queued",
    done: 0,
    total: targets.length,
    targets,
    errors: [],
    startedAt: "",
    finishedAt: "",
    createdAt: nowIso(),
    updatedAt: nowIso(),
    progressText: "排队中",
    completionSummary: "",
    retryOfJobId: safeText(payload.retryOfJobId || ""),
  };

  writeJob(job);
  return job;
}

function createRetryFailedJob(sourceJobId) {
  const sourceJob = readJob(sourceJobId);
  if (!sourceJob) {
    throw new Error("原任务不存在");
  }

  const failedTargets = (sourceJob.errors || [])
    .map((err) => err.target)
    .filter(Boolean);

  if (!failedTargets.length) {
    throw new Error("这个任务没有失败章节可重跑");
  }

  const jobId = `job_${Date.now()}`;
  const buildId = `build_${Date.now()}_retry_${sourceJobId}`;

  const job = {
    id: jobId,
    kind: "study",
    type: "retry_failed",
    scriptureVersionId: safeText(sourceJob.scriptureVersionId || ""),
    scope: sourceJob.scope || "book",
    versionMode: sourceJob.versionMode || "single",
    version: sourceJob.version || "",
    langMode: sourceJob.langMode || "single",
    lang: sourceJob.lang || "",
    bookId: sourceJob.bookId || "",
    chapter: sourceJob.chapter || null,
    startChapter: sourceJob.startChapter || null,
    endChapter: sourceJob.endChapter || null,
    autoPublish: true,
    skipPublishOverwrite: sourceJob.skipPublishOverwrite === true,
    buildId,
    status: "queued",
    done: 0,
    total: failedTargets.length,
    targets: failedTargets,
    errors: [],
    startedAt: "",
    finishedAt: "",
    createdAt: nowIso(),
    updatedAt: nowIso(),
    progressText: `重跑失败章节，来源任务 ${sourceJobId}`,
    completionSummary: "",
    retryOfJobId: sourceJobId,
  };

  writeJob(job);
  return job;
}

function buildCompletionSummary(job) {
  const successCount = Math.max(
    0,
    Number(job.done || 0) - Number(job.errors?.length || 0)
  );
  const errorCount = Number(job.errors?.length || 0);
  let autoPublished = "未自动发布";
  if (job.autoPublish) {
    autoPublished = "已自动逐章合并发布";
  }
  return `完成：成功 ${successCount}，失败 ${errorCount}，${autoPublished}`;
}

async function processBulkJob(job) {
  job.status = "running";
  job.startedAt = job.startedAt || nowIso();
  job.updatedAt = nowIso();
  job.progressText = "开始执行";
  writeJob(job);

  for (let i = job.done; i < job.targets.length; i += 1) {
    const latestJob = readJob(job.id);
    if (!latestJob) return;

    if (latestJob.status === "cancelled") {
      latestJob.updatedAt = nowIso();
      latestJob.finishedAt = latestJob.finishedAt || nowIso();
      latestJob.progressText = "任务已取消";
      latestJob.completionSummary = "已取消";
      writeJob(latestJob);
      return;
    }

    const target = latestJob.targets[i];

    try {
      latestJob.progressText = `正在生成 ${target.versionId} / ${target.lang} / ${target.bookId} / ${target.chapter}`;
      latestJob.updatedAt = nowIso();
      writeJob(latestJob);

      const result = await generateStudyWithRuleConfig({
        versionId: target.versionId,
        lang: target.lang,
        bookId: target.bookId,
        chapter: target.chapter,
      });

      const { savedContent } = saveStudyContentToBuild(
        result,
        latestJob.buildId
      );

      if (latestJob.autoPublish) {
        const cmp = compareStudyContentWithPublished(savedContent);
        const skipOverwrite = latestJob.skipPublishOverwrite === true;
        if (!cmp.changed) {
          latestJob.progressText = `已生成，与已发布一致已跳过发布 ${target.bookId} ${target.chapter} 章（${i + 1} / ${latestJob.total}）`;
        } else if (skipOverwrite && cmp.publishedExists) {
          latestJob.progressText = `已生成，该章已有发布已跳过覆盖 ${target.bookId} ${target.chapter} 章（${i + 1} / ${latestJob.total}）`;
        } else {
          mergePublishOneChapter(savedContent);
          bumpPublishedMergeMetaForOneChapter(
            target.versionId,
            target.lang,
            latestJob.buildId
          );
          invalidateStudyContentCache(
            target.versionId,
            target.lang,
            target.bookId,
            target.chapter
          );
          latestJob.progressText = `已生成并发布 ${target.bookId} ${target.chapter} 章（${i + 1} / ${latestJob.total}）`;
        }
      } else {
        latestJob.progressText = `已完成 ${i + 1} / ${latestJob.total}`;
      }

      latestJob.done = i + 1;
      latestJob.updatedAt = nowIso();
      writeJob(latestJob);

      await sleep(150);
    } catch (error) {
      latestJob.done = i + 1;
      latestJob.errors.push({
        index: i,
        target,
        message: error.message || "未知错误",
        at: nowIso(),
      });
      latestJob.updatedAt = nowIso();
      latestJob.progressText = `有错误，已完成 ${latestJob.done} / ${latestJob.total}`;
      writeJob(latestJob);
    }
  }

  const finalJob = readJob(job.id);
  if (!finalJob) return;

  finalJob.status = "completed";
  finalJob.finishedAt = nowIso();
  finalJob.updatedAt = nowIso();
  finalJob.progressText = `任务完成：${finalJob.done} / ${finalJob.total}`;

  if (finalJob.autoPublish) {
    /* 生成阶段已逐章 mergePublish，此处不再整批 merge，避免重复计数与重复写盘 */
    finalJob.progressText += "，并已自动合并发布（逐章）";
  }

  finalJob.completionSummary = buildCompletionSummary(finalJob);
  writeJob(finalJob);
}

/* =========================================================
   已发布内容管理
   ========================================================= */
function listPublishedBookChapters(versionId, lang, bookId) {
  const book = getBookById(bookId);
  if (!book) {
    throw new Error("未找到书卷");
  }

  const bookDir = path.join(CONTENT_PUBLISHED_DIR, versionId, lang, bookId);
  const existing = [];

  if (fs.existsSync(bookDir)) {
    for (const fileName of fs.readdirSync(bookDir)) {
      if (!fileName.endsWith(".json")) continue;
      const chapter = Number(fileName.replace(".json", ""));
      if (Number.isInteger(chapter)) existing.push(chapter);
    }
  }

  existing.sort((a, b) => a - b);

  const allChapters = Array.from(
    { length: Number(book.chapters) },
    (_, i) => i + 1
  );
  const missing = allChapters.filter((n) => !existing.includes(n));

  return {
    bookId,
    bookCn: book.bookCn,
    bookEn: book.bookEn,
    totalChapters: Number(book.chapters),
    publishedChapters: existing,
    missingChapters: missing,
    publishedCount: existing.length,
  };
}

function listPublishedOverview(versionId, lang) {
  const books = flattenBooks();
  const items = books.map((book) =>
    listPublishedBookChapters(versionId, lang, book.bookId)
  );

  const summary = {
    version: versionId,
    lang,
    totalBooks: items.length,
    booksWithAnyPublished: items.filter((x) => x.publishedCount > 0).length,
    totalPublishedChapters: items.reduce((sum, x) => sum + x.publishedCount, 0),
    totalMissingChapters: items.reduce(
      (sum, x) => sum + x.missingChapters.length,
      0
    ),
  };

  return {
    summary,
    books: items,
  };
}

function deletePublishedChapter(versionId, lang, bookId, chapter) {
  const filePath = getPublishedContentFilePath({
    versionId,
    lang,
    bookId,
    chapter,
  });

  if (!fs.existsSync(filePath)) {
    throw new Error("该章已发布内容不存在");
  }

  const vDir = getChapterVideosDir(versionId, lang, bookId, chapter);
  try {
    fs.rmSync(vDir, { recursive: true, force: true });
  } catch {
    /* ignore */
  }

  fs.unlinkSync(filePath);
  return { deleted: true, filePath };
}

function republishOneChapterFromBuild({
  buildId,
  versionId,
  lang,
  bookId,
  chapter,
  onlyChanged = false,
  dryRun = false,
}) {
  const sourcePath = getBuildContentFilePath({
    buildId,
    versionId,
    lang,
    bookId,
    chapter,
  });

  if (!fs.existsSync(sourcePath)) {
    throw new Error("build 中未找到该章内容");
  }

  const content = readJson(sourcePath, null);
  if (!content) {
    throw new Error("build 章节内容读取失败");
  }

  if (onlyChanged || dryRun) {
    const compare = compareBuildChapterWithPublished({
      buildId,
      versionId,
      lang,
      bookId,
      chapter,
    });
    if (onlyChanged && !compare.changed) {
      return {
        skipped: true,
        changed: false,
        dryRun: Boolean(dryRun),
        publishedPath: compare.publishedPath,
        content,
      };
    }
    if (dryRun) {
      return {
        skipped: false,
        changed: Boolean(compare.changed),
        dryRun: true,
        publishedPath: compare.publishedPath,
        content,
      };
    }
  }

  const publishedPath = mergePublishOneChapter(content);

  const published = loadPublished();
  if (!published[versionId]) published[versionId] = {};
  if (!published[versionId][lang]) published[versionId][lang] = {};

  published[versionId][lang].publishMode = "merge";
  published[versionId][lang].lastMergedBuildId = buildId;
  published[versionId][lang].publishedAt = nowIso();
  published[versionId][lang].publishedCount =
    (published[versionId][lang].publishedCount || 0) + 1;
  savePublished(published);

  return {
    publishedPath,
    content,
    skipped: false,
    changed: true,
  };
}

function findLatestBuildForChapterFromFilesystem({
  versionId,
  lang,
  bookId,
  chapter,
  buildSubdirNames = null,
}) {
  if (!fs.existsSync(CONTENT_BUILDS_DIR)) return null;
  const relSuffix = path.join(versionId, lang, bookId, `${chapter}.json`);
  let best = null;
  let bestMtime = -1;
  let dirNames;
  if (Array.isArray(buildSubdirNames)) {
    dirNames = buildSubdirNames;
  } else {
    try {
      dirNames = fs
        .readdirSync(CONTENT_BUILDS_DIR, { withFileTypes: true })
        .filter((e) => e.isDirectory())
        .map((e) => e.name);
    } catch {
      return null;
    }
  }
  for (const name of dirNames) {
    const candidatePath = path.join(CONTENT_BUILDS_DIR, name, relSuffix);
    if (!fs.existsSync(candidatePath)) continue;
    let st;
    try {
      st = fs.statSync(candidatePath);
    } catch {
      continue;
    }
    const m = st.mtimeMs;
    if (m > bestMtime) {
      bestMtime = m;
      best = {
        jobId: "",
        buildId: name,
        path: candidatePath,
      };
    }
  }
  return best;
}

function findLatestBuildForChapter({
  versionId,
  lang,
  bookId,
  chapter,
  jobsSnapshot = null,
  buildSubdirNames = null,
}) {
  const jobs = Array.isArray(jobsSnapshot)
    ? jobsSnapshot
    : listAllJobsNewestFirst();

  for (const job of jobs) {
    if (job.status !== "completed") continue;
    if (!isNonEmptyString(job.buildId)) continue;

    const candidatePath = getBuildContentFilePath({
      buildId: job.buildId,
      versionId,
      lang,
      bookId,
      chapter,
    });

    if (fs.existsSync(candidatePath)) {
      return {
        jobId: job.id,
        buildId: job.buildId,
        path: candidatePath,
      };
    }
  }

  return findLatestBuildForChapterFromFilesystem({
    versionId,
    lang,
    bookId,
    chapter,
    buildSubdirNames,
  });
}

function autoRepublishChapter({
  versionId,
  lang,
  bookId,
  chapter,
  onlyChanged = false,
  dryRun = false,
  jobsSnapshot = null,
  buildSubdirNames = null,
}) {
  const found = findLatestBuildForChapter({
    versionId,
    lang,
    bookId,
    chapter,
    jobsSnapshot,
    buildSubdirNames,
  });

  if (!found) {
    throw new Error("未找到可用于自动补发的来源记录");
  }

  const result = republishOneChapterFromBuild({
    buildId: found.buildId,
    versionId,
    lang,
    bookId,
    chapter,
    onlyChanged,
    dryRun,
  });

  return {
    sourceJobId: found.jobId,
    sourceBuildId: found.buildId,
    ...result,
  };
}

async function republishBulkFromLatestBuilds({
  mode,
  version,
  lang,
  onlyChanged = false,
  dryRun = false,
  runId = "",
  skipReplacesExisting = false,
}) {
  const jobs = listAllJobsNewestFirst();
  const completed = jobs.filter(
    (job) =>
      job.status === "completed" &&
      isNonEmptyString(job.buildId) &&
      Array.isArray(job.targets) &&
      job.targets.length > 0
  );

  const latestByPair = new Map();
  completed.forEach((job) => {
    const pairs = new Set(
      job.targets.map((t) => `${safeText(t.versionId)}__${safeText(t.lang)}`)
    );
    pairs.forEach((pairKey) => {
      if (!latestByPair.has(pairKey)) latestByPair.set(pairKey, job);
    });
  });

  const selectedPairs = Array.from(latestByPair.keys()).filter((pairKey) => {
    const [pairVersion, pairLang] = pairKey.split("__");
    if (mode === "all") return true;
    if (mode === "version") return pairVersion === version;
    if (mode === "lang") return pairLang === lang;
    if (mode === "version_lang") {
      return pairVersion === version && pairLang === lang;
    }
    return false;
  });

  let totalPublishedCount = 0;
  let totalSkippedCount = 0;
  const details = [];

  for (const pairKey of selectedPairs) {
    if (runId) {
      assertPublishRunNotCancelled(runId);
      await nextTickAsync();
    }
    const [pairVersion, pairLang] = pairKey.split("__");
    const job = latestByPair.get(pairKey);
    if (!job) continue;

    const result = await mergePublishFromBuild({
      buildId: job.buildId,
      versionId: pairVersion,
      lang: pairLang,
      targets: job.targets || [],
      onlyChanged,
      dryRun,
      runId,
      skipReplacesExisting,
    });

    totalPublishedCount += Number(result?.publishedCount || 0);
    totalSkippedCount += Number(result?.skippedCount || 0);
    details.push({
      version: pairVersion,
      lang: pairLang,
      sourceJobId: job.id,
      sourceBuildId: job.buildId,
      publishedCount: Number(result?.publishedCount || 0),
      skippedCount: Number(result?.skippedCount || 0),
      changedTargets: Array.isArray(result?.changedTargets)
        ? result.changedTargets
        : [],
      skippedTargets: Array.isArray(result?.skippedTargets)
        ? result.skippedTargets
        : [],
    });
  }

  return {
    mode,
    matchedPairs: selectedPairs.length,
    totalPublishedCount,
    totalSkippedCount,
    details,
    onlyChanged: Boolean(onlyChanged),
    dryRun: Boolean(dryRun),
  };
}

function listVersionLangPairsByMode({ mode, version, lang }) {
  const versions = getEnabledContentVersions().map((x) => safeText(x.id)).filter(Boolean);
  const langs = getEnabledLanguages()
    .filter((x) => x.contentEnabled !== false)
    .map((x) => safeText(x.id))
    .filter(Boolean);
  const allPairs = [];
  versions.forEach((v) => {
    langs.forEach((l) => {
      allPairs.push({ version: v, lang: l });
    });
  });
  return allPairs.filter((pair) => {
    if (mode === "all") return true;
    if (mode === "version") return pair.version === version;
    if (mode === "lang") return pair.lang === lang;
    if (mode === "version_lang") return pair.version === version && pair.lang === lang;
    return false;
  });
}

async function autoRepublishMissingBulkFromLatestBuilds({
  mode,
  version,
  lang,
  onlyChanged = true,
  dryRun = false,
  runId = "",
}) {
  const jobsSnapshot = listAllJobsNewestFirst();
  let buildSubdirNames = [];
  if (fs.existsSync(CONTENT_BUILDS_DIR)) {
    try {
      buildSubdirNames = fs
        .readdirSync(CONTENT_BUILDS_DIR, { withFileTypes: true })
        .filter((e) => e.isDirectory())
        .map((e) => e.name);
    } catch {
      buildSubdirNames = [];
    }
  }

  const pairs = listVersionLangPairsByMode({ mode, version, lang });
  const books = flattenBooks();
  const details = [];
  let totalMissingBefore = 0;
  let totalAttempted = 0;
  let totalRepublished = 0;
  let totalSkipped = 0;
  let totalFailed = 0;
  let totalNoSource = 0;

  for (const pair of pairs) {
    if (runId) {
      assertPublishRunNotCancelled(runId);
      await nextTickAsync();
    }
    const missingTargets = [];
    for (const book of books) {
      const coverage = listPublishedBookChapters(pair.version, pair.lang, book.bookId);
      (coverage.missingChapters || []).forEach((chapter) => {
        missingTargets.push({
          bookId: book.bookId,
          chapter: Number(chapter),
        });
      });
    }
    const pairDetail = {
      version: pair.version,
      lang: pair.lang,
      missingBefore: missingTargets.length,
      attempted: 0,
      republishedCount: 0,
      skippedCount: 0,
      failedCount: 0,
      noSourceCount: 0,
      republishedTargets: [],
      skippedTargets: [],
      failedTargets: [],
    };

    for (const target of missingTargets) {
      if (runId) {
        assertPublishRunNotCancelled(runId);
        await nextTickAsync();
      }
      pairDetail.attempted += 1;
      totalAttempted += 1;
      try {
        const result = autoRepublishChapter({
          versionId: pair.version,
          lang: pair.lang,
          bookId: target.bookId,
          chapter: target.chapter,
          onlyChanged,
          dryRun,
          jobsSnapshot,
          buildSubdirNames,
        });
        if (result.skipped) {
          pairDetail.skippedCount += 1;
          totalSkipped += 1;
          pairDetail.skippedTargets.push({
            bookId: target.bookId,
            chapter: target.chapter,
          });
        } else {
          pairDetail.republishedCount += 1;
          totalRepublished += 1;
          pairDetail.republishedTargets.push({
            bookId: target.bookId,
            chapter: target.chapter,
            sourceBuildId: safeText(result.sourceBuildId || ""),
            sourceJobId: safeText(result.sourceJobId || ""),
          });
        }
      } catch (error) {
        const message = safeText(error?.message || "未知错误");
        pairDetail.failedCount += 1;
        totalFailed += 1;
        if (message.includes("未找到可用于自动补发的来源记录")) {
          pairDetail.noSourceCount += 1;
          totalNoSource += 1;
        }
        pairDetail.failedTargets.push({
          bookId: target.bookId,
          chapter: target.chapter,
          error: message,
        });
      }
    }

    totalMissingBefore += pairDetail.missingBefore;
    details.push(pairDetail);
  }

  return {
    mode,
    matchedPairs: pairs.length,
    totalMissingBefore,
    totalAttempted,
    totalRepublished,
    totalSkipped,
    totalFailed,
    totalNoSource,
    details,
    onlyChanged: Boolean(onlyChanged),
    dryRun: Boolean(dryRun),
  };
}

/* =========================================================
   任务执行器
   ========================================================= */
let jobRunnerStarted = false;
let isJobRunnerBusy = false;

async function runJobRunnerLoop() {
  if (isJobRunnerBusy) return;
  isJobRunnerBusy = true;

  try {
    while (true) {
      const jobs = fs
        .readdirSync(JOBS_DIR)
        .filter((f) => f.endsWith(".json"))
        .map((f) => readJson(path.join(JOBS_DIR, f), null))
        .filter(Boolean)
        .sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));

      const queuedJob = jobs.find((j) => j.status === "queued");
      if (!queuedJob) break;

      await processBulkJob(queuedJob);
    }
  } catch (error) {
    console.error("任务执行器异常:", error);
  } finally {
    isJobRunnerBusy = false;
  }
}

function startJobRunner() {
  if (jobRunnerStarted) return;
  jobRunnerStarted = true;
  try {
    runAutoDataBackupTick();
  } catch (error) {
    console.error("runAutoDataBackupTick error:", error);
  }

  setInterval(() => {
    runJobRunnerLoop().catch((error) => {
      console.error("runJobRunnerLoop error:", error);
    });
  }, 2000);

  setInterval(() => {
    try {
      runAutoDataBackupTick();
    } catch (error) {
      console.error("runAutoDataBackupTick error:", error);
    }
  }, 30000);
}

/* =========================================================
   全局配色主题（color_themes.json）
   ========================================================= */
const ALLOWED_THEME_VAR_KEYS = new Set(BUILTIN_COLOR_THEME_VARIABLE_KEYS);

function slugColorThemeId(raw) {
  let s = safeText(raw || "")
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
  if (!s) s = `theme-${crypto.randomBytes(4).toString("hex")}`;
  return s;
}

function normalizeThemeVariables(obj) {
  if (!obj || typeof obj !== "object") return {};
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (!ALLOWED_THEME_VAR_KEYS.has(k)) continue;
    const val = String(v ?? "").trim();
    if (!val || val.length > 2400) continue;
    out[k] = val;
  }
  return out;
}

function defaultColorThemesConfig() {
  return {
    version: 1,
    defaultThemeId: "classic",
    themes: [{ id: "classic", label: "经典暖纸", variables: {} }],
  };
}

function loadColorThemesConfig() {
  const raw = readJson(COLOR_THEMES_FILE, null);
  const base = defaultColorThemesConfig();
  if (!raw || typeof raw !== "object") return base;
  const arr = Array.isArray(raw.themes) ? raw.themes : [];
  const themes = arr
    .filter((t) => t && isNonEmptyString(t.id) && isNonEmptyString(t.label))
    .map((t) => ({
      id: slugColorThemeId(t.id),
      label: String(t.label || "").trim().slice(0, 80),
      variables: normalizeThemeVariables(t.variables),
    }))
    .filter((t) => t.id);
  const merged = themes.length ? themes : base.themes;
  let defaultThemeId = slugColorThemeId(raw.defaultThemeId);
  if (!merged.find((x) => x.id === defaultThemeId)) {
    defaultThemeId = merged[0].id;
  }
  return { version: 1, defaultThemeId, themes: merged };
}

function saveColorThemesConfig(cfg) {
  ensureDir(ADMIN_DIR);
  writeJson(COLOR_THEMES_FILE, cfg);
}

function resolveColorThemeVariables(themeId) {
  const cfg = loadColorThemesConfig();
  const id = cfg.themes.find((t) => t.id === themeId)?.id || cfg.defaultThemeId;
  const theme = cfg.themes.find((t) => t.id === id) || cfg.themes[0];
  return {
    themeId: theme.id,
    variables: {
      ...BUILTIN_COLOR_THEME_VARIABLES,
      ...(theme.variables || {}),
    },
  };
}

/* =========================================================
   前台接口
   ========================================================= */
app.get("/api/front/bootstrap", (_req, res) => {
  try {
    const uiLanguages = (loadLanguages().languages || []).filter(
      (x) => x.uiEnabled
    );

    const scriptureVersions = getEnabledScriptureVersions()
      .filter((x) => x.uiEnabled !== false && x.scriptureEnabled !== false)
      .map((x) => ({
        id: x.id,
        label: x.label,
        lang: x.lang,
        description: x.description || "",
        sortOrder: Number(x.sortOrder || 999),
      }));

    const menuContentVersions = getMenuContentVersions();
    const contentVersions = menuContentVersions.map((x) => ({
      id: x.id,
      label: x.label,
    }));

    const defaultPrimary =
      scriptureVersions.find((x) => x.id === "cuvs_zh")?.id ||
      scriptureVersions.find((x) => x.lang === "zh")?.id ||
      scriptureVersions[0]?.id ||
      "";

    const defaultContentVersionId =
      menuContentVersions.find((x) => x.id === "default")?.id ||
      menuContentVersions[0]?.id ||
      "default";

    const colorCfg = loadColorThemesConfig();
    res.set(
      "Cache-Control",
      "private, no-store, no-cache, max-age=0, must-revalidate"
    );
    res.json({
      uiLanguages,
      scriptureVersions,
      contentVersions,
      defaultState: {
        uiLang: "zh",
        primaryScriptureVersionId: defaultPrimary,
        secondaryScriptureVersionIds: [],
        contentVersionId: defaultContentVersionId,
        contentLang: "zh",
      },
      testamentOptions: flattenBooks(),
      colorThemes: {
        defaultThemeId: colorCfg.defaultThemeId,
        themes: colorCfg.themes.map((t) => ({ id: t.id, label: t.label })),
      },
      siteChrome: loadSiteChrome(),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "bootstrap 失败" });
  }
});

app.get("/api/color-themes", (_req, res) => {
  try {
    const cfg = loadColorThemesConfig();
    res.setHeader("Cache-Control", "no-store");
    res.json({
      defaultThemeId: cfg.defaultThemeId,
      themes: cfg.themes.map((t) => ({ id: t.id, label: t.label })),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取主题失败" });
  }
});

app.get("/api/color-themes/variables", (req, res) => {
  try {
    const themeId = safeText(req.query.themeId || "");
    const data = resolveColorThemeVariables(themeId);
    res.setHeader("Cache-Control", "no-store");
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取主题变量失败" });
  }
});

app.get("/api/admin/color-themes", (req, res) => {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    res.json(loadColorThemesConfig());
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取失败" });
  }
});

app.post("/api/admin/color-themes/save", (req, res) => {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const body = req.body || {};
    const themesIn = Array.isArray(body.themes) ? body.themes : null;
    if (!themesIn || !themesIn.length) {
      return res.status(400).json({ error: "至少保留一套主题" });
    }
    const themes = themesIn
      .filter((t) => t && isNonEmptyString(t.id) && isNonEmptyString(t.label))
      .map((t) => ({
        id: slugColorThemeId(t.id),
        label: String(t.label || "").trim().slice(0, 80),
        variables: normalizeThemeVariables(t.variables),
      }))
      .filter((t) => t.id);
    if (!themes.length) {
      return res.status(400).json({ error: "主题列表无效" });
    }
    let defaultThemeId = slugColorThemeId(body.defaultThemeId);
    if (!themes.find((x) => x.id === defaultThemeId)) {
      defaultThemeId = themes[0].id;
    }
    const cfg = { version: 1, defaultThemeId, themes };
    saveColorThemesConfig(cfg);
    res.json({ ok: true, config: cfg });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存失败" });
  }
});

/** 仅更新某一主题下的单个 CSS 变量（其余主题与变量保持磁盘上原状） */
app.post("/api/admin/color-themes/variable", (req, res) => {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const body = req.body || {};
    const themeId = slugColorThemeId(safeText(body.themeId || ""));
    const key = String(body.key || "").trim();
    if (!themeId || !ALLOWED_THEME_VAR_KEYS.has(key)) {
      return res.status(400).json({ error: "主题或变量名无效" });
    }
    const cfg = loadColorThemesConfig();
    const idx = cfg.themes.findIndex((t) => t.id === themeId);
    if (idx < 0) {
      return res.status(404).json({
        error: "服务器上尚无此主题，请先点「保存到服务器」保存整套主题后再试",
      });
    }
    const prev = cfg.themes[idx].variables || {};
    const nextVars = { ...prev };
    if (body.remove === true) {
      delete nextVars[key];
    } else {
      const normalized = normalizeThemeVariables({ [key]: body.value });
      if (!Object.prototype.hasOwnProperty.call(normalized, key)) {
        return res.status(400).json({ error: "变量值无效或超出长度" });
      }
      nextVars[key] = normalized[key];
    }
    const themes = cfg.themes.slice();
    themes[idx] = {
      ...themes[idx],
      variables: normalizeThemeVariables(nextVars),
    };
    const nextCfg = { ...cfg, themes };
    saveColorThemesConfig(nextCfg);
    res.json({ ok: true, config: nextCfg });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存失败" });
  }
});

app.post("/api/user/color-theme", (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) return res.status(401).json({ error: "请先登录" });
    const cfg = loadColorThemesConfig();
    const themeId = safeText(req.body?.themeId || "").trim();
    if (!themeId) {
      authDb
        .prepare(
          "UPDATE users SET color_theme_id = '', updated_at = ? WHERE id = ?"
        )
        .run(nowIso(), authed.id);
      return res.json({
        ok: true,
        colorThemeId: "",
        defaultThemeId: cfg.defaultThemeId,
      });
    }
    if (!cfg.themes.find((t) => t.id === themeId)) {
      return res.status(400).json({ error: "无效的主题" });
    }
    authDb
      .prepare(
        "UPDATE users SET color_theme_id = ?, updated_at = ? WHERE id = ?"
      )
      .run(themeId, nowIso(), authed.id);
    res.json({ ok: true, colorThemeId: themeId });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存失败" });
  }
});

/* 须注册在 /api/scripture 之前，避免部分环境下路径匹配歧义 */
app.get("/api/scripture/search", (req, res) => {
  try {
    const q = safeText(req.query.q);
    const versionId = safeText(req.query.versionId);
    const scopeRaw = safeText(req.query.scope || "all").toLowerCase();
    let scope = "all";
    if (scopeRaw === "ot" || scopeRaw === "old") scope = "ot";
    else if (scopeRaw === "nt" || scopeRaw === "new") scope = "nt";
    const limit = Math.min(80, Math.max(1, Number(req.query.limit) || 40));

    const allowed = new Set(listSearchableScriptureVersionIds());
    if (!versionId || !allowed.has(versionId)) {
      return res.status(400).json({ error: "请选择有效的经文版本" });
    }

    if (!q || q.length < 1) {
      return res.json({
        ok: true,
        versionId,
        query: q,
        scope,
        matches: [],
      });
    }

    if (q.length > 80) {
      return res.status(400).json({ error: "关键词过长" });
    }

    const index = getScriptureSearchIndex(versionId);
    const hits = searchScriptureVersesInIndex(index, q, scope, limit);
    const matches = hits.map((row) => ({
      bookId: row.bookId,
      bookLabel: row.bookCn || row.bookId,
      chapter: row.chapter,
      verse: row.verse,
      snippet: scriptureSearchSnippet(row.text, q, 72),
    }));

    res.json({
      ok: true,
      versionId,
      query: q,
      scope,
      matches,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: error.message || "经文搜索失败",
    });
  }
});

app.get("/api/scripture", (req, res) => {
  try {
    const { bookId, chapter, versions } = req.query;

    if (!bookId || !chapter || !versions) {
      return res.status(400).json({
        error: "缺少 bookId / chapter / versions",
      });
    }

    const scriptureVersionIds = String(versions)
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);
    const cacheKey = `scripture:${String(bookId)}:${Number(chapter)}:${scriptureVersionIds.join(
      ","
    )}`;
    const cached = getReadCache(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const rows = getMultiVersionScriptureRows({
      scriptureVersionIds,
      bookId: String(bookId),
      chapter: Number(chapter),
    });

    const payload = { rows };
    setReadCache(cacheKey, payload);
    res.json(payload);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: error.message || "经文读取失败",
    });
  }
});

/** 读经页章末人物带：keyPeople = 全局章人物表 + 已发布 JSON 的 chapterKeyPeople + 段标题正则，再匹配人物库立绘（与版本/语言无关） */
function generatedImageUrlExists(url) {
  const raw = String(url || "").trim();
  if (!raw) return false;
  if (!raw.startsWith("/generated/")) return true;
  return Boolean(resolveSafeGeneratedPngPath(raw));
}

function resolveExistingChapterRosterPortrait(entry, preferredSlot) {
  const primary = resolveChapterRosterPortrait(entry, preferredSlot);
  if (generatedImageUrlExists(primary.url)) {
    return primary;
  }
  const hero = String(entry?.heroImageUrl || "").trim();
  if (hero && generatedImageUrlExists(hero)) {
    return { url: hero, portraitSlot: null };
  }
  const periods = Array.isArray(entry?.periods) ? entry.periods : [];
  const slots = [String(entry?.imageUrl || "").trim()];
  for (let i = 0; i < periods.length; i++) {
    slots.push(String(periods[i]?.imageUrl || "").trim());
  }
  for (let i = 0; i < slots.length; i++) {
    if (slots[i] && generatedImageUrlExists(slots[i])) {
      return { url: slots[i], portraitSlot: i };
    }
  }
  return { url: "", portraitSlot: null };
}

function readerRosterScaleForEntry(entry, portraitSlot) {
  const slotIndex = Math.max(0, Number(portraitSlot) || 0);
  const statureClass = statureClassForSlot(entry, slotIndex);
  let layoutScale = 1;
  if (statureClass === "child" || statureClass === "youth") {
    layoutScale = layoutScaleHintForStature(statureClass);
  }
  layoutScale = Math.min(1, Math.max(0.5, Number(layoutScale) || 1));
  return { statureClass, layoutScale };
}

function buildChapterCharacterFiguresForReader(chapterData, meta) {
  try {
    if (!chapterData || typeof chapterData !== "object") return [];
    const payload = buildChapterPayloadFromPublished(chapterData, meta, {
      globalKeyPeople: loadChapterKeyPeopleGlobal(meta.bookId, meta.chapter),
    });
    const names = Array.isArray(payload.keyPeople) ? payload.keyPeople : [];
    const slotByZh =
      payload.characterFigurePortraitSlotByZh &&
      typeof payload.characterFigurePortraitSlotByZh === "object"
        ? payload.characterFigurePortraitSlotByZh
        : {};
    const stageRules = loadCharacterStageRules();
    const profilesRoot = loadCharacterIllustrationProfiles();
    const ch =
      profilesRoot.characters && typeof profilesRoot.characters === "object"
        ? profilesRoot.characters
        : {};
    const figures = [];
    const seen = new Set();
    for (let i = 0; i < names.length; i++) {
      const zh = String(names[i] || "").trim();
      if (!zh || seen.has(zh)) continue;
      seen.add(zh);
      const entry = ch[zh];
      if (!entry || typeof entry !== "object") continue;
      const chapterStage = resolveCharacterRosterSlotByChapterStage(
        stageRules,
        meta.bookId,
        meta.chapter,
        zh
      );
      const pref = Object.prototype.hasOwnProperty.call(slotByZh, zh)
        ? slotByZh[zh]
        : chapterStage?.slotIndex;
      const resolved = resolveExistingChapterRosterPortrait(entry, pref);
      const imageUrl = appendGeneratedAssetVersion(
        normalizeIllustrationImageUrlForPublication(resolved.url)
      );
      if (!imageUrl) continue;
      const row = { zhName: zh, imageUrl };
      row.sourceBookId = resolveCharacterSourceBookId(zh, entry.sourceBookId || "");
      row.characterRoleZh = resolveCharacterRoleZh(
        zh,
        entry.characterRoleZh || "",
        row.sourceBookId
      );
      row.isPrimaryCharacter = row.characterRoleZh === "主人物";
      const readerScale = readerRosterScaleForEntry(entry, resolved.portraitSlot);
      row.statureClass = readerScale.statureClass;
      row.layoutScale = readerScale.layoutScale;
      if (typeof resolved.portraitSlot === "number") {
        row.portraitSlot = resolved.portraitSlot;
      }
      if (chapterStage?.stageId) {
        row.stageId = chapterStage.stageId;
      }
      if (chapterStage?.labelZh) {
        row.stageLabelZh = chapterStage.labelZh;
      }
      const rt = appendGeneratedAssetVersion(rosterThumbRelativeUrlIfExists(imageUrl));
      if (rt) row.rosterThumbUrl = rt;
      figures.push(row);
    }
    figures.sort((x, y) =>
      compareZhNamesByBibleRosterOrder(x.zhName, y.zhName)
    );
    return figures;
  } catch (e) {
    console.error("[buildChapterCharacterFiguresForReader]", e);
    return [];
  }
}

function buildBookCharacterTimelineForReader(chapterData, meta) {
  try {
    const bookId = String(meta?.bookId || "").trim();
    const chapterNum = Number(meta?.chapter);
    if (!bookId) {
      return { figures: [], activeNames: [], focusNames: [] };
    }

    const presetBook = CHARACTER_PRESET_BY_BOOK.find(
      (row) => String(row?.bookId || "").trim() === bookId
    );
    const presetNames = Array.isArray(presetBook?.names) ? presetBook.names : [];
    const activeNames =
      chapterData && typeof chapterData === "object" && Number.isFinite(chapterNum) && chapterNum >= 1
        ? buildChapterPayloadFromPublished(chapterData, meta, {
            globalKeyPeople: loadChapterKeyPeopleGlobal(bookId, chapterNum),
          }).keyPeople || []
        : [];
    const activeSet = new Set(
      sanitizeChapterKeyPeopleArray(activeNames).map((name) => String(name || "").trim())
    );

    const stageRules = loadCharacterStageRules();
    const profilesRoot = loadCharacterIllustrationProfiles();
    const ch =
      profilesRoot.characters && typeof profilesRoot.characters === "object"
        ? profilesRoot.characters
        : {};

    const figures = [];
    const seen = new Set();

    for (let i = 0; i < presetNames.length; i++) {
      const zh = String(presetNames[i] || "").trim();
      if (!zh || seen.has(zh)) continue;
      seen.add(zh);
      const entry = ch[zh];
      if (!entry || typeof entry !== "object") continue;
      const chapterStage =
        Number.isFinite(chapterNum) && chapterNum >= 1
          ? resolveCharacterRosterSlotByChapterStage(
              stageRules,
              bookId,
              chapterNum,
              zh
            )
          : null;
      const resolved = resolveExistingChapterRosterPortrait(
        entry,
        activeSet.has(zh) ? chapterStage?.slotIndex : undefined
      );
      const imageUrl = appendGeneratedAssetVersion(
        normalizeIllustrationImageUrlForPublication(resolved.url)
      );
      if (!imageUrl) continue;
      const row = {
        zhName: zh,
        imageUrl,
        isCurrentChapter: activeSet.has(zh),
      };
      row.sourceBookId = resolveCharacterSourceBookId(zh, entry.sourceBookId || "");
      row.characterRoleZh = resolveCharacterRoleZh(
        zh,
        entry.characterRoleZh || "",
        row.sourceBookId
      );
      row.isPrimaryCharacter = row.characterRoleZh === "主人物";
      const readerScale = readerRosterScaleForEntry(entry, resolved.portraitSlot);
      row.statureClass = readerScale.statureClass;
      row.layoutScale = readerScale.layoutScale;
      if (typeof resolved.portraitSlot === "number") {
        row.portraitSlot = resolved.portraitSlot;
      }
      if (row.isCurrentChapter && chapterStage?.stageId) {
        row.stageId = chapterStage.stageId;
      }
      if (row.isCurrentChapter && chapterStage?.labelZh) {
        row.stageLabelZh = chapterStage.labelZh;
      }
      const rt = appendGeneratedAssetVersion(rosterThumbRelativeUrlIfExists(imageUrl));
      if (rt) row.rosterThumbUrl = rt;
      figures.push(row);
    }

    const extraActiveNames = [...activeSet].filter((zh) => !seen.has(zh));
    extraActiveNames.sort(compareZhNamesByBibleRosterOrder);
    for (let i = 0; i < extraActiveNames.length; i++) {
      const zh = extraActiveNames[i];
      const entry = ch[zh];
      if (!entry || typeof entry !== "object") continue;
      const chapterStage =
        Number.isFinite(chapterNum) && chapterNum >= 1
          ? resolveCharacterRosterSlotByChapterStage(
              stageRules,
              bookId,
              chapterNum,
              zh
            )
          : null;
      const resolved = resolveExistingChapterRosterPortrait(
        entry,
        chapterStage?.slotIndex
      );
      const imageUrl = appendGeneratedAssetVersion(
        normalizeIllustrationImageUrlForPublication(resolved.url)
      );
      if (!imageUrl) continue;
      const row = {
        zhName: zh,
        imageUrl,
        isCurrentChapter: true,
      };
      const readerScale = readerRosterScaleForEntry(entry, resolved.portraitSlot);
      row.statureClass = readerScale.statureClass;
      row.layoutScale = readerScale.layoutScale;
      if (typeof resolved.portraitSlot === "number") {
        row.portraitSlot = resolved.portraitSlot;
      }
      if (chapterStage?.stageId) {
        row.stageId = chapterStage.stageId;
      }
      if (chapterStage?.labelZh) {
        row.stageLabelZh = chapterStage.labelZh;
      }
      const rt = appendGeneratedAssetVersion(rosterThumbRelativeUrlIfExists(imageUrl));
      if (rt) row.rosterThumbUrl = rt;
      figures.push(row);
    }

    const focusNames = figures
      .filter((row) => row.isCurrentChapter)
      .map((row) => row.zhName);

    return {
      figures,
      activeNames: [...activeSet],
      focusNames,
    };
  } catch (e) {
    console.error("[buildBookCharacterTimelineForReader]", e);
    return { figures: [], activeNames: [], focusNames: [] };
  }
}

app.get("/api/study-content", (req, res) => {
  try {
    const { version, lang, bookId, chapter } = req.query;

    if (!version || !lang || !bookId || !chapter) {
      return res.status(400).json({
        error: "缺少 version / lang / bookId / chapter",
      });
    }

    const cacheKey = `${STUDY_CONTENT_CACHE_TAG}:${String(version)}:${String(lang)}:${String(
      bookId
    )}:${Number(chapter)}`;
    const cached = getReadCache(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    let data = readPublishedContent({
      versionId: String(version),
      lang: String(lang),
      bookId: String(bookId),
      chapter: Number(chapter),
    });

    if (!data) {
      /* 未发布该语言/版本 JSON 时仍返回 canonical 章节插图，便于读经页与已发布的 zh 等共用同一图 */
      const withIll = mergeChapterIllustrationFromCanonicalIfMissing(
        {},
        String(version),
        String(lang),
        String(bookId),
        Number(chapter)
      );
      const ill = normalizeChapterIllustrationForSave(withIll.chapterIllustration);
      const payload = { missing: true, chapterCharacterFigures: [] };
      if (ill) {
        payload.chapterIllustration = ill;
      }
      setReadCache(cacheKey, payload);
      return res.json(payload);
    }

    data = mergeChapterIllustrationFromCanonicalIfMissing(
      data,
      String(version),
      String(lang),
      String(bookId),
      Number(chapter)
    );

    data = normalizeChapterIllustrationInPublishedReadPayload(data);

    data = applyPresetQuestionCorrectionsToStudyPayload(
      data,
      String(bookId),
      Number(chapter),
      String(version),
      String(lang)
    );

    const studyMeta = {
      versionId: String(version),
      lang: String(lang),
      bookId: String(bookId),
      chapter: Number(chapter),
    };
    data.chapterCharacterFigures = buildChapterCharacterFiguresForReader(
      data,
      studyMeta
    );

    setReadCache(cacheKey, data);
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: error.message || "读取内容失败",
    });
  }
});

/**
 * 仅返回章末人物立绘列表（与 study-content 内 chapterCharacterFigures 同源）。
 * 供读经页在旧缓存/旧响应未带 figures 时补拉，也便于单独调试。
 */
app.get("/api/study-character-figures", (req, res) => {
  try {
    const { version, lang, bookId, chapter } = req.query;
    if (!version || !lang || !bookId || !chapter) {
      return res.status(400).json({
        error: "缺少 version / lang / bookId / chapter",
      });
    }
    const data = readPublishedContent({
      versionId: String(version),
      lang: String(lang),
      bookId: String(bookId),
      chapter: Number(chapter),
    });
    if (!data) {
      return res.json({ figures: [] });
    }
    const studyMeta = {
      versionId: String(version),
      lang: String(lang),
      bookId: String(bookId),
      chapter: Number(chapter),
    };
    const figures = buildChapterCharacterFiguresForReader(data, studyMeta);
    res.json({ figures });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: error.message || "读取章末人物失败",
    });
  }
});

app.get("/api/study-character-timeline", (req, res) => {
  try {
    const { version, lang, bookId, chapter } = req.query;
    if (!version || !lang || !bookId || !chapter) {
      return res.status(400).json({
        error: "缺少 version / lang / bookId / chapter",
      });
    }
    const studyMeta = {
      versionId: String(version),
      lang: String(lang),
      bookId: String(bookId),
      chapter: Number(chapter),
    };
    const data = readPublishedContent(studyMeta);
    const timeline = buildBookCharacterTimelineForReader(data || {}, studyMeta);
    res.json(timeline);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: error.message || "读取人物轴失败",
    });
  }
});

app.get("/api/book-intro", (req, res) => {
  try {
    const version = safeText(req.query.version || "");
    const lang = safeText(req.query.lang || "");
    const bookId = safeText(req.query.bookId || "");
    if (!version || !lang || !bookId) {
      return res.status(400).json({
        error: "缺少 version / lang / bookId",
      });
    }

    const cacheKey = `bookintro:${version}:${lang}:${bookId}`;
    const cached = getReadCache(cacheKey);
    if (cached) {
      return res.json(cached);
    }

    const rec = readPublishedBookIntro({
      versionId: version,
      lang,
      bookId,
    });
    if (!rec || !String(rec.markdown || "").trim()) {
      const payload = { missing: true, markdown: "", updatedAt: "" };
      setReadCache(cacheKey, payload);
      return res.json(payload);
    }

    const payload = {
      missing: false,
      markdown: rec.markdown,
      updatedAt: rec.updatedAt || "",
    };
    setReadCache(cacheKey, payload);
    res.json(payload);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: error.message || "读取书卷介绍失败",
    });
  }
});

app.get("/api/admin/book-intro", (req, res) => {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const version = safeText(req.query.version || "");
    const lang = safeText(req.query.lang || "");
    const bookId = safeText(req.query.bookId || "");
    if (!version || !lang || !bookId) {
      return res.status(400).json({ error: "缺少 version / lang / bookId" });
    }
    const rec = readPublishedBookIntro({ versionId: version, lang, bookId });
    res.json({
      markdown: rec?.markdown || "",
      updatedAt: rec?.updatedAt || "",
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取失败" });
  }
});

app.post("/api/admin/book-intro", (req, res) => {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const version = safeText(req.body?.version || "");
    const lang = safeText(req.body?.lang || "");
    const bookId = safeText(req.body?.bookId || "");
    if (!version || !lang || !bookId) {
      return res.status(400).json({ error: "缺少 version / lang / bookId" });
    }
    if (!Object.prototype.hasOwnProperty.call(req.body || {}, "markdown")) {
      return res.status(400).json({ error: "请求体须包含 markdown 字段" });
    }
    let markdown = String(req.body.markdown ?? "");
    if (markdown.length > BOOK_INTRO_MAX_MARKDOWN) {
      markdown = markdown.slice(0, BOOK_INTRO_MAX_MARKDOWN);
    }
    const filePath = getPublishedBookIntroFilePath({
      versionId: version,
      lang,
      bookId,
    });
    writeJson(filePath, {
      markdown,
      updatedAt: new Date().toISOString(),
    });
    clearReadCacheByPrefix(`bookintro:${version}:${lang}:`);
    res.json({ ok: true, bytes: markdown.length });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存失败" });
  }
});

function sendChapterVideoFile(req, res, filePath, ext) {
  const contentType = ext === "webm" ? "video/webm" : "video/mp4";
  const cv = String((req.query && req.query._cv) || "").trim();
  const cacheCtl = cv
    ? "public, max-age=31536000, immutable"
    : "public, max-age=86400, stale-while-revalidate=604800";
  let stat;
  try {
    stat = fs.statSync(filePath);
  } catch {
    return res.status(404).json({ error: "视频文件不存在" });
  }
  const fileSize = stat.size;
  const range = req.headers.range;
  if (range) {
    const m = /^bytes=(\d*)-(\d*)$/i.exec(String(range));
    if (!m) {
      res.status(416);
      res.setHeader("Content-Range", `bytes */${fileSize}`);
      return res.end();
    }
    let start;
    let end;
    if (m[1] === "") {
      const suffixLen = Number(m[2]);
      if (!Number.isFinite(suffixLen) || suffixLen <= 0) {
        res.status(416);
        res.setHeader("Content-Range", `bytes */${fileSize}`);
        return res.end();
      }
      start = Math.max(0, fileSize - suffixLen);
      end = fileSize - 1;
    } else {
      start = Number(m[1]);
      end = m[2] === "" || m[2] === undefined ? fileSize - 1 : Number(m[2]);
    }
    if (
      !Number.isFinite(start) ||
      !Number.isFinite(end) ||
      start < 0 ||
      start >= fileSize ||
      end < start
    ) {
      res.status(416);
      res.setHeader("Content-Range", `bytes */${fileSize}`);
      return res.end();
    }
    end = Math.min(end, fileSize - 1);
    const chunkSize = end - start + 1;
    res.status(206);
    res.setHeader("Content-Range", `bytes ${start}-${end}/${fileSize}`);
    res.setHeader("Accept-Ranges", "bytes");
    res.setHeader("Content-Length", String(chunkSize));
    res.setHeader("Content-Type", contentType);
    res.setHeader("Cache-Control", cacheCtl);
    res.setHeader("X-Content-Type-Options", "nosniff");
    const stream = fs.createReadStream(filePath, { start, end });
    stream.on("error", () => {
      if (!res.headersSent) res.status(500).end();
    });
    stream.pipe(res);
    return;
  }
  res.status(200);
  res.setHeader("Content-Length", String(fileSize));
  res.setHeader("Accept-Ranges", "bytes");
  res.setHeader("Content-Type", contentType);
  res.setHeader("Cache-Control", cacheCtl);
  res.setHeader("X-Content-Type-Options", "nosniff");
  const stream = fs.createReadStream(filePath);
  stream.on("error", () => {
    if (!res.headersSent) res.status(500).end();
  });
  stream.pipe(res);
}

/** 公开：章节附属视频（支持 Range，便于播放器拖动进度） */
app.get("/api/published/chapter-video", (req, res) => {
  try {
    const versionId = safeText(req.query.version || "");
    const lang = safeText(req.query.lang || "");
    const bookId = safeText(req.query.bookId || "");
    const chapter = parseNonNegativeChapterInt(req.query.chapter);
    const id = safeText(req.query.id || "");
    if (!versionId || !lang || !bookId || chapter === null || !id) {
      return res.status(400).json({ error: "缺少 version / lang / bookId / chapter / id" });
    }
    const hit = resolveChapterVideoFilePath(versionId, lang, bookId, chapter, id);
    if (!hit) {
      return res.status(404).json({ error: "视频不存在" });
    }
    sendChapterVideoFile(req, res, hit.filePath, hit.ext);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取视频失败" });
  }
});

function normalizeLookupIp(address) {
  const s = String(address || "").trim();
  if (s.toLowerCase().startsWith("::ffff:")) return s.slice(7);
  return s;
}

function isBlockedSsrfIp(rawIp) {
  const ip = normalizeLookupIp(rawIp);
  if (ip === "127.0.0.1" || ip === "0.0.0.0") return true;
  if (ip === "::1") return true;
  const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(ip);
  if (m) {
    const a = Number(m[1]);
    const b = Number(m[2]);
    const c = Number(m[3]);
    const d = Number(m[4]);
    if ([a, b, c, d].some((x) => x > 255)) return true;
    if (a === 10) return true;
    if (a === 127) return true;
    if (a === 0) return true;
    if (a === 169 && b === 254) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 192 && b === 168) return true;
    return false;
  }
  const lower = ip.toLowerCase();
  if (lower === "::1") return true;
  if (lower.startsWith("fe80:")) return true;
  if (/^f[cd][0-9a-f]{2}:/i.test(lower)) return true;
  if (lower.startsWith("::ffff:")) return isBlockedSsrfIp(lower.slice(7));
  return false;
}

async function assertChapterVideoImportUrlSafe(urlString) {
  let u;
  try {
    u = new URL(urlString);
  } catch {
    throw new Error("无效网址");
  }
  if (u.username || u.password) throw new Error("网址不可包含用户名或密码");
  if (u.protocol !== "http:" && u.protocol !== "https:") {
    throw new Error("仅支持 http / https 直链");
  }
  const host = u.hostname.toLowerCase();
  if (
    host === "localhost" ||
    host.endsWith(".localhost") ||
    host === "metadata.google.internal"
  ) {
    throw new Error("禁止访问该主机");
  }
  if (isBlockedSsrfIp(host)) throw new Error("禁止访问内网或保留地址");
  let records;
  try {
    records = await dns.lookup(host, { all: true });
  } catch (e) {
    throw new Error("域名无法解析：" + (e.message || "失败"));
  }
  const list = Array.isArray(records) ? records : [records];
  for (const r of list) {
    const addr = r && typeof r === "object" ? r.address : String(r || "");
    if (isBlockedSsrfIp(addr)) {
      throw new Error("域名解析到不可访问的地址");
    }
  }
}

const CHAPTER_VIDEO_URL_FETCH_MAX_REDIRECTS = 5;
const CHAPTER_VIDEO_URL_FETCH_TIMEOUT_MS = 15 * 60 * 1000;

async function fetchChapterVideoResponseWithSsrfChecks(startUrl) {
  let url = startUrl;
  for (let hop = 0; hop <= CHAPTER_VIDEO_URL_FETCH_MAX_REDIRECTS; hop += 1) {
    await assertChapterVideoImportUrlSafe(url);
    const ctrl = new AbortController();
    const tid = setTimeout(() => ctrl.abort(), CHAPTER_VIDEO_URL_FETCH_TIMEOUT_MS);
    let res;
    try {
      res = await fetch(url, {
        method: "GET",
        redirect: "manual",
        signal: ctrl.signal,
        headers: {
          Accept: "video/mp4,video/webm,application/octet-stream;q=0.9,*/*;q=0.5",
          "User-Agent": "AskBibleChapterVideoImport/1.0",
        },
      });
    } finally {
      clearTimeout(tid);
    }
    if (res.status >= 300 && res.status < 400) {
      const loc = res.headers.get("location");
      if (!loc) throw new Error("重定向缺少 Location");
      url = new URL(loc, url).href;
      continue;
    }
    if (!res.ok) {
      throw new Error(`拉取失败 HTTP ${res.status}`);
    }
    return { res, finalUrl: url };
  }
  throw new Error("重定向次数过多");
}

function inferChapterVideoExtFromUrlAndType(urlStr, contentType) {
  const u = String(urlStr || "").toLowerCase();
  if (u.includes(".webm") && !u.includes(".webm.")) return "webm";
  if (u.includes(".mp4") && !u.includes(".mp4.")) return "mp4";
  const ct = String(contentType || "").split(";")[0].trim().toLowerCase();
  if (ct.includes("webm")) return "webm";
  if (ct.includes("mp4")) return "mp4";
  if (ct === "application/octet-stream" || ct === "binary/octet-stream") {
    return "mp4";
  }
  return "mp4";
}

async function downloadChapterVideoFromUrlToTmp(urlString) {
  const { res, finalUrl } = await fetchChapterVideoResponseWithSsrfChecks(urlString);
  const ct = String(res.headers.get("content-type") || "");
  const cl = res.headers.get("content-length");
  if (cl && Number(cl) > CHAPTER_VIDEO_MAX_BYTES) {
    throw new Error("远程文件超过 250MB");
  }
  const ext = inferChapterVideoExtFromUrlAndType(finalUrl, ct);
  const ctMain = ct.split(";")[0].trim().toLowerCase();
  const mimeOk =
    ext === "webm"
      ? ctMain.includes("webm") ||
        ctMain.includes("octet-stream") ||
        ctMain === ""
      : ctMain.includes("mp4") ||
        ctMain.includes("octet-stream") ||
        ctMain.includes("video/") ||
        ctMain === "";
  if (!mimeOk && ctMain && !ctMain.includes("octet-stream")) {
    throw new Error(`不支持的资源类型：${ctMain || "未知"}（需 mp4 或 webm 直链）`);
  }
  const mime = ext === "webm" ? "video/webm" : "video/mp4";
  fs.mkdirSync(CHAPTER_VIDEO_UPLOAD_TMP, { recursive: true });
  const tmpName = `url_${Date.now()}_${crypto.randomBytes(8).toString("hex")}.${ext}`;
  const tmpPath = path.join(CHAPTER_VIDEO_UPLOAD_TMP, tmpName);
  if (!res.body) throw new Error("响应无正文");
  const nodeIn = Readable.fromWeb(res.body);
  let received = 0;
  const limiter = new Transform({
    transform(chunk, _enc, cb) {
      received += chunk.length;
      if (received > CHAPTER_VIDEO_MAX_BYTES) {
        cb(new Error("下载超过 250MB 已中止"));
        return;
      }
      cb(null, chunk);
    },
  });
  try {
    await pipeline(nodeIn, limiter, fs.createWriteStream(tmpPath));
  } catch (e) {
    try {
      fs.unlinkSync(tmpPath);
    } catch {
      /* ignore */
    }
    throw e;
  }
  return { path: tmpPath, mimetype: mime, ext };
}

function chapterVideoUploadMulterMiddleware(req, res, next) {
  chapterVideoMulter.single("video")(req, res, (err) => {
    if (err) {
      const msg =
        err.code === "LIMIT_FILE_SIZE"
          ? "文件过大（单文件最大 250MB）"
          : String(err.message || "上传失败");
      return res.status(400).json({ error: msg });
    }
    next();
  });
}

async function handleChapterVideoUploadPost(req, res) {
  let urlTmpPath = null;
  const file = req.file;
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const metaObj = parseUploadMetaJson(req);
    const videoUrlRaw = safeText(
      metaObj?.videoUrl ?? req.body?.videoUrl ?? ""
    ).trim();
    if (videoUrlRaw.length > 2048) {
      if (file?.path) {
        try {
          fs.unlinkSync(file.path);
        } catch {
          /* ignore */
        }
      }
      return res.status(400).json({ error: "网址过长（最多 2048 字符）" });
    }
    const videoUrl = videoUrlRaw;

    let tempPath = null;
    let mimetype = "";

    if (file?.path) {
      tempPath = file.path;
      mimetype = String(file.mimetype || "");
    } else if (videoUrl) {
      try {
        const got = await downloadChapterVideoFromUrlToTmp(videoUrl);
        urlTmpPath = got.path;
        tempPath = got.path;
        mimetype = got.mimetype;
      } catch (e) {
        const msg = String(e?.message || e || "从网址拉取失败");
        return res.status(400).json({ error: msg });
      }
    } else {
      return res.status(400).json({
        error:
          "请选择视频文件（字段 video），或在 meta 中填写可直链的 videoUrl（mp4/webm）。",
      });
    }

    const versionId = pickStrPreferFlat(req.body?.version, metaObj?.version);
    const lang = pickStrPreferFlat(req.body?.lang, metaObj?.lang);
    const bookId = pickStrPreferFlat(req.body?.bookId, metaObj?.bookId);
    const chapter = parseNonNegativeChapterInt(
      pickChapterRawForUpload(req, metaObj)
    );
    const title = safeText(
      pickStrPreferFlat(req.body?.title, metaObj?.title)
    ).slice(0, 200);
    if (!versionId || !lang || !bookId || chapter === null) {
      try {
        if (tempPath) fs.unlinkSync(tempPath);
      } catch {
        /* ignore */
      }
      return res.status(400).json({ error: "缺少 version / lang / bookId / chapter" });
    }
    const publishedPath = getPublishedContentFilePath({
      versionId,
      lang,
      bookId,
      chapter,
    });
    let data = readJson(publishedPath, null);
    if (!data || typeof data !== "object") {
      if (chapter === 0) {
        data = ensurePublishedJsonForBookLandingVideos(versionId, lang, bookId);
      }
    }
    if (!data || typeof data !== "object") {
      try {
        if (tempPath) fs.unlinkSync(tempPath);
      } catch {
        /* ignore */
      }
      return res
        .status(400)
        .json({ error: "该章尚未发布查经内容，请先生成并发布本章后再上传视频。" });
    }
    const videos = normalizeChapterVideosForSave(data.chapterVideos);
    if (videos.length >= MAX_CHAPTER_VIDEOS_PER_CHAPTER) {
      try {
        if (tempPath) fs.unlinkSync(tempPath);
      } catch {
        /* ignore */
      }
      return res.status(400).json({
        error: `每章最多 ${MAX_CHAPTER_VIDEOS_PER_CHAPTER} 个视频，请先删除后再传。`,
      });
    }
    const mime = String(mimetype || "");
    const ext = mime === "video/webm" ? "webm" : "mp4";
    if (mime !== "video/webm" && mime !== "video/mp4") {
      try {
        if (tempPath) fs.unlinkSync(tempPath);
      } catch {
        /* ignore */
      }
      return res.status(400).json({ error: "仅支持 MP4 / WebM" });
    }
    const id = crypto.randomBytes(16).toString("hex");
    const dir = getChapterVideosDir(versionId, lang, bookId, chapter);
    fs.mkdirSync(dir, { recursive: true });
    fs.mkdirSync(CHAPTER_VIDEO_UPLOAD_TMP, { recursive: true });
    const dest = path.join(dir, `${id}.${ext}`);
    moveUploadedFileToFinal(tempPath, dest);
    urlTmpPath = null;
    const entry = {
      id,
      title: title || `视频 ${videos.length + 1}`,
      mime: ext === "webm" ? "video/webm" : "video/mp4",
      ext,
      updatedAt: nowIso(),
    };
    const nextVideos = [...videos, entry];
    const merged = { ...data, chapterVideos: nextVideos };
    writeJson(publishedPath, merged);
    invalidateStudyContentCache(versionId, lang, bookId, chapter);
    appendAdminAudit(req, authed, "chapter_video_upload", {
      versionId,
      lang,
      bookId,
      chapter,
      videoId: id,
      source: file?.path ? "upload" : "url",
    });
    res.json({ ok: true, video: entry, chapterVideos: nextVideos });
  } catch (error) {
    if (urlTmpPath) {
      try {
        fs.unlinkSync(urlTmpPath);
      } catch {
        /* ignore */
      }
    }
    if (file?.path) {
      try {
        fs.unlinkSync(file.path);
      } catch {
        /* ignore */
      }
    }
    console.error(error);
    if (!res.headersSent) {
      res.status(500).json({ error: error.message || "上传失败" });
    }
  }
}

function chapterVideoUploadPostRoute(req, res) {
  void handleChapterVideoUploadPost(req, res).catch((err) => {
    console.error(err);
    if (!res.headersSent) {
      res.status(500).json({ error: err.message || "上传失败" });
    }
  });
}

/* 与 GET /api/admin/published/chapter 同前缀，便于反代只放行 published 子路径时仍能上传 */
app.post(
  "/api/admin/published/chapter-video-upload",
  chapterVideoUploadMulterMiddleware,
  chapterVideoUploadPostRoute
);
app.post(
  "/api/admin/chapter-video/upload",
  chapterVideoUploadMulterMiddleware,
  chapterVideoUploadPostRoute
);

app.delete("/api/admin/chapter-video", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const versionId = safeText(req.query.version || "");
    const lang = safeText(req.query.lang || "");
    const bookId = safeText(req.query.bookId || "");
    const chapter = parseNonNegativeChapterInt(req.query.chapter);
    const id = safeText(req.query.id || "");
    if (!versionId || !lang || !bookId || chapter === null || !id) {
      return res
        .status(400)
        .json({ error: "缺少 version / lang / bookId / chapter / id" });
    }
    if (!isSafeChapterVideoId(id)) {
      return res.status(400).json({ error: "无效的视频 id" });
    }
    const publishedPath = getPublishedContentFilePath({
      versionId,
      lang,
      bookId,
      chapter,
    });
    const data = readJson(publishedPath, null);
    if (!data || typeof data !== "object") {
      return res.status(404).json({ error: "未找到已发布章节" });
    }
    const videos = normalizeChapterVideosForSave(data.chapterVideos);
    const idx = videos.findIndex((v) => String(v.id).toLowerCase() === id.toLowerCase());
    if (idx < 0) {
      return res.status(404).json({ error: "列表中无此视频" });
    }
    const hit = resolveChapterVideoFilePath(versionId, lang, bookId, chapter, id);
    if (hit) {
      try {
        fs.unlinkSync(hit.filePath);
      } catch {
        /* ignore */
      }
    }
    unlinkChapterVideoPosterFiles(versionId, lang, bookId, chapter, id);
    const nextVideos = videos.filter((_, i) => i !== idx);
    const merged = { ...data, chapterVideos: nextVideos };
    writeJson(publishedPath, merged);
    invalidateStudyContentCache(versionId, lang, bookId, chapter);
    appendAdminAudit(req, authed, "chapter_video_delete", {
      versionId,
      lang,
      bookId,
      chapter,
      videoId: id,
    });
    res.json({ ok: true, chapterVideos: nextVideos });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "删除失败" });
  }
});

function handleChapterVideoPatchTitle(req, res) {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const versionId = safeText(req.body?.version || "");
    const lang = safeText(req.body?.lang || "");
    const bookId = safeText(req.body?.bookId || "");
    const chapter = parseNonNegativeChapterInt(req.body?.chapter);
    const id = safeText(req.body?.id || "");
    if (!versionId || !lang || !bookId || chapter === null || !id) {
      return res
        .status(400)
        .json({ error: "缺少 version / lang / bookId / chapter / id" });
    }
    if (!isSafeChapterVideoId(id)) {
      return res.status(400).json({ error: "无效的视频 id" });
    }
    let title = safeText(req.body?.title ?? "").slice(0, 200);
    const publishedPath = getPublishedContentFilePath({
      versionId,
      lang,
      bookId,
      chapter,
    });
    const data = readJson(publishedPath, null);
    if (!data || typeof data !== "object") {
      return res.status(404).json({ error: "未找到已发布章节" });
    }
    const videos = normalizeChapterVideosForSave(data.chapterVideos);
    const idx = videos.findIndex(
      (v) => String(v.id).toLowerCase() === id.toLowerCase()
    );
    if (idx < 0) {
      return res.status(404).json({ error: "列表中无此视频" });
    }
    const trimmed = title.trim();
    if (!trimmed) {
      title = `视频 ${idx + 1}`;
    } else {
      title = trimmed;
    }
    const nextVideos = videos.map((v, i) =>
      i === idx ? { ...v, title } : v
    );
    const merged = { ...data, chapterVideos: nextVideos };
    writeJson(publishedPath, merged);
    invalidateStudyContentCache(versionId, lang, bookId, chapter);
    appendAdminAudit(req, authed, "chapter_video_title", {
      versionId,
      lang,
      bookId,
      chapter,
      videoId: id,
    });
    res.json({ ok: true, video: nextVideos[idx], chapterVideos: nextVideos });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "更新标题失败" });
  }
}

app.patch("/api/admin/published/chapter-video", handleChapterVideoPatchTitle);
app.patch("/api/admin/chapter-video", handleChapterVideoPatchTitle);

function handleChapterVideoMovePost(req, res) {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const versionId = safeText(req.body?.version || "");
    const lang = safeText(req.body?.lang || "");
    const fromBookId = safeText(req.body?.fromBookId || "");
    const fromChapter = parseNonNegativeChapterInt(req.body?.fromChapter);
    const id = safeText(req.body?.id || "");
    const toBookId = safeText(req.body?.toBookId || "");
    const toChapter = parseNonNegativeChapterInt(req.body?.toChapter);
    const rawInsert = req.body?.insertIndex;
    let insertIndex = null;
    if (rawInsert != null && String(rawInsert).trim() !== "") {
      const n = Number(rawInsert);
      if (Number.isFinite(n) && Number.isInteger(n) && n >= 1) {
        insertIndex = n - 1;
      }
    }
    if (!versionId || !lang || !fromBookId || !toBookId || !id) {
      return res.status(400).json({ error: "缺少 version / lang / fromBookId / toBookId / id" });
    }
    if (fromChapter === null || toChapter === null) {
      return res.status(400).json({ error: "缺少 fromChapter / toChapter" });
    }
    if (!isSafeChapterVideoId(id)) {
      return res.status(400).json({ error: "无效的视频 id" });
    }
    const errFrom = assertValidPublishedBookChapter(fromBookId, fromChapter);
    if (errFrom) return res.status(400).json({ error: errFrom });
    const errTo = assertValidPublishedBookChapter(toBookId, toChapter);
    if (errTo) return res.status(400).json({ error: errTo });
    if (fromBookId === toBookId && fromChapter === toChapter) {
      return res.status(400).json({ error: "目标与当前位置相同" });
    }
    const fromPath = getPublishedContentFilePath({
      versionId,
      lang,
      bookId: fromBookId,
      chapter: fromChapter,
    });
    const fromData = readJson(fromPath, null);
    if (!fromData || typeof fromData !== "object") {
      return res.status(404).json({ error: "来源章节未找到已发布 JSON" });
    }
    const fromVideos = normalizeChapterVideosForSave(fromData.chapterVideos);
    const vIdx = fromVideos.findIndex(
      (v) => String(v.id).toLowerCase() === id.toLowerCase()
    );
    if (vIdx < 0) {
      return res.status(404).json({ error: "来源列表中无此视频" });
    }
    const entry = { ...fromVideos[vIdx] };
    let toPath = getPublishedContentFilePath({
      versionId,
      lang,
      bookId: toBookId,
      chapter: toChapter,
    });
    let toData = readJson(toPath, null);
    if ((!toData || typeof toData !== "object") && toChapter === 0) {
      toData = ensurePublishedJsonForBookLandingVideos(versionId, lang, toBookId);
      toPath = getPublishedContentFilePath({
        versionId,
        lang,
        bookId: toBookId,
        chapter: toChapter,
      });
    }
    if (!toData || typeof toData !== "object") {
      return res.status(400).json({
        error:
          "目标章节尚无已发布查经 JSON，请先在后台生成并发布该章，或选择卷首页（章 0）",
      });
    }
    const toVideos = normalizeChapterVideosForSave(toData.chapterVideos);
    if (toVideos.some((v) => String(v.id).toLowerCase() === id.toLowerCase())) {
      return res.status(400).json({ error: "目标位置已存在相同 id 的视频" });
    }
    if (toVideos.length >= MAX_CHAPTER_VIDEOS_PER_CHAPTER) {
      return res.status(400).json({
        error: `目标章节视频已达上限（${MAX_CHAPTER_VIDEOS_PER_CHAPTER}），请先删除或迁出`,
      });
    }
    const hit = resolveChapterVideoFilePath(
      versionId,
      lang,
      fromBookId,
      fromChapter,
      id
    );
    if (!hit) {
      return res.status(404).json({ error: "未找到视频文件（磁盘）" });
    }
    const fromVDir = getChapterVideosDir(versionId, lang, fromBookId, fromChapter);
    const toVDir = getChapterVideosDir(versionId, lang, toBookId, toChapter);
    fs.mkdirSync(toVDir, { recursive: true });
    const destVideoPath = path.join(
      toVDir,
      `${String(id).toLowerCase()}.${hit.ext}`
    );
    if (fs.existsSync(destVideoPath)) {
      return res.status(400).json({ error: "目标目录已有同名视频文件" });
    }
    moveUploadedFileToFinal(hit.filePath, destVideoPath);
    moveChapterVideoPosterFilesBetweenDirs(fromVDir, toVDir, id);
    const posterUrl = chapterVideoPosterApiPath(
      versionId,
      lang,
      toBookId,
      toChapter,
      id
    );
    const nextEntry = {
      ...entry,
      posterUrl,
      posterUpdatedAt: entry.posterUpdatedAt || nowIso(),
      updatedAt: nowIso(),
    };
    const fromNext = fromVideos.filter((_, i) => i !== vIdx);
    const toNext = toVideos.slice();
    if (insertIndex == null || insertIndex < 0) {
      toNext.push(nextEntry);
    } else {
      const pos = Math.min(insertIndex, toNext.length);
      toNext.splice(pos, 0, nextEntry);
    }
    writeJson(fromPath, { ...fromData, chapterVideos: fromNext });
    writeJson(toPath, { ...toData, chapterVideos: toNext });
    invalidateStudyContentCache(versionId, lang, fromBookId, fromChapter);
    invalidateStudyContentCache(versionId, lang, toBookId, toChapter);
    appendAdminAudit(req, authed, "chapter_video_move", {
      versionId,
      lang,
      fromBookId,
      fromChapter,
      toBookId,
      toChapter,
      videoId: id,
    });
    res.json({
      ok: true,
      fromChapterVideos: fromNext,
      toChapterVideos: toNext,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "迁移失败" });
  }
}

app.post("/api/admin/published/chapter-video-move", handleChapterVideoMovePost);
app.post("/api/admin/chapter-video/move", handleChapterVideoMovePost);

/* =========================================================
   章节仿古插画：英文 Prompt 生成（MVP，本地 JSON 追加记录）
   ========================================================= */

const CHAPTER_PROMPT_LOG_FILE = path.join(__dirname, "data", "prompts.json");

function ensureChapterPromptLogFile() {
  const dir = path.dirname(CHAPTER_PROMPT_LOG_FILE);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  if (!fs.existsSync(CHAPTER_PROMPT_LOG_FILE)) {
    writeJson(CHAPTER_PROMPT_LOG_FILE, { entries: [] });
  }
}

function ensureChapterKeyPeopleFile() {
  ensureDir(path.dirname(CHAPTER_KEY_PEOPLE_FILE));
  if (!fs.existsSync(CHAPTER_KEY_PEOPLE_FILE)) {
    writeJson(CHAPTER_KEY_PEOPLE_FILE, {});
  }
}

function ensureCharacterStageRulesFile() {
  ensureDir(path.dirname(CHARACTER_STAGE_RULES_FILE));
  if (!fs.existsSync(CHARACTER_STAGE_RULES_FILE)) {
    writeJson(CHARACTER_STAGE_RULES_FILE, { characters: {}, books: {} });
  }
}

function loadCharacterStageRules() {
  try {
    ensureCharacterStageRulesFile();
    const root = readJson(CHARACTER_STAGE_RULES_FILE, {});
    return root && typeof root === "object" ? root : { characters: {}, books: {} };
  } catch (_) {
    return { characters: {}, books: {} };
  }
}

function findCharacterStageSpec(stageRulesRoot, zhName) {
  if (!stageRulesRoot || typeof stageRulesRoot !== "object") return null;
  const characters =
    stageRulesRoot.characters && typeof stageRulesRoot.characters === "object"
      ? stageRulesRoot.characters
      : {};
  const spec = characters[String(zhName || "").trim()];
  return spec && typeof spec === "object" ? spec : null;
}

function findStageDefById(stageSpec, stageId) {
  const target = String(stageId || "").trim();
  if (!stageSpec || typeof stageSpec !== "object" || !target) return null;
  const stages = Array.isArray(stageSpec.stages) ? stageSpec.stages : [];
  for (let i = 0; i < stages.length; i++) {
    const row = stages[i];
    if (!row || typeof row !== "object") continue;
    if (String(row.id || "").trim() === target) {
      return row;
    }
  }
  return null;
}

function normalizeStageSlotIndex(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.min(2, Math.floor(n)));
}

function resolveCharacterStageRuleForChapter(stageRulesRoot, bookId, chapter, zhName) {
  const bid = String(bookId || "").trim();
  const chNum = Number(chapter);
  const name = String(zhName || "").trim();
  if (!bid || !Number.isFinite(chNum) || chNum < 1 || !name) return null;
  const books =
    stageRulesRoot &&
    stageRulesRoot.books &&
    typeof stageRulesRoot.books === "object"
      ? stageRulesRoot.books
      : {};
  const byBook = books[bid];
  if (!byBook || typeof byBook !== "object") return null;
  const ranges = byBook[name];
  if (!Array.isArray(ranges)) return null;
  for (let i = 0; i < ranges.length; i++) {
    const row = ranges[i];
    if (!row || typeof row !== "object") continue;
    const from = Number(row.from);
    const toRaw = row.to == null || row.to === "" ? row.from : row.to;
    const to = Number(toRaw);
    if (!Number.isFinite(from) || !Number.isFinite(to)) continue;
    if (chNum < from || chNum > to) continue;
    const stageId = String(row.stageId || "").trim();
    if (!stageId) continue;
    return {
      stageId,
      labelZh: String(row.labelZh || "").trim(),
    };
  }
  return null;
}

function resolveCharacterRosterSlotByChapterStage(stageRulesRoot, bookId, chapter, zhName) {
  const stageSpec = findCharacterStageSpec(stageRulesRoot, zhName);
  const matched = resolveCharacterStageRuleForChapter(
    stageRulesRoot,
    bookId,
    chapter,
    zhName
  );
  if (matched) {
    const def = findStageDefById(stageSpec, matched.stageId);
    const slotIndex = normalizeStageSlotIndex(def?.slotIndex);
    if (slotIndex != null) {
      return {
        slotIndex,
        stageId: matched.stageId,
        labelZh: String(def?.labelZh || matched.labelZh || "").trim(),
      };
    }
  }
  const defaultStageId = String(stageSpec?.defaultStageId || "").trim();
  if (!defaultStageId) return null;
  const def = findStageDefById(stageSpec, defaultStageId);
  const slotIndex = normalizeStageSlotIndex(def?.slotIndex);
  if (slotIndex == null) return null;
  return {
    slotIndex,
    stageId: defaultStageId,
    labelZh: String(def?.labelZh || "").trim(),
  };
}

function loadChapterKeyPeopleGlobal(bookId, chapter) {
  try {
    ensureChapterKeyPeopleFile();
    const root = readJson(CHAPTER_KEY_PEOPLE_FILE, {});
    const bid = String(bookId || "").trim();
    const chNum = Number(chapter);
    if (!bid || !Number.isFinite(chNum) || chNum < 1) return [];
    const chKey = String(chNum);
    const byBook = root[bid];
    if (!byBook || typeof byBook !== "object") return [];
    const arr = byBook[chKey];
    return sanitizeChapterKeyPeopleArray(Array.isArray(arr) ? arr : []);
  } catch (_) {
    return [];
  }
}

const DEFAULT_CHARACTER_ILLUSTRATION_PROFILES_ROOT = {
  characters: {
    亚伯拉罕: {
      englishName: "Abraham",
      scripturePersonalityZh: "圣经中称许的信心之父，信而顺服，蒙召即往未知之地。",
      scripturePersonalityEn:
        "Portrayed as the father of faith—trusting God’s promise, leaving familiar ground in obedience; steady, reverent demeanor rather than bravado.",
      shortSceneTagEn: "lean patriarch with full beard and staff",
      appearanceEn:
        "Lean man about 75, full beard, weathered face, striped ancient Near Eastern robe to the ankle, simple belt, wooden walking staff, calm posture",
    },
    摩西: {
      englishName: "Moses",
      shortSceneTagEn: "strong-bearded man in desert robe",
      appearanceEn:
        "Man about 80, long white-streaked beard, sun-darkened skin, coarse desert robe, rope belt, holding wooden staff, upright bearing",
    },
    大卫: {
      englishName: "David",
      shortSceneTagEn: "ruddy young warrior-king with harp",
      appearanceEn:
        "Young adult male, ruddy complexion, curly dark hair and short beard, simple royal headband optional, knee-length tunic, lyre or sling implied by scene, athletic build",
    },
    撒拉: {
      englishName: "Sarah",
      shortSceneTagEn: "dignified matriarch beside Abraham",
      appearanceEn:
        "Woman past middle age, covered hair with simple veil, modest layered robe, composed posture, shorter than Abraham, warm but firm expression",
    },
  },
};

function persistCharacterIllustrationProfilesRoot(root) {
  if (characterProfilesUsesSqlite()) {
    saveCharacterProfilesRootToSqlite(root);
    return;
  }
  ensureDir(path.dirname(CHARACTER_ILLUSTRATION_PROFILES_FILE));
  writeJson(CHARACTER_ILLUSTRATION_PROFILES_FILE, root);
}

function ensureCharacterIllustrationProfilesFile() {
  if (characterProfilesUsesSqlite()) {
    migrateSeedJsonToSqliteIfEmpty(CHARACTER_PROFILES_SEED_JSON);
    if (countCharacterProfilesInSqlite() === 0) {
      saveCharacterProfilesRootToSqlite(
        JSON.parse(JSON.stringify(DEFAULT_CHARACTER_ILLUSTRATION_PROFILES_ROOT))
      );
    }
    return;
  }
  ensureDir(path.dirname(CHARACTER_ILLUSTRATION_PROFILES_FILE));
  if (!fs.existsSync(CHARACTER_ILLUSTRATION_PROFILES_FILE)) {
    writeJson(
      CHARACTER_ILLUSTRATION_PROFILES_FILE,
      JSON.parse(JSON.stringify(DEFAULT_CHARACTER_ILLUSTRATION_PROFILES_ROOT))
    );
  }
}

function loadCharacterIllustrationProfiles() {
  ensureCharacterIllustrationProfilesFile();
  const root = characterProfilesUsesSqlite()
    ? loadCharacterProfilesRootFromSqlite()
    : readJson(CHARACTER_ILLUSTRATION_PROFILES_FILE, { characters: {} });
  const recovered = applyCharacterProfileImageAuditRecovery(root);
  const englishBackfilled = applyCharacterProfileEnglishNameDefaults(recovered);
  const bookBackfilled = applyCharacterProfileSourceBookDefaults(englishBackfilled.root);
  const identityBackfilled = applyCharacterProfileIdentityDefaults(bookBackfilled.root);
  const roleBackfilled = applyCharacterProfileRoleDefaults(identityBackfilled.root);
  const lifespanBackfilled = applyCharacterProfileLifespanDefaults(roleBackfilled.root);
  if (
    englishBackfilled.changed ||
    bookBackfilled.changed ||
    identityBackfilled.changed ||
    roleBackfilled.changed ||
    lifespanBackfilled.changed
  ) {
    persistCharacterIllustrationProfilesRoot(lifespanBackfilled.root);
    rememberCharacterProfileImageAuditFromProfiles(lifespanBackfilled.root);
  }
  return lifespanBackfilled.root;
}

function loadCharacterProfileImageAudit() {
  const raw = readJson(CHARACTER_PROFILE_IMAGE_AUDIT_FILE, null);
  if (!raw || typeof raw !== "object") return { items: [] };
  return {
    items: Array.isArray(raw.items) ? raw.items : [],
  };
}

function saveCharacterProfileImageAudit(data) {
  ensureDir(path.dirname(CHARACTER_PROFILE_IMAGE_AUDIT_FILE));
  writeJson(CHARACTER_PROFILE_IMAGE_AUDIT_FILE, {
    items: Array.isArray(data?.items) ? data.items : [],
  });
}

function buildCharacterProfileImageAuditEntry(zhName, row) {
  const entry = row && typeof row === "object" ? row : {};
  const periods = Array.isArray(entry.periods) ? entry.periods : [];
  return {
    zhName: safeText(zhName || "").slice(0, 32),
    savedAt: nowIso(),
    heroImageUrl: safeText(entry.heroImageUrl || "").slice(0, 400),
    imageUrl: safeText(entry.imageUrl || "").slice(0, 400),
    comparisonSheetUrl: safeText(entry.comparisonSheetUrl || "").slice(0, 400),
    periods: periods.slice(0, 2).map((p) => ({
      imageUrl: safeText(p?.imageUrl || "").slice(0, 400),
    })),
  };
}

function rememberCharacterProfileImageAuditFromProfiles(profilesRoot) {
  const chars =
    profilesRoot &&
    typeof profilesRoot === "object" &&
    profilesRoot.characters &&
    typeof profilesRoot.characters === "object"
      ? profilesRoot.characters
      : {};
  const db = loadCharacterProfileImageAudit();
  const nextMap = new Map(
    (db.items || [])
      .filter((x) => x && typeof x === "object" && safeText(x.zhName || ""))
      .map((x) => [safeText(x.zhName || ""), x])
  );
  for (const [zhName, row] of Object.entries(chars)) {
    const snapshot = buildCharacterProfileImageAuditEntry(zhName, row);
    const refs = [
      snapshot.heroImageUrl,
      snapshot.imageUrl,
      snapshot.comparisonSheetUrl,
      ...(Array.isArray(snapshot.periods) ? snapshot.periods.map((p) => p.imageUrl) : []),
    ].filter(Boolean);
    if (!refs.length) continue;
    nextMap.set(snapshot.zhName, snapshot);
  }
  const items = [...nextMap.values()]
    .sort((a, b) => String(b.savedAt || "").localeCompare(String(a.savedAt || "")))
    .slice(0, 500);
  saveCharacterProfileImageAudit({ items });
}

function applyCharacterProfileImageAuditRecovery(profilesRoot) {
  const root =
    profilesRoot && typeof profilesRoot === "object" ? profilesRoot : { characters: {} };
  const chars =
    root.characters && typeof root.characters === "object" ? root.characters : {};
  const auditMap = new Map(
    loadCharacterProfileImageAudit()
      .items.filter((x) => x && typeof x === "object" && safeText(x.zhName || ""))
      .map((x) => [safeText(x.zhName || ""), x])
  );
  let changed = false;
  for (const [zhName, row] of Object.entries(chars)) {
    if (!row || typeof row !== "object") continue;
    const snap = auditMap.get(zhName);
    if (!snap) continue;
    const hero = safeText(row.heroImageUrl || "");
    const snapHero = safeText(snap.heroImageUrl || "");
    if ((!hero || !generatedUrlExists(hero)) && snapHero && generatedUrlExists(snapHero)) {
      row.heroImageUrl = snapHero;
      changed = true;
    }
    const img0 = safeText(row.imageUrl || "");
    const snapImg0 = safeText(snap.imageUrl || "");
    if ((!img0 || !generatedUrlExists(img0)) && snapImg0 && generatedUrlExists(snapImg0)) {
      row.imageUrl = snapImg0;
      changed = true;
    }
    const cmp = safeText(row.comparisonSheetUrl || "");
    const snapCmp = safeText(snap.comparisonSheetUrl || "");
    if ((!cmp || !generatedUrlExists(cmp)) && snapCmp && generatedUrlExists(snapCmp)) {
      row.comparisonSheetUrl = snapCmp;
      changed = true;
    }
    if (Array.isArray(row.periods) && Array.isArray(snap.periods)) {
      for (let i = 0; i < row.periods.length && i < snap.periods.length; i++) {
        const period = row.periods[i];
        const snapPeriod = snap.periods[i];
        if (!period || typeof period !== "object") continue;
        const cur = safeText(period.imageUrl || "");
        const old = safeText(snapPeriod?.imageUrl || "");
        if ((!cur || !generatedUrlExists(cur)) && old && generatedUrlExists(old)) {
          period.imageUrl = old;
          changed = true;
        }
      }
    }
  }
  if (changed) {
    persistCharacterIllustrationProfilesRoot(root);
    rememberCharacterProfileImageAuditFromProfiles(root);
  }
  return root;
}

function normalizeCharacterRefSelectionsFromBody(body) {
  const raw = body && body.characterRefSelections;
  if (!Array.isArray(raw) || raw.length === 0) return [];
  const out = [];
  for (const r of raw) {
    if (!r || typeof r !== "object") continue;
    const zhName = safeText(r.zhName || r.nameZh || "").slice(0, 32);
    if (!zhName) continue;
    let slotIndex = Number(r.slotIndex);
    if (!Number.isFinite(slotIndex)) slotIndex = 0;
    slotIndex = Math.max(0, Math.min(2, Math.floor(slotIndex)));
    out.push({ zhName, slotIndex });
    if (out.length >= 6) break;
  }
  return out;
}

/** 供 GPT 从档案中多选人物与时期：校验 zhName 存在且 slot 对该人物有效 */
function sanitizeGptCharacterRefSelections(parsed, profilesRoot) {
  const raw = parsed && parsed.characterRefSelections;
  if (!Array.isArray(raw)) return [];
  const ch =
    profilesRoot &&
    profilesRoot.characters &&
    typeof profilesRoot.characters === "object"
      ? profilesRoot.characters
      : {};
  const out = [];
  const seen = new Set();
  for (const row of raw) {
    if (!row || typeof row !== "object") continue;
    const zhName = safeText(row.zhName || row.nameZh || "").slice(0, 32);
    if (!zhName || !Object.prototype.hasOwnProperty.call(ch, zhName)) continue;
    const entry = ch[zhName];
    if (!entry || typeof entry !== "object") continue;
    let slotIndex = Number(row.slotIndex);
    if (!Number.isFinite(slotIndex)) slotIndex = 0;
    slotIndex = Math.max(0, Math.min(2, Math.floor(slotIndex)));
    const periods = Array.isArray(entry.periods) ? entry.periods : [];
    const maxSlotExclusive = 1 + Math.min(periods.length, 2);
    if (slotIndex >= maxSlotExclusive) slotIndex = 0;
    const key = zhName + "\0" + slotIndex;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ zhName, slotIndex });
    if (out.length >= 6) break;
  }
  return out;
}

function buildCharacterRosterForGptPrompt(profilesRoot) {
  const ch =
    profilesRoot &&
    profilesRoot.characters &&
    typeof profilesRoot.characters === "object"
      ? profilesRoot.characters
      : {};
  const names = Object.keys(ch).sort();
  if (!names.length) {
    return "（当前人物设计库为空：characterRefSelections 请输出 []）";
  }
  const lines = [];
  for (const name of names) {
    const e = ch[name];
    if (!e || typeof e !== "object") continue;
    const en = safeText(e.englishName || "").slice(0, 80);
    const pl0 = safeText(e.periodLabelZh || "").trim() || "第一时期";
    const slotParts = [`slotIndex=0（根档案 · ${pl0}）`];
    const periods = Array.isArray(e.periods) ? e.periods.slice(0, 2) : [];
    periods.forEach((p, i) => {
      if (!p || typeof p !== "object") return;
      const lb = safeText(p.labelZh || "").trim() || "时期 " + (i + 1);
      slotParts.push(`slotIndex=${i + 1}（${lb}）`);
    });
    lines.push(
      `- 「${name}」${en ? " / " + en : ""}：可选时期 — ${slotParts.join("；")}`
    );
  }
  return lines.join("\n");
}

function characterLockLinesForPublishedChapter(body) {
  const version = safeText(body.version || "");
  const lang = safeText(body.lang || "");
  const bookId = extractBookIdFromChapterBody(body);
  const chapterNum = parseInt(String(body.chapter ?? ""), 10);
  if (!version || !lang || !bookId || !Number.isFinite(chapterNum)) {
    return [];
  }
  const published = readPublishedContent({
    versionId: version,
    lang,
    bookId,
    chapter: chapterNum,
  });
  if (!published) return [];
  const payload = buildChapterPayloadFromPublished(published, {
    versionId: version,
    lang,
    bookId,
    chapter: chapterNum,
  }, {
    globalKeyPeople: loadChapterKeyPeopleGlobal(bookId, chapterNum),
  });
  return buildCharacterLockLines(
    payload.keyPeople,
    loadCharacterIllustrationProfiles(),
    6
  );
}

/** 插画管理页勾选人物库时优先用勾选时期；否则按已发布章节 keyPeople 自动推断 */
function resolveCharacterLockLinesForGeneratePrompt(body) {
  const picks = normalizeCharacterRefSelectionsFromBody(body);
  if (picks.length) {
    return buildCharacterLockLinesForRefSelections(
      picks,
      loadCharacterIllustrationProfiles(),
      6
    );
  }
  return characterLockLinesForPublishedChapter(body);
}

function ensureChapterIllustrationStateFile() {
  ensureDir(path.dirname(CHAPTER_ILLUSTRATION_STATE_FILE));
  if (!fs.existsSync(CHAPTER_ILLUSTRATION_STATE_FILE)) {
    writeJson(CHAPTER_ILLUSTRATION_STATE_FILE, {
      chapters: {},
      globalSettings: { overlayOpacity: 85 },
    });
  }
}

function extractBookIdFromChapterBody(body) {
  const direct = safeText(body?.bookId || "");
  if (direct) return direct;
  const book = safeText(body?.book || "");
  const m = /\(([A-Za-z0-9_]+)\)\s*$/.exec(book);
  return m ? m[1] : "";
}

function loadChapterIllustrationStateFromDisk(versionId, lang, bookId, chapter) {
  ensureChapterIllustrationStateFile();
  const key = stateStorageKey({ versionId, lang, bookId, chapter });
  const data = readJson(CHAPTER_ILLUSTRATION_STATE_FILE, { chapters: {} });
  return (data.chapters && data.chapters[key]) || null;
}

function saveChapterIllustrationStateToDisk(state) {
  ensureChapterIllustrationStateFile();
  const key = stateStorageKey({
    versionId: state.versionId,
    lang: state.lang,
    bookId: state.bookId,
    chapter: state.chapterNumber,
  });
  const data = readJson(CHAPTER_ILLUSTRATION_STATE_FILE, { chapters: {} });
  data.chapters = data.chapters || {};
  data.chapters[key] = {
    ...state,
    updatedAt: nowIso(),
  };
  writeJson(CHAPTER_ILLUSTRATION_STATE_FILE, data);
  return key;
}

function normalizeChapterIllustrationGlobalSettings(input) {
  const src = input && typeof input === "object" ? input : {};
  return {
    overlayOpacity: clampChapterPromptOverlayOpacity(
      src.overlayOpacity != null ? src.overlayOpacity : 85
    ),
  };
}

function loadChapterIllustrationGlobalSettingsFromDisk() {
  ensureChapterIllustrationStateFile();
  const data = readJson(CHAPTER_ILLUSTRATION_STATE_FILE, {
    chapters: {},
    globalSettings: {},
  });
  return normalizeChapterIllustrationGlobalSettings(data.globalSettings || {});
}

function saveChapterIllustrationGlobalSettingsToDisk(globalSettings) {
  ensureChapterIllustrationStateFile();
  const data = readJson(CHAPTER_ILLUSTRATION_STATE_FILE, {
    chapters: {},
    globalSettings: {},
  });
  data.globalSettings = normalizeChapterIllustrationGlobalSettings(globalSettings);
  writeJson(CHAPTER_ILLUSTRATION_STATE_FILE, data);
  return data.globalSettings;
}

function clampChapterPromptOverlayOpacity(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 100;
  return Math.max(0, Math.min(100, Math.round(x)));
}

/**
 * 从一段文本切分、去重，得到简短关键词（仅写入 illustrationSpec.elements，供内部分析；不原样进入 buildPrompt）。
 */
function extractKeywordsFromSceneText(text) {
  const raw = String(text || "").trim();
  if (!raw) return [];
  const seen = new Set();
  const out = [];
  const push = (token) => {
    const t = String(token || "").trim();
    if (t.length < 2) return;
    const key = t.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push(t);
  };
  raw.split(/[\s,.;:，。；、!?？！\-_/|"'「」『』]+/).forEach((p) => push(p));
  const latin = raw.match(/[a-zA-Z][a-zA-Z\-]{1,}/g);
  if (latin) latin.forEach((w) => push(w));
  if (out.length === 0 && raw.length >= 2 && raw.length <= 80) push(raw);
  return out.slice(0, 16);
}

/**
 * 结构化中间层：由请求体 + 默认值组装，再交给 buildPrompt(spec) 生成稳定英文 prompt。
 */
function mergeSpecKeywordLists(lists) {
  const seen = new Set();
  const out = [];
  for (const list of lists) {
    if (!Array.isArray(list)) continue;
    for (const t of list) {
      const s = String(t || "").trim();
      if (s.length < 2) continue;
      const k = s.toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(s);
      if (out.length >= 24) return out;
    }
  }
  return out;
}

/** theme 可为字符串（现有发布 JSON）或 { core, resolution } */
function normalizeThemeParts(themeInput) {
  if (
    themeInput &&
    typeof themeInput === "object" &&
    !Array.isArray(themeInput)
  ) {
    return {
      core: safeText(themeInput.core ?? themeInput.summary ?? ""),
      resolution: safeText(
        themeInput.resolution ?? themeInput.resolve ?? themeInput.turn ?? ""
      ),
    };
  }
  const s = safeText(
    typeof themeInput === "string" ? themeInput : String(themeInput || "")
  );
  return { core: s, resolution: "" };
}

function themeHasUsableContent(themeInput) {
  const { core, resolution } = normalizeThemeParts(themeInput);
  return Boolean(safeText(core) || safeText(resolution));
}

function themeToFlatString(themeInput) {
  const { core, resolution } = normalizeThemeParts(themeInput);
  const parts = [safeText(resolution), safeText(core)].filter(Boolean);
  return parts.join("；");
}

function buildIllustrationSpec(body) {
  const scene = safeText(body?.scene || "");
  const theme = themeToFlatString(body?.theme);
  const editorNotes = safeText(body?.editorNotes || "");
  const transparent = body?.transparentBackground === true;
  const compositionMode = safeText(body?.compositionMode || body?.mode || "");
  return {
    book: safeText(body?.book || ""),
    bookId: safeText(body?.bookId || extractBookIdFromChapterBody(body)),
    chapter: safeText(body?.chapter || ""),
    theme,
    scene,
    editorNotes,
    compositionMode,
    composition: safeText(body?.composition || "") || "single focal point",
    mood: safeText(body?.mood || "") || "calm, spacious, peaceful",
    elements: mergeSpecKeywordLists([
      extractKeywordsFromSceneText(theme),
      extractKeywordsFromSceneText(scene),
    ]),
    style: "classical biblical candlelit oil painting",
    stylePreset:
      safeText(body?.stylePreset || "") || "biblical_candlelit_oil_painting",
    transparent,
    overlayOpacity: clampChapterPromptOverlayOpacity(
      body?.overlayOpacity != null ? body.overlayOpacity : 100
    ),
  };
}

function likelyContainsCjk(text) {
  return /[\u3400-\u9FFF]/.test(String(text || ""));
}

async function translateIllustrationEditorNotesToEnglish(editorNotesZh) {
  const raw = safeText(editorNotesZh || "");
  if (!raw) return "";
  if (!likelyContainsCjk(raw)) return raw;
  try {
    const system =
      "You translate Chinese illustration direction into concise natural English for image-generation prompts. Keep concrete art-direction details only. Output English only.";
    const user =
      "Translate the following Chinese notes into concise, concrete English prompt direction. Preserve intent and constraints.\n\n" +
      raw;
    const out = await openAiChatHelper({
      system,
      messages: [{ role: "user", content: user }],
    });
    const en = safeText(out || "");
    return en || raw;
  } catch (_) {
    return raw;
  }
}

/**
 * 由 illustrationSpec 生成最终英文出图 prompt（模块 chapter-illustration / prompt-generator）。
 */
function buildPrompt(spec) {
  const lines = Array.isArray(spec?.characterAppearanceLines)
    ? spec.characterAppearanceLines
    : [];
  const notesEn = safeText(spec?.editorNotesEn || "");
  const sceneBase = safeText(spec?.scene || "");
  const sceneMerged = notesEn
    ? sceneBase +
      (sceneBase ? "\n" : "") +
      "Additional editor direction (must follow): " +
      notesEn
    : sceneBase;
  return generateIllustrationPrompt({
    sceneDescription: sceneMerged,
    transparentBackground: spec?.transparent === true,
    compositionMode: safeText(spec?.compositionMode || ""),
    composition: spec?.composition,
    stylePreset:
      safeText(spec?.stylePreset || "") || "biblical_candlelit_oil_painting",
    characterAppearanceLines: lines,
  });
}

function tryAutoSceneFromPublishedChapter(body) {
  const version = safeText(body.version || "");
  const lang = safeText(body.lang || "");
  const bookId = extractBookIdFromChapterBody(body);
  const chapterNum = parseInt(String(body.chapter ?? ""), 10);
  const sceneVariant = Math.max(0, Number(body.sceneVariant || 0) || 0);
  if (!version || !lang || !bookId || !Number.isFinite(chapterNum)) {
    return null;
  }
  const published = readPublishedContent({
    versionId: version,
    lang,
    bookId,
    chapter: chapterNum,
  });
  if (!published) return null;
  const run = runScenePipelineFromPublishedData(
    published,
    { versionId: version, lang, bookId, chapter: chapterNum },
    {
      alternateIndex: sceneVariant,
      profilesRoot: loadCharacterIllustrationProfiles(),
      globalKeyPeople: loadChapterKeyPeopleGlobal(bookId, chapterNum),
    }
  );
  return {
    scene: run.sceneDescription,
    sceneDescriptionZh: run.sceneDescriptionZh,
    pipeline: {
      confidence: run.confidence,
      warning: run.warning,
      warningZh: run.warningZh,
      chapterType: run.analysis.chapterType,
      chapterTypeZh: run.chapterTypeZh,
      sceneDescriptionZh: run.sceneDescriptionZh,
      analysis: run.analysis,
      selection: run.selection,
      sceneVariant,
    },
  };
}

/**
 * POST /api/chapter-illustration/analyze
 * Body: version, lang, bookId, chapter（与已发布章节一致）
 */
app.post("/api/chapter-illustration/analyze", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const version = safeText(body.version || "");
    const lang = safeText(body.lang || "");
    const bookId = safeText(body.bookId || extractBookIdFromChapterBody(body));
    const chapterNum = parseInt(String(body.chapter ?? ""), 10);
    if (!version || !lang || !bookId || !Number.isFinite(chapterNum)) {
      return res.status(400).json({
        error: "缺少 version、lang、bookId 或 chapter。",
      });
    }
    const published = readPublishedContent({
      versionId: version,
      lang,
      bookId,
      chapter: chapterNum,
    });
    if (!published) {
      return res.status(404).json({
        error: "未找到已发布章节，无法分析。",
      });
    }
    const payload = buildChapterPayloadFromPublished(published, {
      versionId: version,
      lang,
      bookId,
      chapter: chapterNum,
    }, {
      globalKeyPeople: loadChapterKeyPeopleGlobal(bookId, chapterNum),
    });
    const analysis = analyzeChapterForIllustration(payload);
    res.json({ ok: true, analysis, payloadSummary: payload.summary });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "无法分析本章插画要点，请稍后重试。",
    });
  }
});

/**
 * POST /api/chapter-illustration/scene
 * Body: version, lang, bookId, chapter, sceneVariant（可选，换一景）
 */
app.post("/api/chapter-illustration/scene", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const version = safeText(body.version || "");
    const lang = safeText(body.lang || "");
    const bookId = extractBookIdFromChapterBody(body);
    const chapterNum = parseInt(String(body.chapter ?? ""), 10);
    if (!version || !lang || !bookId || !Number.isFinite(chapterNum)) {
      return res.status(400).json({
        error: "缺少 version、lang、bookId 或 chapter。",
      });
    }
    const published = readPublishedContent({
      versionId: version,
      lang,
      bookId,
      chapter: chapterNum,
    });
    if (!published) {
      return res.status(404).json({
        error: "未找到已发布章节，无法生成场景。",
      });
    }
    const run = runScenePipelineFromPublishedData(
      published,
      { versionId: version, lang, bookId, chapter: chapterNum },
      {
        alternateIndex: Math.max(0, Number(body.sceneVariant || 0) || 0),
        profilesRoot: loadCharacterIllustrationProfiles(),
        globalKeyPeople: loadChapterKeyPeopleGlobal(bookId, chapterNum),
      }
    );
    const chapterState = stateFromPipelineRun(
      body,
      published,
      run,
      {
        sceneVariant: Math.max(0, Number(body.sceneVariant || 0) || 0),
      },
      loadCharacterIllustrationProfiles()
    );
    saveChapterIllustrationStateToDisk(chapterState);
    res.json({
      ok: true,
      sceneDescription: run.sceneDescription,
      sceneDescriptionZh: run.sceneDescriptionZh,
      confidence: run.confidence,
      warning: run.warning,
      warningZh: run.warningZh,
      chapterType: run.analysis.chapterType,
      chapterTypeZh: run.chapterTypeZh,
      analysis: run.analysis,
      selection: run.selection,
      chapterState,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "自动生成场景失败，请稍后重试或手写英文场景。",
    });
  }
});

/**
 * POST /api/chapter-illustration/state
 * GET query: version, lang, bookId, chapter
 */
app.get("/api/chapter-illustration/state", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const q = req.query || {};
    const version = safeText(q.version || "");
    const lang = safeText(q.lang || "");
    const bookId = safeText(q.bookId || "");
    const chapter = q.chapter;
    if (!version || !lang || !bookId || chapter === undefined) {
      return res.status(400).json({ error: "缺少查询参数。" });
    }
    const st = loadChapterIllustrationStateFromDisk(version, lang, bookId, chapter);
    res.json({ ok: true, chapterState: st });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "读取保存状态失败。" });
  }
});

app.post("/api/chapter-illustration/state", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const st = body.chapterState;
    if (!st || typeof st !== "object") {
      return res.status(400).json({ error: "缺少 chapterState。" });
    }
    const merged = mergeChapterIllustrationState(
      defaultChapterIllustrationState(),
      st
    );
    saveChapterIllustrationStateToDisk(merged);
    res.json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "保存状态失败。" });
  }
});

app.get("/api/chapter-illustration/global-settings", (_req, res) => {
  try {
    const globalSettings = loadChapterIllustrationGlobalSettingsFromDisk();
    res.json({ ok: true, globalSettings });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "读取插画全局设置失败。" });
  }
});

app.post("/api/chapter-illustration/global-settings", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const raw =
      body && typeof body.globalSettings === "object"
        ? body.globalSettings
        : body;
    const saved = saveChapterIllustrationGlobalSettingsToDisk(raw);
    res.json({ ok: true, globalSettings: saved });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "保存插画全局设置失败。" });
  }
});

function normalizeBcdStatureClass(raw) {
  const s = safeText(raw || "").toLowerCase();
  if (s === "child" || s === "youth" || s === "adult" || s === "elder") return s;
  return "";
}

function handleCharacterIllustrationProfilesGet(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    res.json({ ok: true, profiles: loadCharacterIllustrationProfiles() });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "读取角色档案失败。" });
  }
}

async function handleCharacterIllustrationProfilesPost(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const profiles = body.profiles;
    if (!profiles || typeof profiles !== "object") {
      return res.status(400).json({ error: "缺少 profiles 对象。" });
    }
    const characters = profiles.characters;
    if (!characters || typeof characters !== "object" || Array.isArray(characters)) {
      return res.status(400).json({ error: "profiles 须包含 characters 对象（键为中文名）。" });
    }
    const existingRoot = loadCharacterIllustrationProfiles();
    const existingChars =
      existingRoot.characters && typeof existingRoot.characters === "object"
        ? existingRoot.characters
        : {};
    const relatedBookIdsByProfile = buildRelatedBookIdsByProfile(BIBLE_PRIMARY_CHARACTERS_BY_BOOK);
    const out = { characters: {} };
    for (const [zh, entry] of Object.entries(characters)) {
      const key = safeText(zh).slice(0, 32);
      if (!key) continue;
      if (!entry || typeof entry !== "object") continue;
      const prev = existingChars[key] && typeof existingChars[key] === "object" ? existingChars[key] : {};
      const resolvedEnglishName = resolveCharacterEnglishName(
        key,
        safeText(entry.englishName || prev.englishName || "")
      ).slice(0, 80);
      const resolvedSourceBookId = resolveCharacterSourceBookId(
        key,
        safeText(entry.sourceBookId || prev.sourceBookId || "")
      );
      const row = {
        englishName: resolvedEnglishName,
        shortSceneTagEn: safeText(entry.shortSceneTagEn || "").slice(0, 160),
        appearanceEn: safeText(entry.appearanceEn || "").slice(0, 1200),
      };
      const identity = resolveCharacterIdentity(
        resolvedSourceBookId,
        safeText(entry.displayNameZh || key || "")
      );
      row.displayNameZh = safeText(
        entry.displayNameZh || prev.displayNameZh || identity.displayNameZh || key
      ).slice(0, 32);
      if (resolvedSourceBookId) row.sourceBookId = resolvedSourceBookId;
      const sentBookIds = Array.isArray(entry.bookIds) ? entry.bookIds : [];
      const prevBookIds = Array.isArray(prev.bookIds) ? prev.bookIds : [];
      const impliedBookIds = Array.isArray(relatedBookIdsByProfile[key])
        ? relatedBookIdsByProfile[key]
        : [];
      const mergedBookIds = [...new Set([...prevBookIds, ...sentBookIds, ...impliedBookIds, resolvedSourceBookId])]
        .map((x) => safeText(x).trim().toUpperCase())
        .filter(Boolean)
        .slice(0, 12);
      if (mergedBookIds.length) row.bookIds = mergedBookIds;
      row.characterRoleZh = resolveCharacterRoleZh(
        row.displayNameZh || key,
        entry.characterRoleZh || prev.characterRoleZh || "",
        resolvedSourceBookId
      );
      const spZh = safeText(entry.scripturePersonalityZh || "").slice(0, 500);
      if (spZh) row.scripturePersonalityZh = spZh;
      const spEn = safeText(entry.scripturePersonalityEn || "").slice(0, 600);
      if (spEn) row.scripturePersonalityEn = spEn;
      const lifespanZh = safeText(entry.lifespanZh || "").slice(0, 80);
      row.lifespanZh = (lifespanZh || resolveCharacterLifespanZh(key, row.englishName || "")).slice(
        0,
        80
      );
      const eraLabelZh = safeText(entry.eraLabelZh || "").slice(0, 80);
      if (eraLabelZh) row.eraLabelZh = eraLabelZh;
      const identityTagsZh = safeText(entry.identityTagsZh || "").slice(0, 240);
      if (identityTagsZh) row.identityTagsZh = identityTagsZh;
      const plz = safeText(entry.periodLabelZh || "").slice(0, 32);
      if (plz) row.periodLabelZh = plz;
      const img0 = safeText(entry.imageUrl || "").slice(0, 400);
      if (img0) row.imageUrl = img0;
      const st0 = normalizeBcdStatureClass(entry.statureClass);
      if (st0) row.statureClass = st0;
      const sentComparisonSheet = Object.prototype.hasOwnProperty.call(
        entry,
        "comparisonSheetUrl"
      );
      if (sentComparisonSheet) {
        const cmp = safeText(String(entry.comparisonSheetUrl ?? "")).slice(0, 400);
        if (cmp) row.comparisonSheetUrl = cmp;
      } else {
        const cmp = safeText(entry.comparisonSheetUrl || "").slice(0, 400);
        if (cmp) row.comparisonSheetUrl = cmp;
      }
      const hero = safeText(entry.heroImageUrl || "").slice(0, 400);
      if (hero) row.heroImageUrl = hero;
      const identityRef = safeText(entry.identityReferenceImageUrl || "").slice(0, 400);
      if (identityRef) row.identityReferenceImageUrl = identityRef;
      const identityCoreEn = safeText(entry.identityCoreEn || "").slice(0, 1200);
      if (identityCoreEn) row.identityCoreEn = identityCoreEn;
      let rosterH = Number(entry.heroRosterHeight);
      if (!Number.isFinite(rosterH) || rosterH <= 0) {
        const p = Number(prev.heroRosterHeight);
        rosterH = Number.isFinite(p) && p > 0 ? p : 1;
      }
      row.heroRosterHeight = Math.min(2.5, Math.max(0.35, rosterH));
      const smartGenNotes = safeText(entry.smartGenNotes || "").slice(0, 400);
      if (smartGenNotes) row.smartGenNotes = smartGenNotes;
      const promptArchiveText = safeText(entry.promptArchiveText || "").slice(0, 200000);
      if (promptArchiveText) row.promptArchiveText = promptArchiveText;
      const rawPeriods = Array.isArray(entry.periods) ? entry.periods : [];
      const periods = [];
      for (let i = 0; i < rawPeriods.length && periods.length < 2; i++) {
        const p = rawPeriods[i];
        if (!p || typeof p !== "object") continue;
        const slot = {
          labelZh: safeText(p.labelZh || "").slice(0, 32),
          shortSceneTagEn: safeText(p.shortSceneTagEn || "").slice(0, 160),
          appearanceEn: safeText(p.appearanceEn || "").slice(0, 1200),
        };
        const appearanceDeltaEn = safeText(p.appearanceDeltaEn || "").slice(0, 1200);
        if (appearanceDeltaEn) slot.appearanceDeltaEn = appearanceDeltaEn;
        const img = safeText(p.imageUrl || "").slice(0, 400);
        if (img) slot.imageUrl = img;
        const sourceRef = safeText(p.sourceReferenceImageUrl || "").slice(0, 400);
        if (sourceRef) slot.sourceReferenceImageUrl = sourceRef;
        const derivedFromStageId = safeText(p.derivedFromStageId || "").slice(0, 64);
        if (derivedFromStageId) slot.derivedFromStageId = derivedFromStageId;
        const stp = normalizeBcdStatureClass(p.statureClass);
        if (stp) slot.statureClass = stp;
        periods.push(slot);
      }
      if (periods.length) row.periods = periods;
      if (!row.heroImageUrl && prev.heroImageUrl) {
        row.heroImageUrl = safeText(prev.heroImageUrl).slice(0, 400);
      }
      if (!row.identityReferenceImageUrl && prev.identityReferenceImageUrl) {
        row.identityReferenceImageUrl = safeText(prev.identityReferenceImageUrl).slice(0, 400);
      }
      if (!row.identityCoreEn && prev.identityCoreEn) {
        row.identityCoreEn = safeText(prev.identityCoreEn).slice(0, 1200);
      }
      if (
        !sentComparisonSheet &&
        !row.comparisonSheetUrl &&
        prev.comparisonSheetUrl
      ) {
        row.comparisonSheetUrl = safeText(prev.comparisonSheetUrl).slice(0, 400);
      }
      if (!row.imageUrl && prev.imageUrl) {
        row.imageUrl = safeText(prev.imageUrl).slice(0, 400);
      }
      if (Array.isArray(row.periods) && Array.isArray(prev.periods)) {
        for (let pi = 0; pi < row.periods.length; pi++) {
          const rp = row.periods[pi];
          const pp = prev.periods[pi];
          if (!rp || !pp || typeof rp !== "object" || typeof pp !== "object") continue;
          if (!rp.imageUrl && pp.imageUrl) {
            rp.imageUrl = safeText(pp.imageUrl).slice(0, 400);
          }
          if (!rp.sourceReferenceImageUrl && pp.sourceReferenceImageUrl) {
            rp.sourceReferenceImageUrl = safeText(pp.sourceReferenceImageUrl).slice(0, 400);
          }
          if (!rp.derivedFromStageId && pp.derivedFromStageId) {
            rp.derivedFromStageId = safeText(pp.derivedFromStageId).slice(0, 64);
          }
          if (!rp.appearanceDeltaEn && pp.appearanceDeltaEn) {
            rp.appearanceDeltaEn = safeText(pp.appearanceDeltaEn).slice(0, 1200);
          }
        }
      }
      out.characters[key] = row;
    }
    if (CHARACTER_DATA_DIR) ensureDir(CHARACTER_DATA_DIR);
    persistCharacterIllustrationProfilesRoot(out);
    rememberCharacterProfileImageAuditFromProfiles(out);
    let thumbBuild = { built: [], failed: [] };
    try {
      thumbBuild = await ensureGeneratedRosterThumbsForProfiles(
        out,
        READER_IMAGE_MAX_EDGE
      );
    } catch (thumbErr) {
      console.warn("[character-profiles:auto-thumbs]", thumbErr?.message || thumbErr);
    }
    clearReadCacheByPrefix(`${STUDY_CONTENT_CACHE_TAG}:`);
    appendAdminAudit(req, authed, "bible_character_profiles_save", {
      characterCount: Object.keys(out.characters || {}).length,
      heroCount: Object.values(out.characters || {}).filter((x) => safeText(x?.heroImageUrl || "")).length,
    });
    res.json({
      ok: true,
      profiles: out,
      rosterThumbsBuilt: thumbBuild.built.length,
      rosterThumbsFailed: thumbBuild.failed.length,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "保存角色档案失败。" });
  }
}

app.get("/api/admin/character-illustration-profiles", handleCharacterIllustrationProfilesGet);
app.post("/api/admin/character-illustration-profiles", handleCharacterIllustrationProfilesPost);
/** 扁平别名：个别反代对长连字符 path 返回 404（与 /api/admin/sitechrome 同理） */
app.get("/api/admin/bible-character-profiles", handleCharacterIllustrationProfilesGet);
app.post("/api/admin/bible-character-profiles", handleCharacterIllustrationProfilesPost);
app.post(
  "/api/admin/character-illustration-profiles/generate",
  handleCharacterProfileGenerate
);
app.post("/api/admin/bible-character-profiles/generate", handleCharacterProfileGenerate);
app.post("/api/admin/bible-character-profiles/gentext", handleCharacterProfileGenerate);
app.post(
  "/api/admin/character-illustration-profiles/gentext",
  handleCharacterProfileGenerate
);
app.post(
  "/api/admin/character-illustration-profiles/generate-life-stages",
  handleCharacterProfileGenerateLifeStages
);
app.post(
  "/api/admin/bible-character-profiles/generate-life-stages",
  handleCharacterProfileGenerateLifeStages
);
/** 短路径别名：个别反代对长 path 段返回 404，与 sitechrome 同理 */
app.post(
  "/api/admin/bible-character-profiles/genlifestages",
  handleCharacterProfileGenerateLifeStages
);
app.post(
  "/api/admin/character-illustration-profiles/genlifestages",
  handleCharacterProfileGenerateLifeStages
);
app.post(
  "/api/admin/bible-character-profiles/prompt-archive-translate",
  handleCharacterProfilePromptArchiveTranslate
);
app.post(
  "/api/admin/character-illustration-profiles/prompt-archive-translate",
  handleCharacterProfilePromptArchiveTranslate
);

function sanitizeChapterKeyPeopleFileBody(body) {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  const cleaned = {};
  for (const [bk, chObj] of Object.entries(body)) {
    const bookId = safeText(String(bk || "")).slice(0, 24);
    if (!bookId) continue;
    if (!chObj || typeof chObj !== "object" || Array.isArray(chObj)) continue;
    const byCh = {};
    for (const [ck, arr] of Object.entries(chObj)) {
      const ch = String(ck || "").trim();
      if (!/^\d+$/.test(ch)) continue;
      const n = Number(ch);
      if (!Number.isFinite(n) || n < 1) continue;
      const names = sanitizeChapterKeyPeopleArray(Array.isArray(arr) ? arr : []);
      if (names.length) byCh[String(n)] = names;
    }
    if (Object.keys(byCh).length > 0) cleaned[bookId] = byCh;
  }
  return cleaned;
}

function listZhNamesWithRosterPortraitInProfiles() {
  const profilesRoot = loadCharacterIllustrationProfiles();
  const ch =
    profilesRoot.characters && typeof profilesRoot.characters === "object"
      ? profilesRoot.characters
      : {};
  const out = [];
  for (const [zh, entry] of Object.entries(ch)) {
    if (!entry || typeof entry !== "object") continue;
    const resolved = resolveChapterRosterPortrait(entry, undefined);
    const url = normalizeIllustrationImageUrlForPublication(resolved.url);
    if (url) out.push(String(zh || "").trim());
  }
  const uniq = [...new Set(out.filter(Boolean))];
  uniq.sort((a, b) => a.localeCompare(b, "zh-Hans-CN"));
  return uniq;
}

function mergeChapterKeyPeopleDeep(base, add) {
  const out =
    base && typeof base === "object" && !Array.isArray(base) ? { ...base } : {};
  if (!add || typeof add !== "object" || Array.isArray(add)) return out;
  for (const [bid, chObj] of Object.entries(add)) {
    if (!chObj || typeof chObj !== "object" || Array.isArray(chObj)) continue;
    const prevBook =
      out[bid] && typeof out[bid] === "object" && !Array.isArray(out[bid])
        ? { ...out[bid] }
        : {};
    for (const [ch, arr] of Object.entries(chObj)) {
      const a = Array.isArray(arr)
        ? arr.map((x) => String(x || "").trim()).filter(Boolean)
        : [];
      const p = Array.isArray(prevBook[ch])
        ? prevBook[ch].map((x) => String(x || "").trim()).filter(Boolean)
        : [];
      const merged = [...new Set([...p, ...a])];
      if (merged.length) prevBook[ch] = merged;
    }
    if (Object.keys(prevBook).length) out[bid] = prevBook;
  }
  return out;
}

/** 仅据 theme / 段标题正则推断 keyPeople，再与「人物库已有立绘」名单求交，供全局表自动建议 */
function buildChapterKeyPeopleSuggestionsFromPublished(versionId, lang, rosterZhSet) {
  const vid = safeText(String(versionId || "")).slice(0, 48);
  const lng = safeText(String(lang || "")).slice(0, 24);
  if (!vid || !lng) return {};
  const books = flattenBooks();
  const suggested = {};
  for (const book of books) {
    const bid = book.bookId;
    let cov;
    try {
      cov = listPublishedBookChapters(vid, lng, bid);
    } catch {
      continue;
    }
    for (const chNum of cov.publishedChapters) {
      const data = readPublishedContent({
        versionId: vid,
        lang: lng,
        bookId: bid,
        chapter: chNum,
      });
      if (!data || typeof data !== "object") continue;
      const payload = buildChapterPayloadFromPublished(
        data,
        {
          versionId: vid,
          lang: lng,
          bookId: bid,
          chapter: chNum,
        },
        {
          globalKeyPeople: [],
          inferredKeyPeopleOnly: true,
        }
      );
      const names = (
        Array.isArray(payload.keyPeople) ? payload.keyPeople : []
      ).filter((n) => rosterZhSet.has(String(n || "").trim()));
      if (!names.length) continue;
      const uniq = [
        ...new Set(names.map((n) => String(n || "").trim()).filter(Boolean)),
      ];
      if (!suggested[bid]) suggested[bid] = {};
      suggested[bid][String(chNum)] = uniq;
    }
  }
  return suggested;
}

function handleChapterKeyPeopleRosterNames(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const names = listZhNamesWithRosterPortraitInProfiles();
    res.set("Cache-Control", "no-store");
    res.json({ names });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取失败" });
  }
}

function handleChapterKeyPeopleAutoMerge(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const body = req.body || {};
    const versionId =
      safeText(String(body.versionId || "default")).slice(0, 48) || "default";
    const lang = safeText(String(body.lang || "zh")).slice(0, 24) || "zh";

    const rosterNames = listZhNamesWithRosterPortraitInProfiles();
    const rosterZhSet = new Set(rosterNames);
    const suggestions = buildChapterKeyPeopleSuggestionsFromPublished(
      versionId,
      lang,
      rosterZhSet
    );

    ensureChapterKeyPeopleFile();
    const existing = readJson(CHAPTER_KEY_PEOPLE_FILE, {}) || {};
    const merged = mergeChapterKeyPeopleDeep(existing, suggestions);
    const cleaned = sanitizeChapterKeyPeopleFileBody(merged);
    if (cleaned === null) {
      return res.status(500).json({ error: "合并后校验失败" });
    }

    writeJson(CHAPTER_KEY_PEOPLE_FILE, cleaned);
    clearReadCacheByPrefix(`${STUDY_CONTENT_CACHE_TAG}:`);

    let suggestionChapters = 0;
    for (const chObj of Object.values(suggestions)) {
      if (chObj && typeof chObj === "object") {
        suggestionChapters += Object.keys(chObj).length;
      }
    }

    appendAdminAudit(req, authed, "chapter_key_people_auto_merge", {
      versionId,
      lang,
      rosterPortraitCount: rosterNames.length,
      suggestionChapters,
    });

    res.json({
      ok: true,
      rosterPortraitCount: rosterNames.length,
      suggestionChapters,
      versionId,
      lang,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "合并失败" });
  }
}

app.get("/api/admin/chapter-key-people/roster-names", handleChapterKeyPeopleRosterNames);
/** 短路径别名：个别反代对长 path 段返回 404，与 genlifestages 同理 */
app.get("/api/admin/ckp-roster", handleChapterKeyPeopleRosterNames);

app.post(
  "/api/admin/chapter-key-people/auto-merge-from-published",
  handleChapterKeyPeopleAutoMerge
);
app.post("/api/admin/ckp-automerge", handleChapterKeyPeopleAutoMerge);

app.get("/api/admin/chapter-key-people", (req, res) => {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    ensureChapterKeyPeopleFile();
    res.set("Cache-Control", "no-store");
    res.json(readJson(CHAPTER_KEY_PEOPLE_FILE, {}));
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取失败" });
  }
});

app.post("/api/admin/chapter-key-people", (req, res) => {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const cleaned = sanitizeChapterKeyPeopleFileBody(req.body);
    if (cleaned === null) {
      return res.status(400).json({
        error: "需要 JSON 对象：书卷 id → 章号字符串 → 中文名数组",
      });
    }
    ensureChapterKeyPeopleFile();
    writeJson(CHAPTER_KEY_PEOPLE_FILE, cleaned);
    clearReadCacheByPrefix(`${STUDY_CONTENT_CACHE_TAG}:`);
    appendAdminAudit(req, authed, "chapter_key_people_save", {
      bookCount: Object.keys(cleaned).length,
    });
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存失败" });
  }
});

/**
 * POST /api/generate-prompt
 * Body: theme 必填；scene 可空（空则据已发布 JSON 自动推断场景）；
 * version, lang, bookId, chapter 与 scene 同时用于自动场景；sceneVariant 换候选景。
 */
app.post("/api/generate-prompt", async (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    if (!themeHasUsableContent(body.theme)) {
      return res.status(400).json({
        error:
          "请先完善 theme（本章 JSON 需提供 theme 正文，或 theme.core / theme.resolution）",
      });
    }
    let scene = safeText(body.scene || "");
    let pipelineMeta = null;
    if (!scene) {
      const auto = tryAutoSceneFromPublishedChapter(body);
      if (auto && safeText(auto.scene)) {
        scene = auto.scene;
        pipelineMeta = auto.pipeline;
      }
    }
    if (!scene) {
      return res.status(400).json({
        error:
          "未能自动生成场景。请填写 Scene description，或确认已选版本/语言/书卷并已发布该章。",
      });
    }
    const bodyResolved = { ...body, scene };
    const illustrationSpec = buildIllustrationSpec(bodyResolved);
    illustrationSpec.editorNotesEn = await translateIllustrationEditorNotesToEnglish(
      illustrationSpec.editorNotes || ""
    );
    illustrationSpec.characterAppearanceLines =
      resolveCharacterLockLinesForGeneratePrompt(bodyResolved);
    const prompt = buildPrompt(illustrationSpec);
    console.log("=== FINAL PROMPT ===");
    console.log(prompt);
    const imageUrl = safeText(body.imageUrl || "");
    const transparentPng = safeText(body.transparentPng || "");
    ensureChapterPromptLogFile();
    const data = readJson(CHAPTER_PROMPT_LOG_FILE, { entries: [] });
    const entries = Array.isArray(data.entries) ? data.entries : [];
    const id =
      "cp_" + Date.now() + "_" + Math.random().toString(36).slice(2, 10);
    entries.unshift({
      id,
      createdAt: nowIso(),
      illustrationSpec,
      prompt,
      imageUrl,
      transparentPng,
      book: illustrationSpec.book,
      chapter: illustrationSpec.chapter,
      theme: illustrationSpec.theme,
      scene: illustrationSpec.scene,
      sceneAutoGenerated: Boolean(pipelineMeta),
      pipeline: pipelineMeta || undefined,
      transparentBackground: illustrationSpec.transparent,
      overlayOpacity: illustrationSpec.overlayOpacity,
    });
    writeJson(CHAPTER_PROMPT_LOG_FILE, { entries });
    res.json({
      ok: true,
      prompt,
      illustrationSpec,
      id,
      sceneDescription: scene,
      sceneDescriptionZh: safeText(pipelineMeta?.sceneDescriptionZh || ""),
      sceneAutoGenerated: Boolean(pipelineMeta),
      pipeline: pipelineMeta || null,
      characterAppearanceLines: illustrationSpec.characterAppearanceLines || [],
      transparentBackground: illustrationSpec.transparent,
      overlayOpacity: illustrationSpec.overlayOpacity,
      imageUrl: imageUrl || "",
      transparentPng: transparentPng || "",
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message || "生成失败" });
  }
});

/**
 * POST /api/admin/illustration-admin/gpt-copy
 * POST /api/admin/ill-adm-gptcopy（短路径别名）
 * POST /api/chapter-illustration/gpt-copy（与 scene/state 同前缀，反代/路由表易放行）
 * 根据已发布章节与编辑备注，GPT 生成插画说明与英文场景（供插画管理页）。
 */
async function handleIllustrationAdminGptCopy(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const version = safeText(body.version || "");
    const lang = safeText(body.lang || "");
    const bookId = safeText(body.bookId || "");
    const chapter = Number(body.chapter);
    const editorNotes = safeText(body.editorNotes || "");
    if (!version || !lang || !bookId || !Number.isFinite(chapter)) {
      return res
        .status(400)
        .json({ error: "缺少 version / lang / bookId 或 chapter 无效" });
    }
    const published = readPublishedContent({
      versionId: version,
      lang,
      bookId,
      chapter,
    });
    if (!published) {
      return res.status(400).json({
        error: "未找到已发布章节，请先在后台发布该章后再生成文案。",
      });
    }
    const profilesRoot = loadCharacterIllustrationProfiles();
    const themeStr = themeToFlatString(published.theme);
    const segTitles = (Array.isArray(published.segments) ? published.segments : [])
      .slice(0, 14)
      .map((s) => safeText(s?.title || ""))
      .filter(Boolean)
      .join("；");
    const bookLabel = safeText(published.bookLabel || bookId);
    const rosterBlock = buildCharacterRosterForGptPrompt(profilesRoot);
    const userBlockParts = [
      `书卷：${bookLabel}（${bookId}）`,
      `章节：${chapter === 0 ? "卷首页" : "第 " + chapter + " 章"}`,
      `主题(theme)：${themeStr || "（无）"}`,
      `段落标题摘要：${segTitles || "（无）"}`,
      editorNotes ? `编辑备注与画面要求：${editorNotes}` : "",
      `【人物设计库 — 须由你为本章插画自动选配】\n下面列出档案中已有的人物及可选时期（slotIndex）。请根据本章叙事与你要写的画面，选出所有需要在图中具名呈现、且须与库中参考脸一致的人物（0～6 人）；同一人若在本章应以更年轻或更年长样貌出现，选对应 slotIndex。纯风景、无具名人物、或档案中无此人时，characterRefSelections 输出空数组 []。\n${rosterBlock}`,
    ];
    const userBlock = userBlockParts.filter(Boolean).join("\n");

    const system = `你是圣经读物插画策划编辑。根据已发布章节的主题与段落标题，为一幅「半写实、带完整环境背景的实底插画」（非透明抠图）写说明与出图用词；画面需有地面/天色/空间氛围等，尽量避免整幅纯空白底，除非叙事需要表现光本身；并**自动**从给定「人物设计库」名单中选配本章出镜人物的锁脸参考（可多选）。
只输出一个 JSON 对象（不要 Markdown 代码围栏），键如下：
- illustrationBriefZh：string，2～4 句中文，概括本图画什么、情绪与构图重心（不罗列经文编号）。
- keywordsZh：string，中文关键词，逗号或顿号分隔，约 8～16 个（环境、人物关系、道具、光线氛围等）。
- sceneEnglish：string，一段英文，单一冻结瞬间的具象画面描写，可直接作为图像模型的场景指令；不要出现章节号或书名；人物为古朴中东服饰、端庄得体；适合 classical biblical candlelit oil painting。约 40～120 个英文单词。须写进完整环境（地面、天色、建筑或旷野、暖沙/米色尘雾等），避免主导性纯白空底；亮白仅用于表现光源/天光/神光等。
- sceneEnglishZh：string，与 sceneEnglish 对应的中文意译（供编辑对照出图含义），自然流畅，不要出现章节号或书名；篇幅与英文相当。
- characterRefSelections：array，每项为 { "zhName": string（必须与用户消息中人物库条目的中文名「」内文字完全一致）, "slotIndex": number（0=根档案第一时期，1=第二时期…，以库中说明为准）}。须与 sceneEnglish 中出现的主要人物一致；无具名人物时 []。

sceneEnglish 必须与 characterRefSelections 中每张脸的年龄阶段一致；同一人物在不同人生阶段须「同脸可辨」（仅皱纹/发色/肤质等变化）。
若备注与经文叙事冲突，以经文为主，备注仅作补充。`;

    const raw = await openAiChatHelper({
      system,
      messages: [{ role: "user", content: userBlock }],
    });
    const parsed = tryParseJsonLoose(raw);
    if (!parsed || typeof parsed !== "object") {
      return res.status(502).json({
        error: "模型未返回有效 JSON，请重试或缩短备注",
        rawPreview: String(raw || "").slice(0, 400),
      });
    }
    const characterRefSelections = sanitizeGptCharacterRefSelections(
      parsed,
      profilesRoot
    );
    res.json({
      ok: true,
      illustrationBriefZh: safeText(parsed.illustrationBriefZh || ""),
      keywordsZh: safeText(parsed.keywordsZh || ""),
      sceneEnglish: safeText(parsed.sceneEnglish || ""),
      sceneEnglishZh: safeText(parsed.sceneEnglishZh || ""),
      characterRefSelections,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "GPT 请求失败" });
  }
}

app.post("/api/chapter-illustration/gpt-copy", handleIllustrationAdminGptCopy);
app.post("/api/admin/illustration-admin/gpt-copy", handleIllustrationAdminGptCopy);
app.post("/api/admin/ill-adm-gptcopy", handleIllustrationAdminGptCopy);

/** 无需登录：供插画管理页与本机自检，确认当前 Node 进程已注册 gpt-copy（非 404） */
app.get("/api/chapter-illustration/gpt-copy-probe", (_req, res) => {
  res.json({ ok: true, illustrationAdminGptCopy: true });
});

/**
 * POST /api/admin/published/chapter-illustration
 * 将插画 URL 写入已发布章节 JSON（读经页展示）；imageUrl 空字符串则清除。
 */
app.post("/api/admin/published/chapter-illustration", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const version = safeText(body.version || "");
    const lang = safeText(body.lang || "");
    const bookId = safeText(body.bookId || "");
    const chapter = Number(body.chapter);
    const imageUrlRaw = safeText(body.imageUrl || "");
    if (!version || !lang || !bookId || !Number.isFinite(chapter)) {
      return res
        .status(400)
        .json({ error: "缺少 version / lang / bookId 或 chapter 无效" });
    }
    const filePath = getPublishedContentFilePath({
      versionId: version,
      lang,
      bookId,
      chapter,
    });
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: "未找到已发布章节文件" });
    }
    let illPayload = null;
    if (imageUrlRaw) {
      const imageUrlNorm = normalizeIllustrationImageUrlForPublication(imageUrlRaw);
      if (imageUrlNorm) {
        illPayload = { imageUrl: imageUrlNorm, updatedAt: nowIso() };
      }
    }
    const targets = listPublishedChapterFilesForBookChapter(bookId, chapter);
    let wrote = 0;
    for (const t of targets) {
      const data = readJson(t.path, null);
      if (!data || typeof data !== "object") continue;
      const next = { ...data };
      if (illPayload) {
        next.chapterIllustration = illPayload;
      } else {
        delete next.chapterIllustration;
      }
      writeJson(t.path, next);
      wrote += 1;
    }
    if (!wrote) {
      return res.status(500).json({ error: "未能写入任何已发布章节文件" });
    }
    clearReadCacheByPrefix("study:");
    res.json({ ok: true, chapterIllustration: illPayload });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "写入失败" });
  }
});

async function finalizeIllustrationUploadToPngBuffer(buf) {
  const sharp = await getSharp();
  const meta = await sharp(buf).metadata();
  if (!meta.width || !meta.height) {
    throw new Error("无法读取图片尺寸");
  }
  const maxDim = 4096;
  let pipeline = sharp(buf).rotate();
  if (meta.width > maxDim || meta.height > maxDim) {
    pipeline = pipeline.resize(maxDim, maxDim, {
      fit: "inside",
      withoutEnlargement: true,
    });
  }
  return pipeline.png({ compressionLevel: 9, effort: 6 }).toBuffer();
}

async function handleChapterIllustrationFileUpload(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  const file = req.file;
  if (!file || !file.path) {
    return res.status(400).json({
      ok: false,
      success: false,
      error: "请选择图片文件（表单字段名 file）",
    });
  }
  const bookId = safeText(req.body?.bookId || "")
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, 16);
  const bookTok = bookId || "bk";
  const chRaw = req.body?.chapter;
  const chNum = Number(chRaw);
  const chTok = Number.isFinite(chNum)
    ? String(Math.max(0, Math.floor(chNum)))
    : "0";
  try {
    const buf = fs.readFileSync(file.path);
    const pngBuf = await finalizeIllustrationUploadToPngBuffer(buf);
    ensureDir(CHAPTER_ILLUSTRATION_GENERATED_DIR);
    const filename = `ill-upload-${bookTok}-${chTok}-${Date.now()}.png`;
    const dest = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, filename);
    fs.writeFileSync(dest, pngBuf);
    try {
      await ensureGeneratedThumbForFilename(filename, READER_IMAGE_MAX_EDGE);
    } catch (thumbErr) {
      console.warn("[chapter-illustration-upload:auto-thumb]", thumbErr?.message || thumbErr);
    }
    try {
      fs.unlinkSync(file.path);
    } catch {
      /* ignore */
    }
    const imageUrl = `/generated/${filename}`;
    res.json({ ok: true, success: true, imageUrl });
  } catch (e) {
    try {
      fs.unlinkSync(file.path);
    } catch {
      /* ignore */
    }
    console.error(e);
    res.status(500).json({
      ok: false,
      success: false,
      error: e.message || "处理图片失败",
    });
  }
}

async function handleBibleCharacterPortraitUpload(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  const file = req.file;
  if (!file || !file.path) {
    return res.status(400).json({
      ok: false,
      success: false,
      error: "请选择图片文件（表单字段名 file）",
    });
  }
  const englishName = safeText(req.body?.englishName || "");
  const nameEnTok = englishName.replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 36);
  if (!nameEnTok) {
    try {
      fs.unlinkSync(file.path);
    } catch {
      /* ignore */
    }
    return res.status(400).json({
      ok: false,
      success: false,
      error: "请填写有效英文名 englishName（用于文件名）",
    });
  }
  let slotIndex = Number(req.body?.slotIndex);
  if (!Number.isFinite(slotIndex) || slotIndex < 0 || slotIndex > 2) {
    slotIndex = 0;
  }
  slotIndex = Math.floor(slotIndex);
  const nameSlotTok = "p" + String(slotIndex);
  try {
    const buf = fs.readFileSync(file.path);
    const pngBuf = await finalizeIllustrationUploadToPngBuffer(buf);
    ensureDir(CHAPTER_ILLUSTRATION_GENERATED_DIR);
    const filename = `ill-char-${nameEnTok}-${nameSlotTok}-up-${Date.now()}.png`;
    const dest = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, filename);
    fs.writeFileSync(dest, pngBuf);
    try {
      await ensureGeneratedThumbForFilename(filename, READER_IMAGE_MAX_EDGE);
    } catch (thumbErr) {
      console.warn("[bible-character-upload:auto-thumb]", thumbErr?.message || thumbErr);
    }
    try {
      fs.unlinkSync(file.path);
    } catch {
      /* ignore */
    }
    const imageUrl = `/generated/${filename}`;
    res.json({ ok: true, success: true, imageUrl });
  } catch (e) {
    try {
      fs.unlinkSync(file.path);
    } catch {
      /* ignore */
    }
    console.error(e);
    res.status(500).json({
      ok: false,
      success: false,
      error: e.message || "处理图片失败",
    });
  }
}

function chapterIllustrationUploadPostRoute(req, res) {
  void handleChapterIllustrationFileUpload(req, res).catch((err) => {
    console.error(err);
    if (!res.headersSent) {
      res.status(500).json({
        ok: false,
        success: false,
        error: err.message || "上传失败",
      });
    }
  });
}

function bibleCharacterPortraitUploadPostRoute(req, res) {
  void handleBibleCharacterPortraitUpload(req, res).catch((err) => {
    console.error(err);
    if (!res.headersSent) {
      res.status(500).json({
        ok: false,
        success: false,
        error: err.message || "上传失败",
      });
    }
  });
}

/** 与 chapter-illustration 同前缀，便于反代放行 */
app.post(
  "/api/chapter-illustration/upload-file",
  illustrationImageUploadMiddleware,
  chapterIllustrationUploadPostRoute
);
app.post(
  "/api/admin/bible-character/upload-portrait",
  illustrationImageUploadMiddleware,
  bibleCharacterPortraitUploadPostRoute
);

function sanitizeGeneratedRelativePngPath(rawPath) {
  const raw = String(rawPath || "").trim().replace(/\\/g, "/");
  if (!raw) return "";
  const noQuery = raw.split("?")[0].replace(/^\/+/, "");
  const base = path.posix.basename(noQuery);
  if (!/^[a-zA-Z0-9_.-]+\.png$/i.test(base)) return "";
  const parts = noQuery.split("/").filter(Boolean);
  if (!parts.length) return "";
  const clean = [];
  for (const part of parts) {
    if (part === "." || part === "..") return "";
    if (!/^[a-zA-Z0-9_.-]+$/.test(part)) return "";
    clean.push(part);
  }
  if (clean[0] === "generated") clean.shift();
  if (!clean.length) return "";
  if (clean[0] === "thumbs") return "";
  return clean.join("/");
}

function resolveSafeGeneratedPngPath(urlPath) {
  const s = String(urlPath || "").trim();
  if (!s.startsWith("/generated/")) return null;
  const rel = sanitizeGeneratedRelativePngPath(s.slice("/generated/".length));
  if (!rel) return null;
  const full = path.resolve(path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, rel));
  const root = path.resolve(CHAPTER_ILLUSTRATION_GENERATED_DIR) + path.sep;
  if (!full.startsWith(root)) return null;
  try {
    if (!fs.statSync(full).isFile()) return null;
  } catch {
    return null;
  }
  return full;
}

/**
 * 章末人物缩图：支持 n=文件名.png（推荐，查询串无斜杠，反代/浏览器兼容好）或 path=/generated/…
 */
function rosterPortraitQueryToGeneratedPath(req) {
  let raw = String(req.query.n || req.query.path || "").trim();
  try {
    raw = decodeURIComponent(raw);
  } catch (_) {
    /* ignore */
  }
  raw = String(raw || "").trim();
  if (!raw) return null;
  if (!raw.includes("/")) {
    return `/generated/${raw.replace(/^\/+/, "")}`;
  }
  return raw.startsWith("/") ? raw : `/${raw.replace(/^\/+/, "")}`;
}

async function handleRosterPortraitGet(req, res) {
  try {
    const urlPath = rosterPortraitQueryToGeneratedPath(req);
    if (!urlPath) {
      res.status(400).type("text/plain").send("Missing n or path");
      return;
    }
    const filePath = resolveSafeGeneratedPngPath(urlPath);
    if (!filePath) {
      res.status(400).type("text/plain").send("Invalid path");
      return;
    }
    const wRaw = Number(req.query.w ?? req.query.max ?? READER_IMAGE_MAX_EDGE);
    const dim = Number.isFinite(wRaw)
      ? Math.min(READER_IMAGE_MAX_EDGE_LIMIT, Math.max(48, Math.round(wRaw)))
      : READER_IMAGE_MAX_EDGE;

    const buf = await fs.promises.readFile(filePath);
    let outBuf = buf;
    try {
      const sharp = await getSharp();
      outBuf = await sharp(buf)
        .rotate()
        .resize({
          width: dim,
          height: dim,
          fit: "inside",
          withoutEnlargement: true,
        })
        .png({ compressionLevel: 9, effort: 6 })
        .toBuffer();
    } catch (e) {
      console.warn("[roster-portrait:fallback]", e.message || e);
    }

    res.setHeader("Content-Type", "image/png");
    res.setHeader(
      "Cache-Control",
      "public, max-age=604800, stale-while-revalidate=86400"
    );
    res.send(outBuf);
  } catch (e) {
    console.error("[roster-portrait]", e);
    if (!res.headersSent) {
      res.status(500).type("text/plain").send("Resize failed");
    }
  }
}

app.get("/api/roster-portrait", handleRosterPortraitGet);
/** 短路径：个别反代对长 URL 段不友好时可用 */
app.get("/api/rp", handleRosterPortraitGet);

async function handleChapterIllustrationImageGet(req, res) {
  try {
    const urlPath = rosterPortraitQueryToGeneratedPath(req);
    if (!urlPath) {
      res.status(400).type("text/plain").send("Missing n or path");
      return;
    }
    const filePath = resolveSafeGeneratedPngPath(urlPath);
    if (!filePath) {
      res.status(400).type("text/plain").send("Invalid path");
      return;
    }
    const widthRaw = Number(req.query.w ?? 960);
    const width = Number.isFinite(widthRaw)
      ? Math.min(2200, Math.max(320, Math.round(widthRaw)))
      : 960;

    const buf = await fs.promises.readFile(filePath);
    let outBuf = buf;
    try {
      const sharp = await getSharp();
      outBuf = await sharp(buf)
        .rotate()
        .resize({
          width,
          withoutEnlargement: true,
        })
        .png({ compressionLevel: 9, effort: 6 })
        .toBuffer();
    } catch (e) {
      console.warn("[chapter-illustration-image:fallback]", e.message || e);
    }

    res.setHeader("Content-Type", "image/png");
    res.setHeader(
      "Cache-Control",
      "public, max-age=604800, stale-while-revalidate=86400"
    );
    res.send(outBuf);
  } catch (e) {
    console.error("[chapter-illustration-image]", e);
    if (!res.headersSent) {
      res.status(500).type("text/plain").send("Resize failed");
    }
  }
}

app.get("/api/chapter-illustration-image", handleChapterIllustrationImageGet);
app.get("/api/ci", handleChapterIllustrationImageGet);

function listGeneratedPngRelativePaths() {
  const root = CHAPTER_ILLUSTRATION_GENERATED_DIR;
  if (!fs.existsSync(root)) return [];
  const out = [];

  function walk(absDir, relDir = "") {
    const entries = fs.readdirSync(absDir, { withFileTypes: true });
    for (const ent of entries) {
      const name = ent.name;
      if (!name || name.startsWith(".")) continue;
      const nextRel = relDir ? `${relDir}/${name}` : name;
      const nextAbs = path.join(absDir, name);
      if (ent.isDirectory()) {
        if (name === "thumbs") continue;
        walk(nextAbs, nextRel);
        continue;
      }
      if (!ent.isFile()) continue;
      const rel = sanitizeGeneratedRelativePngPath(nextRel);
      if (!rel) continue;
      out.push(rel);
    }
  }

  walk(root, "");
  out.sort((a, b) => a.localeCompare(b, "en"));
  return out;
}

/** 若已存在 public/generated/thumbs/<同名>.png，读经章末人物优先用静态小图（免动态缩图） */
function rosterThumbRelativeUrlIfExists(imageUrlNorm) {
  const base = path.basename(String(imageUrlNorm || "").split("?")[0]);
  if (!/^[a-zA-Z0-9_.-]+\.png$/i.test(base)) return "";
  const thumbAbs = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, "thumbs", base);
  try {
    if (fs.existsSync(thumbAbs) && fs.statSync(thumbAbs).isFile()) {
      return `/generated/thumbs/${base}`;
    }
  } catch (_) {
    /* ignore */
  }
  return "";
}

function appendGeneratedAssetVersion(url) {
  const raw = String(url || "").trim();
  if (!raw || !raw.startsWith("/generated/")) return raw;
  const abs = resolveSafeGeneratedPngPath(raw);
  if (!abs) return raw;
  try {
    const st = fs.statSync(abs);
    const ver = Math.max(1, Math.floor(Number(st.mtimeMs) || 0));
    if (!ver) return raw;
    return raw + (raw.includes("?") ? "&" : "?") + "v=" + String(ver);
  } catch (_) {
    return raw;
  }
}

function sanitizeCharacterEnglishToken(value) {
  return String(value || "")
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, 36);
}

function generatedUrlExists(rawUrl) {
  const s = String(rawUrl || "").trim();
  if (!s) return false;
  if (/^https?:\/\//i.test(s)) return true;
  return Boolean(resolveSafeGeneratedPngPath(s));
}

function collectInvalidCharacterProfileRefs(profilesRoot) {
  const chars =
    profilesRoot &&
    typeof profilesRoot === "object" &&
    profilesRoot.characters &&
    typeof profilesRoot.characters === "object"
      ? profilesRoot.characters
      : {};
  const out = [];

  function pushInvalid(zh, field, value) {
    const raw = String(value || "").trim();
    if (!raw || generatedUrlExists(raw)) return;
    out.push({ zhName: zh, field, value: raw });
  }

  for (const [zh, row] of Object.entries(chars)) {
    if (!row || typeof row !== "object") continue;
    pushInvalid(zh, "imageUrl", row.imageUrl);
    pushInvalid(zh, "heroImageUrl", row.heroImageUrl);
    pushInvalid(zh, "comparisonSheetUrl", row.comparisonSheetUrl);
    const periods = Array.isArray(row.periods) ? row.periods : [];
    for (let i = 0; i < periods.length; i++) {
      pushInvalid(zh, `periods[${i}].imageUrl`, periods[i]?.imageUrl);
    }
  }
  return out;
}

function scanCharacterGeneratedImages() {
  const out = new Map();
  let names = [];
  try {
    names = fs.readdirSync(CHAPTER_ILLUSTRATION_GENERATED_DIR);
  } catch {
    return out;
  }

  function ensureBucket(enKey) {
    if (!out.has(enKey)) {
      out.set(enKey, {
        hero: [],
        p0: [],
        p1: [],
        p2: [],
        sheet: [],
        legacyHero: [],
        legacyCmp: [],
        legacyAmb: [],
      });
    }
    return out.get(enKey);
  }

  function pushSlot(enKey, slot, filename, ts) {
    if (!enKey || !slot) return;
    const bucket = ensureBucket(enKey);
    if (!Array.isArray(bucket[slot])) return;
    bucket[slot].push({
      url: `/generated/${filename}`,
      ts: Number.isFinite(ts) ? ts : 0,
      filename,
    });
  }

  for (const filename of names) {
    if (!/^ill-char-.*\.png$/i.test(filename)) continue;
    let m = filename.match(/^ill-char-([A-Za-z0-9_-]+)-(hero|p0|p1|p2|sheet)-(\d{12,})\.png$/i);
    if (m) {
      pushSlot(m[1].toLowerCase(), m[2], filename, Number(m[3]));
      continue;
    }
    m = filename.match(/^ill-char-([A-Za-z0-9_-]+)-(p0|p1|p2)-up-(\d{12,})\.png$/i);
    if (m) {
      pushSlot(m[1].toLowerCase(), m[2], filename, Number(m[3]));
      continue;
    }
    m = filename.match(/^ill-char-([A-Za-z0-9_-]+)-he-(\d{12,})\.png$/i);
    if (m) {
      pushSlot(m[1].toLowerCase(), "legacyHero", filename, Number(m[2]));
      continue;
    }
    m = filename.match(/^ill-char-([A-Za-z0-9_-]+)-cm-(\d{12,})\.png$/i);
    if (m) {
      pushSlot(m[1].toLowerCase(), "legacyCmp", filename, Number(m[2]));
      continue;
    }
    m = filename.match(/^ill-char-([A-Za-z0-9_-]+)--(\d{12,})\.png$/i);
    if (m) {
      pushSlot(m[1].toLowerCase(), "legacyAmb", filename, Number(m[2]));
    }
  }

  for (const bucket of out.values()) {
    for (const key of Object.keys(bucket)) {
      bucket[key].sort((a, b) => b.ts - a.ts);
    }
  }
  return out;
}

function chooseLatestCharacterGeneratedUrl(bucket, slots) {
  if (!bucket || !Array.isArray(slots)) return "";
  for (const slot of slots) {
    const arr = Array.isArray(bucket[slot]) ? bucket[slot] : [];
    if (arr.length && arr[0]?.url) return arr[0].url;
  }
  return "";
}

function repairCharacterProfileImageRefs(profilesRoot) {
  const root =
    profilesRoot && typeof profilesRoot === "object" ? profilesRoot : { characters: {} };
  const characters =
    root.characters && typeof root.characters === "object" ? root.characters : {};
  const scanned = scanCharacterGeneratedImages();
  const repaired = [];
  let touched = 0;

  for (const [zh, row] of Object.entries(characters)) {
    if (!row || typeof row !== "object") continue;
    const enKey = sanitizeCharacterEnglishToken(row.englishName).toLowerCase();
    const bucket = enKey ? scanned.get(enKey) : null;
    let changedForRow = false;

    const currentP0 = String(row.imageUrl || "").trim();
    if (!generatedUrlExists(currentP0)) {
      const recoveredP0 = chooseLatestCharacterGeneratedUrl(bucket, ["p0"]);
      if (recoveredP0) {
        row.imageUrl = recoveredP0;
        repaired.push(`${zh}: imageUrl ← ${recoveredP0}`);
        changedForRow = true;
      }
    }

    if (Array.isArray(row.periods)) {
      for (let i = 0; i < row.periods.length; i++) {
        const period = row.periods[i];
        if (!period || typeof period !== "object") continue;
        const current = String(period.imageUrl || "").trim();
        if (generatedUrlExists(current)) continue;
        const recovered = chooseLatestCharacterGeneratedUrl(bucket, [`p${i + 1}`]);
        if (!recovered) continue;
        period.imageUrl = recovered;
        repaired.push(`${zh}: periods[${i}].imageUrl ← ${recovered}`);
        changedForRow = true;
      }
    }

    const currentHero = String(row.heroImageUrl || "").trim();
    if (!generatedUrlExists(currentHero)) {
      const recoveredHero = chooseLatestCharacterGeneratedUrl(bucket, [
        "hero",
        "p1",
        "p0",
        "p2",
        "legacyHero",
        "legacyAmb",
      ]);
      if (recoveredHero) {
        row.heroImageUrl = recoveredHero;
        repaired.push(`${zh}: heroImageUrl ← ${recoveredHero}`);
        changedForRow = true;
      }
    }

    const currentCmp = String(row.comparisonSheetUrl || "").trim();
    if (!generatedUrlExists(currentCmp)) {
      const recoveredCmp =
        chooseLatestCharacterGeneratedUrl(bucket, ["sheet", "legacyCmp", "legacyAmb"]) ||
        String(row.imageUrl || "").trim() ||
        String(row.periods?.[0]?.imageUrl || "").trim() ||
        String(row.heroImageUrl || "").trim();
      if (generatedUrlExists(recoveredCmp)) {
        row.comparisonSheetUrl = recoveredCmp;
        repaired.push(`${zh}: comparisonSheetUrl ← ${recoveredCmp}`);
        changedForRow = true;
      }
    }

    if (changedForRow) touched += 1;
  }

  return {
    profiles: root,
    touchedCharacters: touched,
    repaired,
  };
}

function summarizeCharacterProfileImageStats(profilesRoot) {
  const chars =
    profilesRoot &&
    typeof profilesRoot === "object" &&
    profilesRoot.characters &&
    typeof profilesRoot.characters === "object"
      ? profilesRoot.characters
      : {};
  let profileCount = 0;
  let heroRefCount = 0;
  let heroResolvedCount = 0;
  let anyImageResolvedCount = 0;
  for (const row of Object.values(chars)) {
    if (!row || typeof row !== "object") continue;
    profileCount += 1;
    const hero = String(row.heroImageUrl || "").trim();
    if (hero) heroRefCount += 1;
    if (hero && generatedUrlExists(hero)) heroResolvedCount += 1;
    const refs = [
      String(row.imageUrl || "").trim(),
      hero,
      String(row.comparisonSheetUrl || "").trim(),
      ...(Array.isArray(row.periods)
        ? row.periods.map((p) => String(p?.imageUrl || "").trim())
        : []),
    ].filter(Boolean);
    if (refs.some((ref) => generatedUrlExists(ref))) {
      anyImageResolvedCount += 1;
    }
  }
  return {
    profileCount,
    heroRefCount,
    heroResolvedCount,
    anyImageResolvedCount,
  };
}

function collectGeneratedPortraitBaseNamesFromProfiles(profilesRoot) {
  const out = [];
  const seen = new Set();
  const chars =
    profilesRoot &&
    typeof profilesRoot === "object" &&
    profilesRoot.characters &&
    typeof profilesRoot.characters === "object"
      ? profilesRoot.characters
      : {};

  function pushUrl(raw) {
    const s = String(raw || "").trim();
    if (!s) return;
    let pathname = s;
    if (/^https?:\/\//i.test(s)) {
      try {
        pathname = new URL(s).pathname || "";
      } catch {
        return;
      }
    }
    if (!pathname.startsWith("/generated/")) return;
    const base = path.basename(pathname.split("?")[0]);
    if (!/^[a-zA-Z0-9_.-]+\.png$/i.test(base)) return;
    if (seen.has(base)) return;
    seen.add(base);
    out.push(base);
  }

  for (const entry of Object.values(chars)) {
    if (!entry || typeof entry !== "object") continue;
    pushUrl(entry.imageUrl);
    pushUrl(entry.heroImageUrl);
    pushUrl(entry.comparisonSheetUrl);
    const periods = Array.isArray(entry.periods) ? entry.periods : [];
    for (const period of periods) {
      if (!period || typeof period !== "object") continue;
      pushUrl(period.imageUrl);
    }
  }
  return out;
}

async function ensureGeneratedRosterThumbsForProfiles(
  profilesRoot,
  maxEdge = READER_IMAGE_MAX_EDGE
) {
  const names = collectGeneratedPortraitBaseNamesFromProfiles(profilesRoot);
  if (!names.length) {
    return { built: [], failed: [] };
  }
  const sharp = await getSharp();
  const dim = Math.min(
    READER_IMAGE_MAX_EDGE_LIMIT,
    Math.max(64, Math.round(Number(maxEdge) || READER_IMAGE_MAX_EDGE))
  );
  const thumbDir = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, "thumbs");
  ensureDir(thumbDir);
  const built = [];
  const failed = [];
  for (const name of names) {
    try {
      const srcPath = resolveSafeGeneratedPngPath(`/generated/${name}`);
      if (!srcPath) {
        failed.push({ name, error: "无效或不存在" });
        continue;
      }
      const destPath = path.join(thumbDir, name);
      const buf = await fs.promises.readFile(srcPath);
      const outBuf = await sharp(buf)
        .rotate()
        .resize({
          width: dim,
          height: dim,
          fit: "inside",
          withoutEnlargement: true,
        })
        .png({ compressionLevel: 9, effort: 6 })
        .toBuffer();
      await fs.promises.writeFile(destPath, outBuf);
      built.push(name);
    } catch (e) {
      failed.push({ name, error: e.message || String(e) });
    }
  }
  return { built, failed };
}

async function ensureGeneratedThumbForFilename(
  name,
  maxEdge = READER_IMAGE_MAX_EDGE
) {
  const base = path.basename(String(name || "").trim());
  if (!/^[a-zA-Z0-9_.-]+\.png$/i.test(base)) return false;
  const srcPath = resolveSafeGeneratedPngPath(`/generated/${base}`);
  if (!srcPath) return false;
  const sharp = await getSharp();
  const dim = Math.min(
    READER_IMAGE_MAX_EDGE_LIMIT,
    Math.max(64, Math.round(Number(maxEdge) || READER_IMAGE_MAX_EDGE))
  );
  const thumbDir = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, "thumbs");
  ensureDir(thumbDir);
  const destPath = path.join(thumbDir, base);
  const buf = await fs.promises.readFile(srcPath);
  const outBuf = await sharp(buf)
    .rotate()
    .resize({
      width: dim,
      height: dim,
      fit: "inside",
      withoutEnlargement: true,
    })
    .png({ compressionLevel: 9, effort: 6 })
    .toBuffer();
  await fs.promises.writeFile(destPath, outBuf);
  return true;
}

function handleAdminGeneratedPngsList(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const page = Math.max(1, Math.floor(Number(req.query.page) || 1));
    const pageSize = Math.min(
      80,
      Math.max(8, Math.floor(Number(req.query.pageSize) || 24))
    );
    const q = safeText(req.query.q || "").toLowerCase();
    let names = listGeneratedPngRelativePaths();
    if (q) names = names.filter((n) => n.toLowerCase().includes(q));
    const total = names.length;
    const start = (page - 1) * pageSize;
    const slice = names.slice(start, start + pageSize);
    const thumbDir = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, "thumbs");
    ensureDir(thumbDir);
    const items = slice.map((name) => {
      const full = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, name);
      const st = fs.statSync(full);
      const th = path.join(thumbDir, path.basename(name));
      return {
        name,
        bytes: st.size,
        mtime: st.mtime.toISOString(),
        hasThumb: fs.existsSync(th),
      };
    });
    res.set("Cache-Control", "no-store");
    res.json({ total, page, pageSize, items });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "列出失败" });
  }
}

async function handleAdminGeneratedPngsRebuildThumbs(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const sharp = await getSharp();
    const body = req.body || {};
    const namesIn = Array.isArray(body.names) ? body.names : [];
    const dim = Math.min(
      READER_IMAGE_MAX_EDGE_LIMIT,
      Math.max(64, Math.round(Number(body.maxEdge) || READER_IMAGE_MAX_EDGE))
    );
    const MAX = 80;
    const names = [];
    const seen = new Set();
    for (const raw of namesIn) {
      const n = sanitizeGeneratedRelativePngPath(safeText(String(raw || "")));
      if (!n) continue;
      if (seen.has(n)) continue;
      seen.add(n);
      names.push(n);
      if (names.length >= MAX) break;
    }
    if (!names.length) {
      return res.status(400).json({
        error: "缺少有效的 names 数组（public/generated 根目录下的 .png 文件名）",
      });
    }
    const thumbDir = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, "thumbs");
    ensureDir(thumbDir);
    const built = [];
    const failed = [];
    for (const name of names) {
      try {
        const srcPath = resolveSafeGeneratedPngPath(`/generated/${name}`);
        if (!srcPath) {
          failed.push({ name, error: "无效或不存在" });
          continue;
        }
        const destPath = path.join(thumbDir, path.basename(name));
        const buf = await fs.promises.readFile(srcPath);
        const outBuf = await sharp(buf)
          .rotate()
          .resize({
            width: dim,
            height: dim,
            fit: "inside",
            withoutEnlargement: true,
          })
          .png({ compressionLevel: 9, effort: 6 })
          .toBuffer();
        await fs.promises.writeFile(destPath, outBuf);
        built.push(name);
      } catch (e) {
        failed.push({ name, error: e.message || String(e) });
      }
    }
    clearReadCacheByPrefix(`${STUDY_CONTENT_CACHE_TAG}:`);
    appendAdminAudit(req, authed, "generated_png_rebuild_thumbs", {
      dim,
      built: built.length,
      failed: failed.length,
    });
    res.json({ ok: true, dim, built, failed });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "生成失败" });
  }
}

async function handleAdminGeneratedPngsResetThumbs(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const sharp = await getSharp();
    const body = req.body || {};
    const dim = Math.min(
      READER_IMAGE_MAX_EDGE_LIMIT,
      Math.max(64, Math.round(Number(body.maxEdge) || READER_IMAGE_MAX_EDGE))
    );
    const allNames = listGeneratedPngRelativePaths();
    const thumbDir = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, "thumbs");
    ensureDir(thumbDir);

    const removedThumbs = [];
    const staleEntries = fs.readdirSync(thumbDir, { withFileTypes: true });
    for (const ent of staleEntries) {
      if (!ent.isFile()) continue;
      if (!/\.png$/i.test(ent.name)) continue;
      const abs = path.join(thumbDir, ent.name);
      fs.unlinkSync(abs);
      removedThumbs.push(ent.name);
    }

    const built = [];
    const failed = [];
    for (const name of allNames) {
      try {
        const srcPath = resolveSafeGeneratedPngPath(`/generated/${name}`);
        if (!srcPath) {
          failed.push({ name, error: "无效或不存在" });
          continue;
        }
        const destPath = path.join(thumbDir, path.basename(name));
        const buf = await fs.promises.readFile(srcPath);
        const outBuf = await sharp(buf)
          .rotate()
          .resize({
            width: dim,
            height: dim,
            fit: "inside",
            withoutEnlargement: true,
          })
          .png({ compressionLevel: 9, effort: 6 })
          .toBuffer();
        await fs.promises.writeFile(destPath, outBuf);
        built.push(name);
      } catch (e) {
        failed.push({ name, error: e.message || String(e) });
      }
    }
    clearReadCacheByPrefix(`${STUDY_CONTENT_CACHE_TAG}:`);
    appendAdminAudit(req, authed, "generated_png_reset_thumbs", {
      dim,
      removedThumbs: removedThumbs.length,
      rebuilt: built.length,
      failed: failed.length,
    });
    res.json({
      ok: true,
      dim,
      removedThumbs,
      built,
      failed,
      totalSourcePngs: allNames.length,
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "重建全部缩略图失败" });
  }
}

async function handleAdminGeneratedPngsDelete(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const body = req.body || {};
    const namesIn = Array.isArray(body.names) ? body.names : [];
    const MAX = 80;
    const names = [];
    const seen = new Set();
    for (const raw of namesIn) {
      const n = sanitizeGeneratedRelativePngPath(safeText(String(raw || "")));
      if (!n) continue;
      if (seen.has(n)) continue;
      seen.add(n);
      names.push(n);
      if (names.length >= MAX) break;
    }
    if (!names.length) {
      return res.status(400).json({
        error: "缺少有效的 names 数组（public/generated 根目录下的 .png 文件名）",
      });
    }
    const thumbDir = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, "thumbs");
    ensureDir(thumbDir);
    const deleted = [];
    const missing = [];
    const failed = [];
    for (const name of names) {
      const srcPath = resolveSafeGeneratedPngPath(`/generated/${name}`);
      if (!srcPath) {
        missing.push(name);
        continue;
      }
      try {
        await fs.promises.unlink(srcPath);
        const thumbPath = path.join(thumbDir, path.basename(name));
        let thumbDeleted = false;
        try {
          await fs.promises.unlink(thumbPath);
          thumbDeleted = true;
        } catch (thumbErr) {
          if (thumbErr && thumbErr.code !== "ENOENT") throw thumbErr;
        }
        deleted.push({ name, thumbDeleted });
      } catch (e) {
        failed.push({ name, error: e.message || String(e) });
      }
    }
    clearReadCacheByPrefix(`${STUDY_CONTENT_CACHE_TAG}:`);
    appendAdminAudit(req, authed, "generated_png_delete", {
      requested: names.length,
      deleted: deleted.length,
      missing: missing.length,
      failed: failed.length,
    });
    res.json({ ok: true, deleted, missing, failed });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "删除失败" });
  }
}

function listPublishedChapterIllustrationSummaries() {
  const rowsByKey = new Map();
  const imageUsage = new Map();
  const versions = listPublishedContentVersionIds();

  for (const versionId of versions) {
    const versionDir = path.join(CONTENT_PUBLISHED_DIR, versionId);
    let langs = [];
    try {
      langs = fs
        .readdirSync(versionDir, { withFileTypes: true })
        .filter((d) => d.isDirectory())
        .map((d) => d.name);
    } catch {
      continue;
    }
    for (const lang of langs) {
      const langDir = path.join(versionDir, lang);
      let bookDirs = [];
      try {
        bookDirs = fs
          .readdirSync(langDir, { withFileTypes: true })
          .filter((d) => d.isDirectory())
          .map((d) => d.name);
      } catch {
        continue;
      }
      for (const bookId of bookDirs) {
        const bookDir = path.join(langDir, bookId);
        let chapterFiles = [];
        try {
          chapterFiles = fs
            .readdirSync(bookDir, { withFileTypes: true })
            .filter((d) => d.isFile() && /\.json$/i.test(d.name))
            .map((d) => d.name);
        } catch {
          continue;
        }
        for (const fileName of chapterFiles) {
          const chapter = Number(String(fileName).replace(/\.json$/i, ""));
          if (!Number.isFinite(chapter)) continue;
          const fullPath = path.join(bookDir, fileName);
          const data = readJson(fullPath, null);
          const ill = normalizeChapterIllustrationForSave(data?.chapterIllustration);
          if (!ill || !ill.imageUrl) continue;
          const key = `${bookId}:${chapter}`;
          const ref = {
            versionId,
            lang,
            imageUrl: ill.imageUrl,
            updatedAt: safeText(ill.updatedAt || ""),
          };
          let row = rowsByKey.get(key);
          if (!row) {
            row = {
              bookId,
              bookName: getBookLabelById(bookId),
              chapter,
              refs: [],
            };
            rowsByKey.set(key, row);
          }
          row.refs.push(ref);
        }
      }
    }
  }

  function refPriority(ref) {
    const v = String(ref?.versionId || "");
    const l = String(ref?.lang || "");
    if (v === "default" && l === "zh") return 0;
    if (v === "default") return 1;
    if (l === "zh") return 2;
    return 3;
  }

  const rows = [];
  for (const row of rowsByKey.values()) {
    const refs = Array.isArray(row.refs) ? row.refs.slice() : [];
    refs.sort((a, b) => {
      const pd = refPriority(a) - refPriority(b);
      if (pd !== 0) return pd;
      const vd = String(a.versionId).localeCompare(String(b.versionId));
      if (vd !== 0) return vd;
      return String(a.lang).localeCompare(String(b.lang));
    });
    const preferred = refs[0] || {};
    const imageUrl = safeText(preferred.imageUrl || "");
    const uniqueUrls = [...new Set(refs.map((x) => safeText(x.imageUrl || "")).filter(Boolean))];
    if (imageUrl) {
      imageUsage.set(imageUrl, (imageUsage.get(imageUrl) || 0) + 1);
    }
    rows.push({
      bookId: row.bookId,
      bookName: row.bookName,
      chapter: row.chapter,
      imageUrl,
      updatedAt:
        safeText(preferred.updatedAt || "") ||
        refs.map((x) => safeText(x.updatedAt || "")).filter(Boolean).sort().reverse()[0] ||
        "",
      refCount: refs.length,
      variantCount: uniqueUrls.length,
      refs: refs.map((ref) => ({
        versionId: ref.versionId,
        lang: ref.lang,
        imageUrl: ref.imageUrl,
        updatedAt: ref.updatedAt,
      })),
    });
  }

  rows.forEach((row) => {
    row.usageCount = row.imageUrl ? imageUsage.get(row.imageUrl) || 0 : 0;
    const normalizedImage = safeText(row.imageUrl || "");
    row.generatedName = normalizedImage.startsWith("/generated/")
      ? path.basename(normalizedImage.split("?")[0])
      : "";
  });

  rows.sort((a, b) => {
    const bd = String(a.bookId).localeCompare(String(b.bookId));
    if (bd !== 0) return bd;
    return Number(a.chapter) - Number(b.chapter);
  });
  return rows;
}

function handleAdminChapterIllustrationsList(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const page = Math.max(1, Math.floor(Number(req.query.page) || 1));
    const pageSize = Math.min(80, Math.max(8, Math.floor(Number(req.query.pageSize) || 24)));
    const q = safeText(req.query.q || "").toLowerCase();
    let rows = listPublishedChapterIllustrationSummaries();
    if (q) {
      rows = rows.filter((row) => {
        const hay = [
          row.bookId,
          row.bookName,
          String(row.chapter),
          row.imageUrl,
          row.generatedName,
        ]
          .join(" ")
          .toLowerCase();
        return hay.includes(q);
      });
    }
    const total = rows.length;
    const start = (page - 1) * pageSize;
    const items = rows.slice(start, start + pageSize);
    res.set("Cache-Control", "no-store");
    res.json({ total, page, pageSize, items });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message || "列出章节插图失败" });
  }
}

function extractChapterIllustrationGeneratedTimestamp(relPath) {
  const base = path.basename(String(relPath || "").trim());
  const m = base.match(/-(\d{10,})\.png$/i);
  return m ? Number(m[1]) || 0 : 0;
}

function listGeneratedChapterIllustrationCandidates(bookId, chapter) {
  const bid = safeText(bookId || "").toUpperCase();
  const ch = Number(chapter);
  if (!bid || !Number.isFinite(ch)) return [];
  const prefix = `ill-${bid}-${ch}-`;
  return listGeneratedPngRelativePaths()
    .filter((rel) => path.basename(rel).startsWith(prefix))
    .sort(
      (a, b) =>
        extractChapterIllustrationGeneratedTimestamp(b) -
          extractChapterIllustrationGeneratedTimestamp(a) ||
        path.basename(b).localeCompare(path.basename(a), "en")
    )
    .map((rel) => `/generated/${rel}`);
}

function findRepairableChapterIllustrationUrl({
  versionId,
  lang,
  bookId,
  chapter,
}) {
  const seen = new Set();
  const candidates = [];
  const push = (url) => {
    const norm = normalizeIllustrationImageUrlForPublication(url);
    if (!norm || seen.has(norm)) return;
    if (!resolveSafeGeneratedPngPath(norm)) return;
    seen.add(norm);
    candidates.push(norm);
  };

  const st = loadChapterIllustrationStateFromDisk(versionId, lang, bookId, chapter);
  push(st?.imageUrl || "");
  listGeneratedChapterIllustrationCandidates(bookId, chapter).forEach(push);
  return candidates[0] || "";
}

function auditPublishedChapterIllustrationRefs({ repair = false } = {}) {
  const versions = listPublishedContentVersionIds();
  const missing = [];
  const repaired = [];
  const unrecovered = [];
  let touchedChapters = 0;

  for (const versionId of versions) {
    const versionDir = path.join(CONTENT_PUBLISHED_DIR, versionId);
    let langs = [];
    try {
      langs = fs
        .readdirSync(versionDir, { withFileTypes: true })
        .filter((d) => d.isDirectory())
        .map((d) => d.name);
    } catch {
      continue;
    }
    for (const lang of langs) {
      const langDir = path.join(versionDir, lang);
      let bookDirs = [];
      try {
        bookDirs = fs
          .readdirSync(langDir, { withFileTypes: true })
          .filter((d) => d.isDirectory())
          .map((d) => d.name);
      } catch {
        continue;
      }
      for (const bookId of bookDirs) {
        const bookDir = path.join(langDir, bookId);
        let chapterFiles = [];
        try {
          chapterFiles = fs
            .readdirSync(bookDir, { withFileTypes: true })
            .filter((d) => d.isFile() && /\.json$/i.test(d.name))
            .map((d) => d.name);
        } catch {
          continue;
        }
        for (const fileName of chapterFiles) {
          const chapter = Number(String(fileName).replace(/\.json$/i, ""));
          if (!Number.isFinite(chapter)) continue;
          const fullPath = path.join(bookDir, fileName);
          const data = readJson(fullPath, null);
          const ill = normalizeChapterIllustrationForSave(data?.chapterIllustration);
          if (!ill || !ill.imageUrl) continue;
          const imageUrl = safeText(ill.imageUrl || "");
          if (!imageUrl.startsWith("/generated/")) continue;
          if (resolveSafeGeneratedPngPath(imageUrl)) continue;

          const label = `${bookId} ${chapter}章 [${versionId}/${lang}]`;
          missing.push(`${label}: ${imageUrl}`);

          if (!repair) continue;
          const recovered = findRepairableChapterIllustrationUrl({
            versionId,
            lang,
            bookId,
            chapter,
          });
          if (!recovered || recovered === imageUrl) {
            unrecovered.push(`${label}: ${imageUrl}`);
            continue;
          }

          data.chapterIllustration = {
            ...(data.chapterIllustration && typeof data.chapterIllustration === "object"
              ? data.chapterIllustration
              : {}),
            imageUrl: recovered,
            updatedAt: nowIso(),
          };
          writeJson(fullPath, data);
          repaired.push(`${label}: ${imageUrl} -> ${recovered}`);
          touchedChapters += 1;
        }
      }
    }
  }

  return {
    missingCount: missing.length,
    missing,
    touchedChapters,
    repaired,
    unrecovered,
  };
}

function handleChapterIllustrationsRepairPreview(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    res.json({ ok: true, ...auditPublishedChapterIllustrationRefs({ repair: false }) });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message || "读取章插画缺图预检失败。" });
  }
}

function handleChapterIllustrationsRepair(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const result = auditPublishedChapterIllustrationRefs({ repair: true });
    clearReadCacheByPrefix(`${STUDY_CONTENT_CACHE_TAG}:`);
    appendAdminAudit(req, authed, "chapter_illustrations_repair_images", {
      missingCount: result.missingCount,
      repairedRefs: result.repaired.length,
      unrecoveredRefs: result.unrecovered.length,
      touchedChapters: result.touchedChapters,
    });
    res.json({ ok: true, ...result });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message || "修复章插画缺图失败。" });
  }
}

app.get("/api/admin/generated-pngs", handleAdminGeneratedPngsList);
/** 短路径：个别反代对较长 path 返回 404 时可用 */
app.get("/api/admin/gpngs", handleAdminGeneratedPngsList);
app.post("/api/admin/generated-pngs/rebuild-thumbs", handleAdminGeneratedPngsRebuildThumbs);
app.post("/api/admin/gpngs-rebuild", handleAdminGeneratedPngsRebuildThumbs);
app.post("/api/admin/generated-pngs/reset-thumbs", handleAdminGeneratedPngsResetThumbs);
app.post("/api/admin/gpngs-reset", handleAdminGeneratedPngsResetThumbs);
app.post("/api/admin/generated-pngs/delete", handleAdminGeneratedPngsDelete);
app.post("/api/admin/gpngs-delete", handleAdminGeneratedPngsDelete);
app.get("/api/admin/chapter-illustrations", handleAdminChapterIllustrationsList);
app.get("/api/admin/chapter-ills", handleAdminChapterIllustrationsList);
app.get(
  "/api/admin/chapter-illustrations/repair-images-preview",
  handleChapterIllustrationsRepairPreview
);
app.get("/api/admin/chapter-ills/repair-preview", handleChapterIllustrationsRepairPreview);
app.post(
  "/api/admin/chapter-illustrations/repair-images",
  handleChapterIllustrationsRepair
);
app.post("/api/admin/chapter-ills/repair", handleChapterIllustrationsRepair);

async function handleCharacterProfilesRepairImages(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const profiles = loadCharacterIllustrationProfiles();
    const result = repairCharacterProfileImageRefs(profiles);
    persistCharacterIllustrationProfilesRoot(result.profiles);
    rememberCharacterProfileImageAuditFromProfiles(result.profiles);
    let thumbBuild = { built: [], failed: [] };
    try {
      thumbBuild = await ensureGeneratedRosterThumbsForProfiles(
        result.profiles,
        READER_IMAGE_MAX_EDGE
      );
    } catch (thumbErr) {
      console.warn("[character-profiles:repair:auto-thumbs]", thumbErr?.message || thumbErr);
    }
    clearReadCacheByPrefix(`${STUDY_CONTENT_CACHE_TAG}:`);
    appendAdminAudit(req, authed, "bible_character_profiles_repair_images", {
      touchedCharacters: result.touchedCharacters,
      repairedRefs: result.repaired.length,
      rosterThumbsBuilt: thumbBuild.built.length,
      rosterThumbsFailed: thumbBuild.failed.length,
    });
    res.json({
      ok: true,
      touchedCharacters: result.touchedCharacters,
      repaired: result.repaired,
      rosterThumbsBuilt: thumbBuild.built.length,
      rosterThumbsFailed: thumbBuild.failed.length,
      profiles: result.profiles,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message || "修复人物图片引用失败。" });
  }
}

app.post(
  "/api/admin/bible-character-profiles/repair-images",
  handleCharacterProfilesRepairImages
);
app.post(
  "/api/admin/character-illustration-profiles/repair-images",
  handleCharacterProfilesRepairImages
);

function handleCharacterProfilesRepairPreview(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const profiles = loadCharacterIllustrationProfiles();
    const stats = summarizeCharacterProfileImageStats(profiles);
    const invalidRefs = collectInvalidCharacterProfileRefs(profiles);
    const cloned = JSON.parse(JSON.stringify(profiles || { characters: {} }));
    const repairPreview = repairCharacterProfileImageRefs(cloned);
    res.json({
      ok: true,
      ...stats,
      invalidRefCount: invalidRefs.length,
      invalidRefs,
      touchedCharacters: repairPreview.touchedCharacters,
      repaired: repairPreview.repaired,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message || "读取修复预检失败。" });
  }
}

app.get(
  "/api/admin/bible-character-profiles/repair-images-preview",
  handleCharacterProfilesRepairPreview
);
app.get(
  "/api/admin/character-illustration-profiles/repair-images-preview",
  handleCharacterProfilesRepairPreview
);

function bcdRgbDist2(r1, g1, b1, r2, g2, b2) {
  const dr = r1 - r2;
  const dg = g1 - g2;
  const db = b1 - b2;
  return dr * dr + dg * dg + db * db;
}

/**
 * 从裁切条四边向内做「与角点平均色接近」的 flood，将背景变透明（不调用 AI）。
 * tol2 为 RGB 欧氏距离平方阈值。
 */
function applyBcdEdgeFloodTransparent(rgba, subW, h, tol2) {
  let br = 0;
  let bg = 0;
  let bb = 0;
  const corners = [
    [0, 0],
    [subW - 1, 0],
    [0, h - 1],
    [subW - 1, h - 1],
  ];
  for (let i = 0; i < corners.length; i++) {
    const cx = corners[i][0];
    const cy = corners[i][1];
    const o = (cy * subW + cx) * 4;
    br += rgba[o];
    bg += rgba[o + 1];
    bb += rgba[o + 2];
  }
  br = Math.round(br / 4);
  bg = Math.round(bg / 4);
  bb = Math.round(bb / 4);
  const visited = new Uint8Array(subW * h);
  const qx = [];
  const qy = [];
  function tryEnqueue(x, y) {
    if (x < 0 || x >= subW || y < 0 || y >= h) return;
    const idx = y * subW + x;
    if (visited[idx]) return;
    const o = idx * 4;
    if (bcdRgbDist2(rgba[o], rgba[o + 1], rgba[o + 2], br, bg, bb) > tol2)
      return;
    visited[idx] = 1;
    qx.push(x);
    qy.push(y);
  }
  for (let x = 0; x < subW; x++) {
    tryEnqueue(x, 0);
    tryEnqueue(x, h - 1);
  }
  for (let y = 0; y < h; y++) {
    tryEnqueue(0, y);
    tryEnqueue(subW - 1, y);
  }
  let qi = 0;
  while (qi < qx.length) {
    const x = qx[qi];
    const y = qy[qi];
    qi++;
    const o = (y * subW + x) * 4;
    rgba[o + 3] = 0;
    tryEnqueue(x + 1, y);
    tryEnqueue(x - 1, y);
    tryEnqueue(x, y + 1);
    tryEnqueue(x, y - 1);
  }
}

/**
 * 非透明像素的轴对齐包围盒（用于裁切并列图单列：去掉四周留白再缩放，否则 fit:contain 会把「小人+大透明」整体缩小）。
 */
function bcdAlphaBoundingBox(rgba, subW, h, alphaMin = 14) {
  let minX = subW;
  let minY = h;
  let maxX = -1;
  let maxY = -1;
  let x;
  let y;
  for (y = 0; y < h; y++) {
    for (x = 0; x < subW; x++) {
      const o = (y * subW + x) * 4;
      if (rgba[o + 3] > alphaMin) {
        if (x < minX) minX = x;
        if (y < minY) minY = y;
        if (x > maxX) maxX = x;
        if (y > maxY) maxY = y;
      }
    }
  }
  if (maxX < minX) return null;
  return {
    left: minX,
    top: minY,
    width: maxX - minX + 1,
    height: maxY - minY + 1,
  };
}

function resolveBcdComparisonCropPrescale(body) {
  const b = body && typeof body === "object" ? body : {};
  const raw = b.comparisonCropPrescale ?? b.comparisonDetailScale;
  let n = Number(raw);
  if (!Number.isFinite(n) || n < 1) {
    const env = Number(process.env.BCD_COMPARISON_CROP_PRESCALE);
    n = Number.isFinite(env) && env >= 1 ? env : 1;
  }
  if (n > 2.5) n = 2.5;
  if (n < 1) n = 1;
  return n;
}

async function bcdExtractHeroPngFromComparisonBuffer(
  inputBuf,
  periodCount,
  columnIndex,
  opts
) {
  const sharp = await getSharp();
  opts = opts && typeof opts === "object" ? opts : {};
  const skipFlood = opts.skipFlood === true;
  let preScale = Number(opts.preScale);
  if (!Number.isFinite(preScale) || preScale < 1) preScale = 1;
  if (preScale > 2.5) preScale = 2.5;

  let buf = inputBuf;
  let meta = await sharp(buf).metadata();
  let w = meta.width || 0;
  let h = meta.height || 0;
  if (w < 32 || h < 32) {
    throw new Error("对比图尺寸过小");
  }
  if (preScale > 1.001) {
    const tw = Math.min(4096, Math.max(1, Math.round(w * preScale)));
    const th = Math.max(1, Math.round(h * preScale));
    buf = await sharp(buf)
      .resize(tw, th, { kernel: sharp.kernel.lanczos3 })
      .png()
      .toBuffer();
    meta = await sharp(buf).metadata();
    w = meta.width || 0;
    h = meta.height || 0;
  }
  const n = Math.min(3, Math.max(1, Math.floor(periodCount)));
  const col = Math.min(n - 1, Math.max(0, Math.floor(columnIndex)));
  const colW = Math.floor(w / n);
  const x0 = col * colW;
  const x1 = col === n - 1 ? w : (col + 1) * colW;
  const subW = x1 - x0;
  if (subW < 16) {
    throw new Error("裁切列宽过小");
  }
  const { data, info } = await sharp(buf)
    .extract({ left: x0, top: 0, width: subW, height: h })
    .ensureAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });
  const rgba = Buffer.from(data);
  if (!skipFlood) {
    const tol2 = 42 * 42;
    applyBcdEdgeFloodTransparent(rgba, info.width, info.height, tol2);
  }
  const subH = info.height;
  const box = bcdAlphaBoundingBox(rgba, subW, subH);
  const pad = 6;
  let left = 0;
  let top = 0;
  let extW = subW;
  let extH = subH;
  if (
    box &&
    box.width >= 24 &&
    box.height >= 48 &&
    box.width <= subW &&
    box.height <= subH
  ) {
    left = Math.max(0, box.left - pad);
    top = Math.max(0, box.top - pad);
    extW = Math.min(subW - left, box.width + 2 * pad);
    extH = Math.min(subH - top, box.height + 2 * pad);
  }
  if (extW < 16 || extH < 16) {
    left = 0;
    top = 0;
    extW = subW;
    extH = subH;
  }
  const tightPng = await sharp(rgba, {
    raw: { width: subW, height: subH, channels: 4 },
  })
    .extract({ left, top, width: extW, height: extH })
    .png()
    .toBuffer();
  // 紧裁后保持紧贴人物的宽高比，不再塞进固定 1024×1536 + contain（会在左右留透明）。
  // 仅当超过 1024×1536 时等比缩小（fit: inside）；小于等于则不放大。
  return sharp(tightPng)
    .resize(1024, 1536, {
      fit: "inside",
      withoutEnlargement: true,
    })
    .png()
    .toBuffer();
}

async function bcdExtractHeroPngFromComparisonFile(
  absPngPath,
  periodCount,
  columnIndex,
  opts
) {
  const inputBuf = fs.readFileSync(absPngPath);
  return bcdExtractHeroPngFromComparisonBuffer(
    inputBuf,
    periodCount,
    columnIndex,
    opts
  );
}

async function handleBcdExtractHeroFromComparison(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const body = req.body || {};
    const comparisonImageUrl = safeText(body.comparisonImageUrl || "").trim();
    let periodCount = Number(body.periodCount);
    let columnIndex = Number(body.columnIndex);
    if (!Number.isFinite(periodCount) || periodCount < 1 || periodCount > 3) {
      return res.status(400).json({
        success: false,
        error: "periodCount 应为 1～3",
      });
    }
    periodCount = Math.floor(periodCount);
    if (!Number.isFinite(columnIndex) || columnIndex < 0 || columnIndex >= periodCount) {
      return res.status(400).json({
        success: false,
        error: "columnIndex 与时期数不匹配",
      });
    }
    columnIndex = Math.floor(columnIndex);
    const abs = resolveSafeGeneratedPngPath(comparisonImageUrl);
    if (!abs) {
      return res.status(400).json({
        success: false,
        error: "无效的对比图路径（仅允许 /generated/*.png）",
      });
    }
    const skipFlood =
      body.skipFlood === true ||
      body.skipFlood === "true" ||
      body.skipFlood === 1 ||
      body.skipFlood === "1";
    const preScale = resolveBcdComparisonCropPrescale(body);
    let outBuf;
    try {
      outBuf = await bcdExtractHeroPngFromComparisonFile(
        abs,
        periodCount,
        columnIndex,
        { skipFlood, preScale }
      );
    } catch (e) {
      console.error("[bcd-extract-hero]", e);
      return res.status(500).json({
        success: false,
        error: String(e.message || e) || "裁切去底失败",
      });
    }
    ensureDir(CHAPTER_ILLUSTRATION_GENERATED_DIR);
    const filename = `ill-bcd-hero-crop-${Date.now()}-${crypto.randomBytes(4).toString("hex")}.png`;
    const filePath = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, filename);
    fs.writeFileSync(filePath, outBuf);
    try {
      await ensureGeneratedThumbForFilename(filename, READER_IMAGE_MAX_EDGE);
    } catch (thumbErr) {
      console.warn("[bcd-extract-hero:auto-thumb]", thumbErr?.message || thumbErr);
    }
    res.json({
      success: true,
      imageUrl: `/generated/${filename}`,
      localPath: `public/generated/${filename}`,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      success: false,
      error: err.message || "失败",
    });
  }
}

app.post(
  "/api/admin/bcd-extract-hero-from-comparison",
  handleBcdExtractHeroFromComparison
);

async function handleBcdExtractAllPeriodsFromComparison(req, res) {
  const authed = requireAdminUser(req, res);
  if (!authed) return;
  try {
    const body = req.body || {};
    const comparisonImageUrl = safeText(body.comparisonImageUrl || "").trim();
    let periodCount = Number(body.periodCount);
    if (!Number.isFinite(periodCount) || periodCount < 1 || periodCount > 3) {
      return res.status(400).json({
        success: false,
        error: "periodCount 应为 1～3",
      });
    }
    periodCount = Math.floor(periodCount);
    const abs = resolveSafeGeneratedPngPath(comparisonImageUrl);
    if (!abs) {
      return res.status(400).json({
        success: false,
        error: "无效的对比图路径（仅允许 /generated/*.png）",
      });
    }
    const skipFlood =
      body.skipFlood === true ||
      body.skipFlood === "true" ||
      body.skipFlood === 1 ||
      body.skipFlood === "1";
    const preScale = resolveBcdComparisonCropPrescale(body);
    const imageUrls = [];
    const ts = Date.now();
    const rand = crypto.randomBytes(4).toString("hex");
    try {
      for (let col = 0; col < periodCount; col++) {
        const outBuf = await bcdExtractHeroPngFromComparisonFile(
          abs,
          periodCount,
          col,
          { skipFlood, preScale }
        );
        ensureDir(CHAPTER_ILLUSTRATION_GENERATED_DIR);
        const filename = `ill-bcd-period-${ts}-${col}-${rand}.png`;
        const filePath = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, filename);
        fs.writeFileSync(filePath, outBuf);
        try {
          await ensureGeneratedThumbForFilename(filename, READER_IMAGE_MAX_EDGE);
        } catch (thumbErr) {
          console.warn("[bcd-extract-period:auto-thumb]", thumbErr?.message || thumbErr);
        }
        imageUrls.push(`/generated/${filename}`);
      }
    } catch (e) {
      console.error("[bcd-extract-all-periods]", e);
      return res.status(500).json({
        success: false,
        error: String(e.message || e) || "批量裁切失败",
      });
    }
    res.json({ success: true, imageUrls });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      success: false,
      error: err.message || "失败",
    });
  }
}

app.post(
  "/api/admin/bcd-extract-all-periods-from-comparison",
  handleBcdExtractAllPeriodsFromComparison
);
/** 短路径：个别反代对长 URL 段返回 404（与 sitechrome、genlifestages 同理） */
app.post("/api/admin/bcd-cut-periods", handleBcdExtractAllPeriodsFromComparison);
app.post("/api/admin/bcd-cut-hero", handleBcdExtractHeroFromComparison);

/**
 * 透明 PNG 存盘前：按 Alpha 裁掉四周留白，使画布紧贴人物外轮廓（含左右宽度）。
 * 失败或尺寸异常时返回原 buffer。可调 TRANSPARENT_PNG_TRIM_THRESHOLD（0–99，默认 14）或设 DISABLE_TRANSPARENT_PNG_TRIM=1 关闭。
 */
async function trimTransparentIllustrationPngBuffer(buf) {
  try {
    const sharp = await getSharp();
    const meta = await sharp(buf).metadata();
    const w0 = meta.width || 0;
    const h0 = meta.height || 0;
    if (w0 < 8 || h0 < 8) return buf;
    const thrRaw = Number(process.env.TRANSPARENT_PNG_TRIM_THRESHOLD);
    const threshold = Number.isFinite(thrRaw)
      ? Math.min(99, Math.max(0, thrRaw))
      : 14;
    const trimmed = await sharp(buf)
      .ensureAlpha()
      .trim({ threshold })
      .png()
      .toBuffer();
    const m2 = await sharp(trimmed).metadata();
    const w = m2.width || 0;
    const h = m2.height || 0;
    if (w < 32 || h < 64) return buf;
    if (w > w0 || h > h0) return buf;
    return trimmed;
  } catch (e) {
    console.warn("[trimTransparentIllustrationPngBuffer]", e.message || e);
    return buf;
  }
}

function characterStageRecordForSlot(entry, slotIndex) {
  const si = Math.max(0, Math.min(2, Math.floor(Number(slotIndex) || 0)));
  if (!entry || typeof entry !== "object") return null;
  if (si === 0) return entry;
  const periods = Array.isArray(entry.periods) ? entry.periods : [];
  const p = periods[si - 1];
  return p && typeof p === "object" ? p : null;
}

function resolveCharacterProfileIdentityReference(body) {
  const zhName = safeText(body.characterZhName || body.chineseName || "").slice(0, 32);
  const explicitUrl = safeText(
    body.referenceImageUrl || body.identityReferenceImageUrl || ""
  ).slice(0, 400);
  const explicitCoreEn = safeText(body.identityCoreEn || "").slice(0, 1200);
  const explicitDeltaEn = safeText(body.stageAppearanceDeltaEn || "").slice(0, 1200);
  const explicitTargetStageId = safeText(body.targetStageId || "").slice(0, 64);
  const explicitDerivedFromStageId = safeText(body.derivedFromStageId || "").slice(0, 64);
  const explicitSourceSlot = Number(body.sourceSlotIndex);
  const explicitTargetSlot = Number(body.targetSlotIndex);
  if (!zhName) {
    return {
      zhName: "",
      referenceImageUrl: explicitUrl,
      referenceImagePath: resolveSafeGeneratedPngPath(explicitUrl),
      identityCoreEn: explicitCoreEn,
      stageAppearanceDeltaEn: explicitDeltaEn,
      targetStageId: explicitTargetStageId,
      derivedFromStageId: explicitDerivedFromStageId,
      sourceSlotIndex: Number.isFinite(explicitSourceSlot) ? explicitSourceSlot : null,
      targetSlotIndex: Number.isFinite(explicitTargetSlot) ? explicitTargetSlot : null,
    };
  }
  const profilesRoot = loadCharacterIllustrationProfiles();
  const characters =
    profilesRoot.characters && typeof profilesRoot.characters === "object"
      ? profilesRoot.characters
      : {};
  const entry = characters[zhName];
  if (!entry || typeof entry !== "object") {
    return {
      zhName,
      referenceImageUrl: explicitUrl,
      referenceImagePath: resolveSafeGeneratedPngPath(explicitUrl),
      identityCoreEn: explicitCoreEn,
      stageAppearanceDeltaEn: explicitDeltaEn,
      targetStageId: explicitTargetStageId,
      derivedFromStageId: explicitDerivedFromStageId,
      sourceSlotIndex: Number.isFinite(explicitSourceSlot) ? explicitSourceSlot : null,
      targetSlotIndex: Number.isFinite(explicitTargetSlot) ? explicitTargetSlot : null,
    };
  }

  const sourceSlotIndex = Number.isFinite(explicitSourceSlot)
    ? Math.max(0, Math.min(2, Math.floor(explicitSourceSlot)))
    : null;
  const targetSlotIndex = Number.isFinite(explicitTargetSlot)
    ? Math.max(0, Math.min(2, Math.floor(explicitTargetSlot)))
    : null;
  const sourceStage = sourceSlotIndex == null ? null : characterStageRecordForSlot(entry, sourceSlotIndex);
  const targetStage = targetSlotIndex == null ? null : characterStageRecordForSlot(entry, targetSlotIndex);
  const referenceImageUrl =
    explicitUrl ||
    safeText(sourceStage?.imageUrl || "").slice(0, 400) ||
    safeText(entry.identityReferenceImageUrl || "").slice(0, 400) ||
    safeText(entry.heroImageUrl || "").slice(0, 400) ||
    safeText(entry.imageUrl || "").slice(0, 400) ||
    safeText(entry.comparisonSheetUrl || "").slice(0, 400);
  const identityCoreEn =
    explicitCoreEn ||
    safeText(entry.identityCoreEn || "").slice(0, 1200) ||
    safeText(entry.appearanceEn || "").slice(0, 1200);
  const stageAppearanceDeltaEn =
    explicitDeltaEn ||
    safeText(targetStage?.appearanceDeltaEn || "").slice(0, 1200) ||
    safeText(targetStage?.appearanceEn || "").slice(0, 1200);
  const targetStageId =
    explicitTargetStageId ||
    safeText(targetStage?.id || "").slice(0, 64) ||
    (targetSlotIndex != null ? `slot-${targetSlotIndex}` : "");
  const derivedFromStageId =
    explicitDerivedFromStageId ||
    safeText(sourceStage?.derivedFromStageId || "").slice(0, 64) ||
    (sourceSlotIndex != null ? `slot-${sourceSlotIndex}` : "");
  return {
    zhName,
    englishName: safeText(entry.englishName || "").slice(0, 80),
    referenceImageUrl,
    referenceImagePath: resolveSafeGeneratedPngPath(referenceImageUrl),
    identityCoreEn,
    stageAppearanceDeltaEn,
    targetStageId,
    derivedFromStageId,
    sourceSlotIndex,
    targetSlotIndex,
  };
}

function buildReferenceAwareIllustrationPrompt(basePrompt, identityRef) {
  const prompt = safeText(basePrompt || "");
  if (!identityRef || typeof identityRef !== "object") return prompt;
  const refLines = [];
  if (identityRef.referenceImagePath) {
    refLines.push(
      "IDENTITY REFERENCE (highest priority): Use the attached reference image as the same biblical individual at a different life stage. Preserve the same bone structure, eye spacing, nose, jawline, brows, hairline, and overall face identity."
    );
  }
  const who = safeText(identityRef.englishName || identityRef.zhName || "").trim();
  if (who) {
    refLines.push(`Character identity: ${who}.`);
  }
  if (identityRef.identityCoreEn) {
    refLines.push(
      `Stable identity traits that must NOT change: ${safeText(identityRef.identityCoreEn).trim()}`
    );
  }
  if (identityRef.stageAppearanceDeltaEn) {
    refLines.push(
      `Target life-stage changes for this generation: ${safeText(identityRef.stageAppearanceDeltaEn).trim()}`
    );
  }
  if (identityRef.derivedFromStageId || identityRef.targetStageId) {
    refLines.push(
      `Life-stage transform: source=${safeText(identityRef.derivedFromStageId || "existing").trim()} -> target=${safeText(identityRef.targetStageId || "requested").trim()}. Change age, wrinkles, hair color/length, body maturity, and costume as needed, but never invent a new face.`
    );
  }
  if (!refLines.length) return prompt;
  return `${refLines.join(" ")} ${prompt}`.trim();
}

/** 降低图像安全策略误判：明确教育/端庄语境，避免与「裸露/性感」类提示混淆。 */
const OPENAI_IMAGE_SAFETY_PREFIX =
  "[Educational family-safe historical illustration] AI-generated interpretive art — not a photograph or guaranteed likeness of any historical individual. Wholesome museum-style Bible reference art: every person fully clothed in modest period dress; dignified standing or calm narrative poses; focus on costume, face, and hands. Costume must follow the biblical narrative phase: primeval Adam/Eve (before Cain in Genesis order) in simple animal-hide dress only; from Cain onward match office and era — priests and kings in historically plausible biblical-era garb for their role, not generic identical tunics for everyone. Where faces are clearly visible, prefer calm dignified eye contact toward the viewer when the pose allows (soft direct or gentle three-quarter gaze) so figures feel present alongside text or in display — warm human engagement, never aggressive glaring, never vacant or unfocused eyes. When multiple standing adults appear in one image or roster-style row, preserve natural height differences (adult women typically shorter than adult men of the same setting unless the prompt specifies otherwise); do not stretch every figure to identical silhouette height. Absolutely no nudity, no sensual or romanticized anatomy, no fetish content. ";

/**
 * POST /api/generate-illustration
 * Body: { prompt: string, transparent: boolean, skipTransparentTrim?: boolean, englishName?: string, nameSlot?: string }
 * 人物档（bookId=char）：建议传 englishName + nameSlot（如 p0、hero），文件名 ill-char-{En}-{slot}-{ts}.png 便于检索。
 * Key 与全站一致：环境变量 OPENAI_API_KEY 优先，否则管理后台「系统密钥」。
 * 成功：{ success: true, imageUrl, transparentPng }（transparentPng 为 boolean，表示是否请求透明背景）。
 * 尺寸与 quality：transparent 与 opaque 仅 background 不同，不传 imageSize 时默认竖版 1024×1536（总像素高于 1024²）。
 * 透明图：默认在写入前按 Alpha 裁边（紧贴人物左右与上下）；skipTransparentTrim 或 DISABLE_TRANSPARENT_PNG_TRIM=1 可跳过。
 */
app.post("/api/generate-illustration", async (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const body = req.body || {};
    const bcdNeutral =
      body.bcdNeutralColor === true ||
      body.bcdNeutralColor === "true" ||
      body.bcdNeutralColor === 1;
    const transparent =
      body.transparent === true ||
      body.transparent === "true" ||
      body.transparent === 1 ||
      body.transparent === "1";
    const identityRef = resolveCharacterProfileIdentityReference(body);
    let prompt = buildReferenceAwareIllustrationPrompt(
      safeText(body.prompt || ""),
      identityRef
    );
    if (bcdNeutral) {
      const colorLockTransparentPng =
        transparent &&
        " CUTOUT / TRANSPARENT PNG (critical): No background means no sky bounce or warm ambient fill — models often fake “golden” key on cutouts. Light the figure like a neutral softbox product shot on gray (D65 / ~6500K feel): white and gray hair and beard must read neutral white-to-silver, NOT butter-cream, NOT golden highlights; off-white robes stay neutral paper-white, not ivory-yellow. Browns and earth tones are desaturated neutral brown, NOT orange-umber or honey glaze. No amber rim light, no campfire warmth, no “museum oil” varnish cast over the full costume and visible face and hands. ";
      prompt =
        "COLOR LOCK (highest priority — apply before any artistic or historical style): Neutral daylight white balance (~5200K), like an overcast day or north-window studio — NOT golden hour, NOT sunset warmth. Absolutely NO yellow, amber, gold, or sepia cast on faces, hands, or across the image; NO brown oil-painting glaze or aged varnish look. Whites and linens read as clean neutral white unless the text specifies a dyed color. Complexion (faces and hands only): natural realistic human tones, not orange, not jaundiced, not honey-filtered. " +
        (colorLockTransparentPng || "") +
        prompt +
        " COLOR LOCK (repeat): If the result looks warm-yellow or vintage-brown overall, it is wrong — rebalance to neutral, true-to-life color." +
        (transparent
          ? " FINAL CHECK (transparent PNG): If white hair or pale cloth looks cream or tan on screen, shift white balance cooler — same figure should match sRGB-neutral reference, not a sepia photograph."
          : "");
    }
    if (!prompt.trim()) {
      return res.status(400).json({
        success: false,
        error: "缺少 prompt",
      });
    }
    if (transparent) {
      prompt +=
        " UNIFIED PAINTING + SETTING (mandatory): one continuous artwork with modest biblical environment (ground, distance, air, warm sand or beige atmospheric haze) when needed — NOT sticker cut-outs on flat empty white. Transition to transparency must be SLOW and WIDE: let the SETTING (mist, ground, sky wash) dissolve gradually toward the canvas edges into full alpha so the reader page blends through — opacity must NOT collapse in a tight band hugging figure silhouettes. Avoid a hard horizontal chop through feet, bench, or bodies. No isolated floating texture blobs. Unpainted pixels = alpha 0. Forbid edge-to-edge flat white or flat cream studio fills with no gradient; soft graduated beige/parchment in outer zones for environmental fusion is OK. No decorative frame/mat/card inset.";
    } else {
      prompt +=
        " OPAQUE FULL SCENE (mandatory): one continuous biblical-era illustration with a painted environmental background filling the entire image — ground, sky, architecture, landscape, shadow, or haze. NOT a transparency cutout, NOT empty pure-white (#FFFFFF) studio void as the dominant backdrop. Prefer warm parchment, sand, earth, soft gray-blue sky, or dim interior tones. Bright white is only for intentional light (sun edge, beam, lamp, glory, fire). No decorative frame/mat/card inset.";
    }
    prompt = OPENAI_IMAGE_SAFETY_PREFIX + prompt;

    const apiKey = getCurrentOpenAiApiKey();
    if (!apiKey) {
      return res.status(503).json({
        success: false,
        error:
          "未配置 OpenAI API Key：请在环境变量 OPENAI_API_KEY 或管理后台「系统密钥」中设置后重启服务。",
      });
    }

    const imageClient = new OpenAI({ apiKey });

    let imgResp;
    try {
      const imgSizeRaw = safeText(body.imageSize || "").trim();
      const allowedSizes = new Set([
        "1024x1024",
        "1536x1024",
        "1024x1536",
        "auto",
      ]);
      const size = allowedSizes.has(imgSizeRaw)
        ? imgSizeRaw
        : imgSizeRaw === ""
          ? "1024x1536"
          : "1024x1024";
      const imgQ = safeText(body.imageQuality || "").toLowerCase();
      const quality =
        imgQ === "low" || imgQ === "medium" || imgQ === "high" ? imgQ : "high";
      const imageReq = {
        model: "gpt-image-1",
        prompt,
        size,
        quality,
        background: transparent ? "transparent" : "opaque",
        output_format: "png",
      };
      if (identityRef.referenceImagePath) {
        try {
          imgResp = await imageClient.images.edit({
            ...imageReq,
            image: fs.createReadStream(identityRef.referenceImagePath),
          });
        } catch (editErr) {
          console.warn(
            "[generate-illustration:images.edit:fallback]",
            editErr?.message || editErr
          );
          imgResp = await imageClient.images.generate(imageReq);
        }
      } else {
        imgResp = await imageClient.images.generate(imageReq);
      }
    } catch (apiErr) {
      const rawMsg =
        safeText(apiErr?.message || "") ||
        safeText(apiErr?.error?.message || "") ||
        safeText(
          apiErr?.error && typeof apiErr.error === "object"
            ? apiErr.error.message || apiErr.error.code
            : ""
        ) ||
        "";
      let msg = rawMsg || "OpenAI 图片接口调用失败";
      let httpStatus = 502;
      if (
        /safety_violations|safety system|rejected by the safety|content_policy|content policy/i.test(
          rawMsg
        )
      ) {
        httpStatus = 400;
        const reqM = /\breq_[a-f0-9]{20,}\b/i.exec(rawMsg);
        const sexual = /sexual/i.test(rawMsg);
        msg =
          "图片请求被 OpenAI 安全策略拦截" +
          (sexual ? "（标记为敏感类）" : "") +
          "，多为误判。可尝试：① 精简外观描写，避免涉及裸露、身材曲线、情色联想词；② 女性与未成年人用「端庄长袖长袍、遮盖头发」等中性表述；③ 删除英文描述中的 bare、chest、breast、naked、seductive、virgin（可改为 young unmarried woman）等词后重试。" +
          (reqM ? " 请求 ID：" + reqM[0] + "（可向 help.openai.com 申诉）" : "");
      }
      return res.status(httpStatus).json({ success: false, error: msg });
    }

    const d0 = imgResp?.data?.[0];
    const b64 = d0?.b64_json || d0?.b64Json;
    let buf;
    if (b64) {
      try {
        buf = Buffer.from(b64, "base64");
      } catch {
        return res.status(502).json({
          success: false,
          error: "图像 Base64 解码失败",
        });
      }
    } else if (d0?.url && typeof fetch === "function") {
      try {
        const fr = await fetch(d0.url);
        if (!fr.ok) {
          return res.status(502).json({
            success: false,
            error: "OpenAI 返回的图片 URL 拉取失败 HTTP " + fr.status,
          });
        }
        buf = Buffer.from(await fr.arrayBuffer());
      } catch (fe) {
        const em =
          safeText(fe?.message || "") || "从 OpenAI 图片 URL 下载失败";
        return res.status(502).json({ success: false, error: em });
      }
    } else {
      return res.status(502).json({
        success: false,
        error: "OpenAI 响应中无图像数据（缺少 b64_json / url）",
      });
    }

    const skipTransparentTrim =
      body.skipTransparentTrim === true ||
      body.skipTransparentTrim === "true" ||
      body.skipTransparentTrim === 1 ||
      process.env.DISABLE_TRANSPARENT_PNG_TRIM === "1";
    if (transparent && !skipTransparentTrim) {
      buf = await trimTransparentIllustrationPngBuffer(buf);
    }

    ensureDir(CHAPTER_ILLUSTRATION_GENERATED_DIR);
    const bookIdRaw = safeText(body.bookId || "");
    const chRaw = safeText(body.chapter ?? "");
    const nameEnTok = safeText(body.englishName || "")
      .replace(/[^a-zA-Z0-9_-]/g, "")
      .slice(0, 36);
    const nameSlotTok = safeText(body.nameSlot || "")
      .replace(/[^a-zA-Z0-9_-]/g, "")
      .slice(0, 16);
    let filename;
    if (bookIdRaw === "char" && nameEnTok && nameSlotTok) {
      filename = `ill-char-${nameEnTok}-${nameSlotTok}-${Date.now()}.png`;
    } else if (bookIdRaw && chRaw !== "") {
      const safeBook =
        bookIdRaw.replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 16) || "bk";
      const safeCh = chRaw.replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 48) || "0";
      filename = `ill-${safeBook}-${safeCh}-${Date.now()}.png`;
    } else {
      filename = `gen-${Date.now()}.png`;
    }
    const filePath = path.join(CHAPTER_ILLUSTRATION_GENERATED_DIR, filename);
    fs.writeFileSync(filePath, buf);
    try {
      await ensureGeneratedThumbForFilename(filename, READER_IMAGE_MAX_EDGE);
    } catch (thumbErr) {
      console.warn("[generate-illustration:auto-thumb]", thumbErr?.message || thumbErr);
    }

    const imageUrl = `/generated/${filename}`;
    res.json({
      success: true,
      imageUrl,
      localPath: `public/generated/${filename}`,
      transparentPng: transparent,
      sceneDescription: safeText(body.sceneDescription || ""),
      promptUsed: prompt.slice(0, 500),
      referenceImageUrl: identityRef.referenceImageUrl || "",
      usedReferenceImage: !!identityRef.referenceImagePath,
      targetStageId: identityRef.targetStageId || "",
      derivedFromStageId: identityRef.derivedFromStageId || "",
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      success: false,
      error: err.message || "生成失败",
    });
  }
});

function chapterVideoPosterMulterMiddleware(req, res, next) {
  chapterVideoPosterMulter.single("poster")(req, res, (err) => {
    if (err) {
      const msg =
        err.code === "LIMIT_FILE_SIZE"
          ? "封面图过大（最大 5MB）"
          : String(err.message || "上传失败");
      return res.status(400).json({ error: msg });
    }
    next();
  });
}

function handleChapterVideoPosterUpload(req, res) {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const file = req.file;
    if (!file || !file.path) {
      return res.status(400).json({ error: "请选择封面图（字段名 poster）" });
    }
    const metaObj = parseUploadMetaJson(req);
    const versionId = pickStrPreferFlat(req.body?.version, metaObj?.version);
    const lang = pickStrPreferFlat(req.body?.lang, metaObj?.lang);
    const bookId = pickStrPreferFlat(req.body?.bookId, metaObj?.bookId);
    const chapter = parseNonNegativeChapterInt(
      pickChapterRawForUpload(req, metaObj)
    );
    const id = pickStrPreferFlat(req.body?.id, metaObj?.id);
    if (!versionId || !lang || !bookId || chapter === null || !id) {
      try {
        fs.unlinkSync(file.path);
      } catch {
        /* ignore */
      }
      return res
        .status(400)
        .json({ error: "缺少 version / lang / bookId / chapter / id" });
    }
    if (!isSafeChapterVideoId(id)) {
      try {
        fs.unlinkSync(file.path);
      } catch {
        /* ignore */
      }
      return res.status(400).json({ error: "无效的视频 id" });
    }
    const publishedPath = getPublishedContentFilePath({
      versionId,
      lang,
      bookId,
      chapter,
    });
    let data = readJson(publishedPath, null);
    if ((!data || typeof data !== "object") && chapter === 0) {
      data = ensurePublishedJsonForBookLandingVideos(versionId, lang, bookId);
    }
    if (!data || typeof data !== "object") {
      try {
        fs.unlinkSync(file.path);
      } catch {
        /* ignore */
      }
      return res.status(404).json({ error: "未找到已发布章节" });
    }
    const videos = normalizeChapterVideosForSave(data.chapterVideos);
    const idx = videos.findIndex((v) => String(v.id).toLowerCase() === id.toLowerCase());
    if (idx < 0) {
      try {
        fs.unlinkSync(file.path);
      } catch {
        /* ignore */
      }
      return res.status(404).json({ error: "列表中无此视频" });
    }
    const mime = String(file.mimetype || "").toLowerCase();
    let ext = "jpg";
    if (mime.includes("png")) ext = "png";
    else if (mime.includes("webp")) ext = "webp";
    unlinkChapterVideoPosterFiles(versionId, lang, bookId, chapter, id);
    const dir = getChapterVideosDir(versionId, lang, bookId, chapter);
    fs.mkdirSync(dir, { recursive: true });
    fs.mkdirSync(CHAPTER_VIDEO_UPLOAD_TMP, { recursive: true });
    const dest = path.join(dir, `${String(id).toLowerCase()}.poster.${ext}`);
    moveUploadedFileToFinal(file.path, dest);
    const posterUrl = chapterVideoPosterApiPath(
      versionId,
      lang,
      bookId,
      chapter,
      id
    );
    const posterUpdatedAt = nowIso();
    const nextVideos = videos.map((row, i) =>
      i === idx ? { ...row, posterUrl, posterUpdatedAt } : row
    );
    const merged = { ...data, chapterVideos: nextVideos };
    writeJson(publishedPath, merged);
    invalidateStudyContentCache(versionId, lang, bookId, chapter);
    appendAdminAudit(req, authed, "chapter_video_poster_upload", {
      versionId,
      lang,
      bookId,
      chapter,
      videoId: id,
    });
    res.json({
      ok: true,
      posterUrl,
      posterUpdatedAt,
      chapterVideos: nextVideos,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "封面上传失败" });
  }
}

function handleChapterVideosListGet(req, res) {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const versionId = safeText(req.query.version || "");
    const lang = safeText(req.query.lang || "");
    if (!versionId || !lang) {
      return res.status(400).json({ error: "缺少 version / lang" });
    }
    const items = listChapterVideosOverview(versionId, lang);
    res.json({ ok: true, items });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取视频列表失败" });
  }
}

/* 与 published 子路径并列，便于反代只放行 /api/admin/published/ 时仍能拉列表 */
app.get("/api/admin/published/chapter-videos-list", handleChapterVideosListGet);
app.get("/api/admin/chapter-videos/list", handleChapterVideosListGet);

app.post(
  "/api/admin/published/chapter-video-poster-upload",
  chapterVideoPosterMulterMiddleware,
  handleChapterVideoPosterUpload
);
app.post(
  "/api/admin/chapter-video/poster",
  chapterVideoPosterMulterMiddleware,
  handleChapterVideoPosterUpload
);

app.get("/api/published/chapter-video-poster", (req, res) => {
  try {
    const versionId = safeText(req.query.version || "");
    const lang = safeText(req.query.lang || "");
    const bookId = safeText(req.query.bookId || "");
    const chapter = parseNonNegativeChapterInt(req.query.chapter);
    const id = safeText(req.query.id || "");
    if (!versionId || !lang || !bookId || chapter === null || !id) {
      return res
        .status(400)
        .json({ error: "缺少 version / lang / bookId / chapter / id" });
    }
    const p = resolveChapterVideoPosterFilePath(
      versionId,
      lang,
      bookId,
      chapter,
      id
    );
    if (!p) {
      return res.status(404).json({ error: "封面不存在" });
    }
    const low = p.toLowerCase();
    const contentType = low.endsWith(".png")
      ? "image/png"
      : low.endsWith(".webp")
        ? "image/webp"
        : "image/jpeg";
    res.setHeader("Content-Type", contentType);
    res.setHeader("Cache-Control", "public, max-age=86400");
    res.sendFile(path.resolve(p), (err) => {
      if (err && !res.headersSent) res.status(500).end();
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取封面失败" });
  }
});

app.get("/api/global-favorites", (req, res) => {
  try {
    const type = safeText(req.query.type || "verse");
    const limit = Math.max(1, Math.min(200, toSafeNumber(req.query.limit, 50)));
    const db = loadGlobalFavorites();
    const rows = Object.values(db.items || {})
      .filter((item) => safeText(item?.type) === type && toSafeNumber(item?.count, 0) > 0)
      .sort((a, b) => {
        const countDiff = toSafeNumber(b?.count, 0) - toSafeNumber(a?.count, 0);
        if (countDiff !== 0) return countDiff;
        return String(b?.updatedAt || "").localeCompare(String(a?.updatedAt || ""));
      })
      .slice(0, limit);
    res.json({ items: rows });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取热门收藏失败" });
  }
});

app.get("/api/divine-speech-verses", (req, res) => {
  try {
    const enabledVersions = getEnabledScriptureVersions().filter(
      (x) => x.uiEnabled !== false && x.scriptureEnabled !== false
    );
    const versionId =
      safeText(req.query.versionId || req.query.version) ||
      safeText(enabledVersions[0]?.id);
    if (!versionId) {
      return res.status(400).json({ error: "缺少 versionId" });
    }
    const limit = Math.max(1, Math.min(1200, toSafeNumber(req.query.limit, 500)));

    const items = [];
    const books = flattenBooks();
    for (const book of books) {
      const chapterCount = Number(book.chapters || 0);
      for (let chapter = 1; chapter <= chapterCount; chapter += 1) {
        const rows = getScriptureRowsForVersion({
          scriptureVersionId: versionId,
          bookId: book.bookId,
          chapter,
        });
        const divineSet = buildDivineSpeechVerseSetFromRows(rows);
        for (const row of rows) {
          const verse = Number(row?.verse || 0);
          const text = safeText(row?.text || "");
          if (!verse || !text) continue;
          if (!divineSet.has(verse)) continue;
          items.push({
            key: `${versionId}|${book.bookId}|${chapter}|${verse}`,
            bookId: book.bookId,
            chapter,
            verse,
            title: `${book.bookCn || book.bookEn || book.bookId} ${chapter}:${verse}`,
            content: text,
            versionId,
          });
          if (items.length >= limit) break;
        }
        if (items.length >= limit) break;
      }
      if (items.length >= limit) break;
    }

    res.json({ items });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "提取主说话语失败" });
  }
});

app.get("/api/community-articles", (req, res) => {
  try {
    const columnId = safeText(req.query.columnId || "");
    const db = loadCommunityArticles();
    let items = (db.items || []).slice();
    if (columnId) {
      items = items.filter((x) => safeText(x?.columnId) === columnId);
    }
    items.sort((a, b) =>
      String(b?.createdAt || "").localeCompare(String(a?.createdAt || ""))
    );
    res.json({ items });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取社区文章失败" });
  }
});

/** 公开：全站顶栏 / 底栏配置（无鉴权） */
function handleSiteChromePublicGet(_req, res) {
  try {
    res.set(
      "Cache-Control",
      "private, no-store, no-cache, max-age=0, must-revalidate"
    );
    res.json(loadSiteChrome());
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取失败" });
  }
}
app.get("/api/site-chrome", handleSiteChromePublicGet);
/** 扁平别名：个别反代或旧规则对带连字符 path 返回 404 */
app.get("/api/sitechrome", handleSiteChromePublicGet);

function handleSiteChromeAdminGet(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    res.set("Cache-Control", "private, no-store");
    res.json(loadSiteChrome());
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取失败" });
  }
}

function handleSiteChromeAdminPost(req, res) {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const saved = saveSiteChromeFromBody(req.body || {});
    appendAdminAudit(req, authed, "site_chrome_save", {
      updatedAt: saved.updatedAt,
    });
    res.set("Cache-Control", "private, no-store");
    res.json({ ok: true, ...saved });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "保存失败" });
  }
}

app.get("/api/admin/site-chrome", handleSiteChromeAdminGet);
app.post("/api/admin/site-chrome", handleSiteChromeAdminPost);
app.get("/api/admin/site-chrome-config", handleSiteChromeAdminGet);
app.post("/api/admin/site-chrome-config", handleSiteChromeAdminPost);
app.get("/api/admin/sitechrome", handleSiteChromeAdminGet);
app.post("/api/admin/sitechrome", handleSiteChromeAdminPost);

app.post("/api/analytics/collect", handleAnalyticsCollectPost);
app.get("/api/admin/analytics/overview", handleAdminAnalyticsOverviewGet);
app.get("/api/admin/analytics/users", handleAdminAnalyticsUsersGet);

function handleSiteSeoPublicGet(_req, res) {
  try {
    res.set(
      "Cache-Control",
      "private, no-store, no-cache, max-age=0, must-revalidate"
    );
    res.json(loadSiteSeo());
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取失败" });
  }
}

/** 公开：首页 / 宣传页 SEO 文案（无鉴权；由静态页 seo-apply.js 拉取） */
app.get("/api/site-seo", handleSiteSeoPublicGet);
/** 扁平别名：个别反代对带连字符 path 返回 404（与 /api/sitechrome 同理） */
app.get("/api/siteseo", handleSiteSeoPublicGet);

app.get("/api/admin/site-seo", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    res.json(loadSiteSeo());
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取失败" });
  }
});

app.post("/api/admin/site-seo", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const saved = saveSiteSeoFromBody(req.body || {});
    appendAdminAudit(req, authed, "site_seo_save", {
      updatedAt: saved.updatedAt,
    });
    res.json({ ok: true, ...saved });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "保存失败" });
  }
});

/** 公开：宣传页 Markdown 正文（无鉴权） */
app.get("/api/promo-page", (_req, res) => {
  try {
    res.set(
      "Cache-Control",
      "private, no-store, no-cache, max-age=0, must-revalidate"
    );
    const p = loadPromoPagePayload();
    res.json({
      markdown: p.markdown,
      customCss: p.customCss,
      updatedAt: p.updatedAt,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取宣传页失败" });
  }
});

/** 管理员：读取宣传页 Markdown（与公开接口同源，便于编辑） */
app.get("/api/admin/promo-page", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    res.set(
      "Cache-Control",
      "private, no-store, no-cache, max-age=0, must-revalidate"
    );
    const p = loadPromoPagePayload();
    res.json({
      markdown: p.markdown,
      customCss: p.customCss,
      updatedAt: p.updatedAt,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取宣传页失败" });
  }
});

/** 管理员：仅保存中间区自定义 CSS（绝不读取或改写 markdown，避免误清空正文） */
app.post("/api/admin/promo-page/custom-css", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const prev = loadPromoPagePayload();
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const has = Object.prototype.hasOwnProperty;
    if (!has.call(body, "customCss") || typeof body.customCss !== "string") {
      res.status(400).json({ error: "请求体须包含字符串字段 customCss" });
      return;
    }
    let customCss = body.customCss;
    if (customCss.length > PROMO_PAGE_MAX_CUSTOM_CSS) {
      customCss = customCss.slice(0, PROMO_PAGE_MAX_CUSTOM_CSS);
    }
    const payload = writePromoPageRecord({
      markdown: prev.markdown,
      customCss,
    });
    appendAdminAudit(req, authed, "promo_page_css_save", {
      bytes: payload.markdown.length,
      cssBytes: payload.customCss.length,
    });
    res.json({ ok: true, updatedAt: payload.updatedAt });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存样式失败" });
  }
});

/** 管理员：保存宣传页 Markdown（customCss 保持磁盘原样） */
app.post("/api/admin/promo-page", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const prev = loadPromoPagePayload();
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const has = Object.prototype.hasOwnProperty;
    if (!has.call(body, "markdown") || typeof body.markdown !== "string") {
      res.status(400).json({ error: "请求体须包含字符串字段 markdown" });
      return;
    }
    let markdown = body.markdown;
    if (markdown.length > PROMO_PAGE_MAX_MARKDOWN) {
      markdown = markdown.slice(0, PROMO_PAGE_MAX_MARKDOWN);
    }
    const payload = writePromoPageRecord({
      markdown,
      customCss: prev.customCss,
    });
    appendAdminAudit(req, authed, "promo_page_save", {
      bytes: payload.markdown.length,
      cssBytes: payload.customCss.length,
    });
    res.json({ ok: true, updatedAt: payload.updatedAt });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存宣传页失败" });
  }
});

/** 管理员：列出社区文章（含正文预览） */
app.get("/api/admin/community-articles", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const db = loadCommunityArticles();
    let items = (db.items || []).slice();
    items.sort((a, b) =>
      String(b?.createdAt || "").localeCompare(String(a?.createdAt || ""))
    );
    res.json({
      items: items.map((x) => ({
        id: safeText(x?.id || ""),
        columnId: safeText(x?.columnId || ""),
        title: safeText(x?.title || ""),
        authorId: safeText(x?.authorId || ""),
        authorName: safeText(x?.authorName || ""),
        createdAt: safeText(x?.createdAt || ""),
        bodyPreview: safeText(x?.body || "").slice(0, 280),
      })),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取社区文章失败" });
  }
});

/** 管理员：删除一篇社区文章 */
app.delete("/api/admin/community-articles/:id", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const id = safeText(req.params.id || "");
    if (!id) return res.status(400).json({ error: "缺少文章 id" });
    const db = loadCommunityArticles();
    const prevLen = (db.items || []).length;
    const items = (db.items || []).filter((x) => safeText(x?.id || "") !== id);
    if (items.length === prevLen) {
      return res.status(404).json({ error: "未找到该文章" });
    }
    saveCommunityArticles({ items });
    appendAdminAudit(req, authed, "community_article_delete", { id });
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "删除失败" });
  }
});

app.post("/api/auth/register", (req, res) => {
  try {
    const name = safeText(req.body?.name || "");
    const email = safeText(req.body?.email || "").toLowerCase();
    const password = safeText(req.body?.password || "");
    if (!name) return res.status(400).json({ error: "缺少用户名" });
    if (!email || !email.includes("@")) {
      return res.status(400).json({ error: "邮箱格式不正确" });
    }
    if (password.length < 6) {
      return res.status(400).json({ error: "密码至少 6 位" });
    }

    const exists = authDb
      .prepare("SELECT id FROM users WHERE email = ? LIMIT 1")
      .get(email);
    if (exists) {
      return res.status(400).json({ error: "该邮箱已注册" });
    }

    const user = {
      id: `u_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      name,
      email,
      passwordHash: hashPassword(password),
      createdAt: nowIso(),
    };
    authDb
      .prepare(
        "INSERT INTO users (id, name, email, password_hash, created_at, is_admin, admin_role) VALUES (?, ?, ?, ?, ?, 0, '')"
      )
      .run(user.id, user.name, user.email, user.passwordHash, user.createdAt);
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "注册失败" });
  }
});

app.post("/api/auth/login", (req, res) => {
  try {
    const email = safeText(req.body?.email || "").toLowerCase();
    const password = safeText(req.body?.password || "");
    if (!email || !password) {
      return res.status(400).json({ error: "缺少邮箱或密码" });
    }
    const user = authDb
      .prepare(
        "SELECT id, name, email, password_hash, is_admin, admin_role FROM users WHERE email = ? LIMIT 1"
      )
      .get(email);
    if (!user || !verifyPassword(password, user.password_hash)) {
      return res.status(401).json({ error: "邮箱或密码错误" });
    }
    if (
      user.password_hash &&
      !String(user.password_hash).startsWith("$2a$") &&
      !String(user.password_hash).startsWith("$2b$") &&
      !String(user.password_hash).startsWith("$2y$")
    ) {
      authDb
        .prepare("UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?")
        .run(hashPassword(password), nowIso(), user.id);
    }
    const token = createUserToken();
    const createdAt = nowIso();
    const expiresAt = new Date(Date.now() + USER_SESSION_TTL_MS).toISOString();
    const meta = readClientMeta(req);
    authDb.prepare("DELETE FROM user_sessions WHERE user_id = ?").run(user.id);
    authDb
      .prepare(
        "INSERT INTO user_sessions (token, user_id, created_at, expires_at, ip_hash, user_agent, device_id) VALUES (?, ?, ?, ?, ?, ?, ?)"
      )
      .run(
        token,
        user.id,
        createdAt,
        expiresAt,
        meta.ipHash,
        meta.userAgent,
        meta.deviceId
      );
    const role = normalizeAdminRole(user.admin_role || "");
    res.json({
      ok: true,
      token,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        isAdmin: Boolean(Number(user.is_admin || 0) === 1 || role),
        adminRole: role,
      },
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "登录失败" });
  }
});

app.get("/api/auth/me", (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) return res.status(401).json({ error: "未登录" });
    const adminRole = normalizeAdminRole(authed.adminRole || "");
    res.json({
      user: {
        id: authed.id,
        name: authed.name,
        email: authed.email,
        adminRole,
        isAdmin: Boolean(authed.isAdmin === true || adminRole),
        userLevel: getCommunityUserLevelFromSubmissions(authed.id, authed.email),
        totalOnlineSeconds: Number(authed.totalOnlineSeconds || 0),
        colorThemeId: safeText(authed.colorThemeId || ""),
      },
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取登录态失败" });
  }
});

/** 登录用户前台在线时长累积（心跳，约每 45s 一次） */
app.post("/api/user/online/pulse", (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) return res.status(401).json({ error: "请先登录" });
    const raw = Number(req.body?.seconds);
    let chunk = Number.isFinite(raw) ? Math.floor(raw) : 45;
    chunk = Math.max(1, Math.min(120, chunk));
    const rl = checkWriteRateLimit({
      req,
      actionKey: `online_pulse:${authed.id}`,
      limit: 80,
      windowMs: 60000,
    });
    if (!rl.ok) {
      return res.status(429).json({
        error: "请求过于频繁",
        retryAfterSec: rl.retryAfterSec,
      });
    }
    const row = authDb
      .prepare(
        "SELECT COALESCE(online_seconds_total, 0) AS t FROM users WHERE id = ? LIMIT 1"
      )
      .get(authed.id);
    const prev = Number(row?.t || 0);
    const cap = 86400 * 365 * 80;
    const next = Math.min(prev + chunk, cap);
    authDb
      .prepare("UPDATE users SET online_seconds_total = ?, updated_at = ? WHERE id = ?")
      .run(next, nowIso(), authed.id);
    res.json({ ok: true, totalOnlineSeconds: next, addedSeconds: chunk });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "上报失败" });
  }
});

app.post("/api/auth/logout", (req, res) => {
  try {
    const token = getAuthTokenFromReq(req);
    if (!token) return res.json({ ok: true });
    authDb.prepare("DELETE FROM user_sessions WHERE token = ?").run(token);
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "退出失败" });
  }
});

app.post("/api/auth/profile/update", (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) return res.status(401).json({ error: "未登录" });

    const name = safeText(req.body?.name || "");
    const email = safeText(req.body?.email || "").toLowerCase();
    if (!name) return res.status(400).json({ error: "昵称不能为空" });
    if (!email || !email.includes("@")) {
      return res.status(400).json({ error: "邮箱格式不正确" });
    }

    const conflict = authDb
      .prepare("SELECT id FROM users WHERE email = ? AND id <> ? LIMIT 1")
      .get(email, authed.id);
    if (conflict) return res.status(400).json({ error: "邮箱已被占用" });
    authDb
      .prepare("UPDATE users SET name = ?, email = ?, updated_at = ? WHERE id = ?")
      .run(name, email, nowIso(), authed.id);
    res.json({ ok: true, user: { id: authed.id, name, email } });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "更新用户信息失败" });
  }
});

app.post("/api/auth/password/update", (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) return res.status(401).json({ error: "未登录" });
    const currentPassword = safeText(req.body?.currentPassword || "");
    const newPassword = safeText(req.body?.newPassword || "");
    if (!currentPassword || !newPassword) {
      return res.status(400).json({ error: "缺少当前密码或新密码" });
    }
    if (newPassword.length < 6) {
      return res.status(400).json({ error: "新密码至少 6 位" });
    }

    const me = authDb
      .prepare("SELECT id, password_hash FROM users WHERE id = ? LIMIT 1")
      .get(authed.id);
    if (!me) return res.status(404).json({ error: "用户不存在" });
    if (!verifyPassword(currentPassword, me.password_hash)) {
      return res.status(400).json({ error: "当前密码不正确" });
    }
    authDb
      .prepare("UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?")
      .run(hashPassword(newPassword), nowIso(), authed.id);
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "修改密码失败" });
  }
});

app.post("/api/global-favorites/toggle", (req, res) => {
  try {
    const rate = checkWriteRateLimit({
      req,
      actionKey: "favorite_toggle",
      limit: 60,
      windowMs: 60 * 1000,
    });
    if (!rate.ok) {
      return res.status(429).json({
        error: `操作过于频繁，请 ${rate.retryAfterSec}s 后重试`,
      });
    }

    const body = req.body || {};
    const action = safeText(body.action);
    const type = safeText(body.type);
    const key = safeText(body.key);
    if (!["add", "remove"].includes(action)) {
      return res.status(400).json({ error: "action 必须是 add 或 remove" });
    }
    if (!["verse", "question"].includes(type)) {
      return res.status(400).json({ error: "type 必须是 verse 或 question" });
    }
    if (!key) {
      return res.status(400).json({ error: "缺少 key" });
    }
    const dedupeOk = checkWriteDedupe({
      dedupeKey: `favorite_toggle:${getClientIp(req)}:${action}:${type}:${key}`,
      ttlMs: 2000,
    });
    if (!dedupeOk) {
      return res.json({ ok: true, deduped: true });
    }

    const db = loadGlobalFavorites();
    const map = db.items || {};
    const now = nowIso();
    const prev = map[key] || {
      key,
      type,
      count: 0,
      createdAt: now,
      updatedAt: now,
    };

    const nextCount =
      action === "add"
        ? toSafeNumber(prev.count, 0) + 1
        : Math.max(0, toSafeNumber(prev.count, 0) - 1);

    if (nextCount <= 0) {
      delete map[key];
    } else {
      map[key] = {
        ...prev,
        key,
        type,
        bookId: safeText(body.bookId || prev.bookId || ""),
        chapter: toSafeNumber(body.chapter, toSafeNumber(prev.chapter, 0)),
        verse: toSafeNumber(body.verse, toSafeNumber(prev.verse, 0)),
        title: safeText(body.title || prev.title || ""),
        content: safeText(body.content || prev.content || ""),
        contentVersion: safeText(body.contentVersion || prev.contentVersion || ""),
        contentLang: safeText(body.contentLang || prev.contentLang || ""),
        count: nextCount,
        createdAt: prev.createdAt || now,
        updatedAt: now,
      };
    }

    db.items = map;
    saveGlobalFavorites(db);
    res.json({ ok: true, count: nextCount });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "更新热门收藏失败" });
  }
});

app.post("/api/questions/submit", (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) {
      return res.status(401).json({ error: "请先登录后再提交问题" });
    }
    const rate = checkWriteRateLimit({
      req,
      actionKey: "question_submit",
      limit: 12,
      windowMs: 60 * 1000,
    });
    if (!rate.ok) {
      return res.status(429).json({
        error: `提交过于频繁，请 ${rate.retryAfterSec}s 后重试`,
      });
    }

    const body = req.body || {};
    const questionText = safeText(body.questionText);
    if (!questionText || questionText.length < 4) {
      return res.status(400).json({ error: "问题内容至少 4 个字" });
    }
    const dedupeOk = checkWriteDedupe({
      dedupeKey: `question_submit:${getClientIp(req)}:${safeText(
        body.bookId
      )}:${toSafeNumber(body.chapter, 0)}:${questionText}`,
      ttlMs: 10000,
    });
    if (!dedupeOk) {
      return res.json({ ok: true, deduped: true });
    }

    const db = loadQuestionSubmissions();
    const moderation = assessQuestionSubmissionRisk({
      questionText,
      authed,
      req,
      db,
      bookId: body.bookId,
      chapter: body.chapter,
    });

    const item = {
      id: `q_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      questionText,
      note: safeText(body.note || ""),
      bookId: safeText(body.bookId || ""),
      chapter: toSafeNumber(body.chapter, 0),
      rangeStart: toSafeNumber(body.rangeStart, 0),
      rangeEnd: toSafeNumber(body.rangeEnd, 0),
      contentVersion: safeText(body.contentVersion || ""),
      contentLang: safeText(body.contentLang || ""),
      userId: authed.id,
      userName: authed.name,
      userEmail: authed.email,
      ipHash: "",
      userAgent: "",
      deviceId: "",
      status: moderation.status,
      moderationReasons: moderation.reasons,
      reviewedAt: moderation.autoReviewed ? nowIso() : "",
      reviewedBy: moderation.autoReviewed ? "system" : "",
      reviewedByName: moderation.autoReviewed ? "系统风控" : "",
      createdAt: nowIso(),
    };
    const clientMeta = readClientMeta(req);
    item.ipHash = clientMeta.ipHash;
    item.userAgent = clientMeta.userAgent;
    item.deviceId = clientMeta.deviceId;

    db.items = [item, ...(db.items || [])];
    saveQuestionSubmissions(db);
    res.json({
      ok: true,
      id: item.id,
      status: item.status,
      moderationReasons: item.moderationReasons || [],
      message: moderation.userMessage,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "提交失败" });
  }
});

app.get("/api/questions/approved", (req, res) => {
  try {
    const bookId = safeText(req.query.bookId || "");
    const chapter = toSafeNumber(req.query.chapter, 0);
    const db = loadQuestionSubmissions();
    const approvedAll = (db.items || []).filter(
      (x) => safeText(x.status) === "approved"
    );
    const approvedCountByUser = new Map();
    approvedAll.forEach((x) => {
      const uid = safeText(x.userId || x.userEmail || "");
      if (!uid) return;
      approvedCountByUser.set(uid, (approvedCountByUser.get(uid) || 0) + 1);
    });
    const items = (db.items || [])
      .filter((x) => safeText(x.status) === "approved")
      .filter((x) => (bookId ? safeText(x.bookId) === bookId : true))
      .filter((x) =>
        bookId
          ? toSafeNumber(x.chapter, 0) === chapter
          : chapter > 0
            ? toSafeNumber(x.chapter, 0) === chapter
            : true
      )
      .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")))
      .map((x) => {
        const uid = safeText(x.userId || x.userEmail || "");
        const approvedCount = Number(approvedCountByUser.get(uid) || 1);
        const level = Math.max(1, Math.min(12, Math.floor((approvedCount - 1) / 3) + 1));
        return {
          ...x,
          userNickname: safeText(x.userName || ""),
          userLevel: level,
          replies: Array.isArray(x.replies)
            ? x.replies
                .map((reply) => ({
                  id: safeText(reply?.id || ""),
                  questionId: safeText(reply?.questionId || x.id || ""),
                  replyText: safeText(reply?.replyText || ""),
                  userName: safeText(reply?.userName || ""),
                  createdAt: safeText(reply?.createdAt || ""),
                }))
                .filter((reply) => reply.id && reply.replyText)
                .sort((a, b) => String(a.createdAt || "").localeCompare(String(b.createdAt || "")))
            : [],
        };
      });
    res.json({ items });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取已审核问题失败" });
  }
});

app.post("/api/questions/reply", (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) {
      return res.status(401).json({ error: "请先登录后再回复" });
    }
    const rate = checkWriteRateLimit({
      req,
      actionKey: "question_reply",
      limit: 20,
      windowMs: 60 * 1000,
    });
    if (!rate.ok) {
      return res.status(429).json({
        error: `回复过于频繁，请 ${rate.retryAfterSec}s 后重试`,
      });
    }

    const questionId = safeText(req.body?.questionId || "");
    const replyText = safeText(req.body?.replyText || "");
    if (!questionId) {
      return res.status(400).json({ error: "缺少问题编号" });
    }
    if (!replyText || replyText.length < 2) {
      return res.status(400).json({ error: "回复内容至少 2 个字" });
    }

    const dedupeOk = checkWriteDedupe({
      dedupeKey: `question_reply:${authed.id}:${questionId}:${replyText}`,
      ttlMs: 5000,
    });
    if (!dedupeOk) {
      return res.json({ ok: true, deduped: true });
    }

    const db = loadQuestionSubmissions();
    const target = (db.items || []).find((x) => safeText(x.id || "") === questionId);
    if (!target) {
      return res.status(404).json({ error: "问题不存在" });
    }
    if (safeText(target.status || "") !== "approved") {
      return res.status(400).json({ error: "仅可回复已采纳问题" });
    }

    const reply = {
      id: `qr_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      questionId,
      replyText,
      userId: safeText(authed.id || ""),
      userName: safeText(authed.name || ""),
      userEmail: safeText(authed.email || ""),
      createdAt: nowIso(),
    };
    if (!Array.isArray(target.replies)) target.replies = [];
    target.replies.push(reply);
    saveQuestionSubmissions(db);
    res.json({ ok: true, reply });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "回复失败" });
  }
});

app.post("/api/question-corrections/submit", (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) return res.status(401).json({ error: "请先登录" });

    const body = req.body || {};
    const targetType = safeText(body.targetType || "");
    const proposedText = safeText(body.proposedText || "");
    const originalText = safeText(body.originalText || "");

    if (!["preset", "approved"].includes(targetType)) {
      return res.status(400).json({ error: "targetType 无效" });
    }
    if (!proposedText || proposedText.length < 2) {
      return res.status(400).json({ error: "纠错内容至少 2 个字" });
    }
    if (proposedText.length > 4000) {
      return res.status(400).json({ error: "内容过长" });
    }
    if (!originalText || originalText.length < 2) {
      return res.status(400).json({ error: "请提供原文" });
    }

    const rate = checkWriteRateLimit({
      req,
      actionKey: "question_correction_submit",
      limit: 20,
      windowMs: 60 * 1000,
    });
    if (!rate.ok) {
      return res.status(429).json({
        error: `操作过于频繁，请 ${rate.retryAfterSec}s 后重试`,
      });
    }

    const isQian = normalizeAdminRole(authed.adminRole || "") === "qianfuzhang";

    if (targetType === "preset") {
      const bookId = safeText(body.bookId || "");
      const chapter = toSafeNumber(body.chapter, 0);
      const contentVersion = safeText(body.contentVersion || "");
      const contentLang = safeText(body.contentLang || "");
      const rangeStart = toSafeNumber(body.rangeStart, 0);
      const rangeEnd = toSafeNumber(body.rangeEnd, 0);
      const segmentTitle = safeText(body.segmentTitle || "");
      const questionIndex = toSafeNumber(body.questionIndex, 0);
      if (!bookId || !chapter || !contentVersion || !contentLang) {
        return res.status(400).json({ error: "缺少书籍或版本信息" });
      }
      const cur = readCurrentPresetQuestionTextFromPublished({
        bookId,
        chapter,
        contentVersion,
        contentLang,
        rangeStart,
        rangeEnd,
        segmentTitle,
        questionIndex,
      });
      if (cur.error) return res.status(400).json({ error: cur.error });
      if (safeText(cur.text) !== safeText(originalText)) {
        return res.status(409).json({ error: "原文已变更，请刷新页面后重试" });
      }
      const stableKey = stablePresetCorrectionKey(
        bookId,
        chapter,
        contentVersion,
        contentLang,
        rangeStart,
        rangeEnd,
        segmentTitle,
        questionIndex
      );

      const cdb = loadQuestionCorrections();
      const rowBase = {
        targetType: "preset",
        stableKey,
        bookId,
        chapter,
        contentVersion,
        contentLang,
        rangeStart,
        rangeEnd,
        segmentTitle,
        questionIndex,
        originalText,
        proposedText,
        submitterId: authed.id,
        submitterName: authed.name,
        submitterEmail: authed.email,
      };

      if (isQian) {
        const row = {
          ...rowBase,
          id: `qcr_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
          status: "approved",
          createdAt: nowIso(),
          reviewedAt: nowIso(),
          reviewedById: authed.id,
          reviewedByName: authed.name,
        };
        cdb.items = [row, ...(cdb.items || [])];
        saveQuestionCorrections(cdb);
        invalidateStudyContentCache(contentVersion, contentLang, bookId, chapter);
        appendAdminAudit(req, authed, "question_correction_apply", {
          id: row.id,
          stableKey,
          targetType: "preset",
        });
        return res.json({ ok: true, status: "approved", applied: true });
      }

      const row = {
        ...rowBase,
        id: `qcr_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        status: "pending",
        createdAt: nowIso(),
      };
      cdb.items = [row, ...(cdb.items || [])];
      saveQuestionCorrections(cdb);
      return res.json({ ok: true, status: "pending" });
    }

    const questionId = safeText(body.questionId || "");
    if (!questionId) return res.status(400).json({ error: "缺少问题编号" });
    const qdb = loadQuestionSubmissions();
    const target = (qdb.items || []).find((x) => safeText(x.id) === questionId);
    if (!target) return res.status(404).json({ error: "问题不存在" });
    if (safeText(target.status) !== "approved") {
      return res.status(400).json({ error: "仅可对已采纳问题纠错" });
    }
    if (safeText(target.questionText) !== safeText(originalText)) {
      return res.status(409).json({ error: "原文已变更，请刷新后重试" });
    }

    if (isQian) {
      const prev = String(target.questionText || "").slice(0, 120);
      target.questionText = proposedText;
      target.adminTextEditedAt = nowIso();
      target.adminTextEditedBy = authed.id;
      target.adminTextEditedByName = authed.name;
      saveQuestionSubmissions(qdb);
      appendAdminAudit(req, authed, "question_correction_apply", {
        questionId,
        targetType: "approved",
        prevPreview: prev,
      });
      return res.json({ ok: true, status: "approved", applied: true });
    }

    const stableKey = `qapproved_${questionId}`;
    const cdb = loadQuestionCorrections();
    const row = {
      id: `qcr_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      targetType: "approved",
      stableKey,
      questionId,
      bookId: safeText(target.bookId || ""),
      chapter: toSafeNumber(target.chapter, 0),
      contentVersion: safeText(target.contentVersion || ""),
      contentLang: safeText(target.contentLang || ""),
      originalText,
      proposedText,
      status: "pending",
      createdAt: nowIso(),
      submitterId: authed.id,
      submitterName: authed.name,
      submitterEmail: authed.email,
    };
    cdb.items = [row, ...(cdb.items || [])];
    saveQuestionCorrections(cdb);
    return res.json({ ok: true, status: "pending" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "提交失败" });
  }
});

app.get("/api/admin/question-corrections", (req, res) => {
  try {
    const authed = requirePermission(req, res, "review_questions");
    if (!authed) return;
    const status = safeText(req.query.status || "pending");
    const cdb = loadQuestionCorrections();
    const items = (cdb.items || [])
      .filter((x) => (status === "all" ? true : safeText(x.status) === status))
      .sort((a, b) =>
        String(b.createdAt || "").localeCompare(String(a.createdAt || ""))
      );
    res.json({ items });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取失败" });
  }
});

app.post("/api/admin/question-corrections/review", (req, res) => {
  try {
    const authed = requirePermission(req, res, "review_questions");
    if (!authed) return;
    const id = safeText(req.body?.id || "");
    const action = safeText(req.body?.action || "");
    if (!id || !["approve", "reject"].includes(action)) {
      return res.status(400).json({ error: "参数无效" });
    }
    const cdb = loadQuestionCorrections();
    const row = (cdb.items || []).find((x) => safeText(x.id) === id);
    if (!row) return res.status(404).json({ error: "记录不存在" });
    if (safeText(row.status) !== "pending") {
      return res.status(400).json({ error: "该记录已处理" });
    }

    if (action === "reject") {
      row.status = "rejected";
      row.reviewedAt = nowIso();
      row.reviewedById = authed.id;
      row.reviewedByName = authed.name;
      saveQuestionCorrections(cdb);
      appendAdminAudit(req, authed, "question_correction_review", {
        id,
        action: "reject",
      });
      return res.json({ ok: true });
    }

    if (safeText(row.targetType) === "preset") {
      const cur = readCurrentPresetQuestionTextFromPublished(row);
      if (cur.error) return res.status(400).json({ error: cur.error });
      if (safeText(cur.text) !== safeText(row.originalText)) {
        return res.status(409).json({ error: "原文已变更，无法采纳此纠错" });
      }
      row.status = "approved";
      row.reviewedAt = nowIso();
      row.reviewedById = authed.id;
      row.reviewedByName = authed.name;
      saveQuestionCorrections(cdb);
      invalidateStudyContentCache(
        row.contentVersion,
        row.contentLang,
        row.bookId,
        row.chapter
      );
      appendAdminAudit(req, authed, "question_correction_review", {
        id,
        action: "approve",
        targetType: "preset",
      });
      return res.json({ ok: true });
    }

    if (safeText(row.targetType) === "approved") {
      const qdb = loadQuestionSubmissions();
      const t = (qdb.items || []).find(
        (x) => safeText(x.id) === safeText(row.questionId)
      );
      if (!t) return res.status(404).json({ error: "目标问题不存在" });
      if (safeText(t.status) !== "approved") {
        return res.status(400).json({ error: "目标问题已不是已采纳状态" });
      }
      if (safeText(t.questionText) !== safeText(row.originalText)) {
        return res.status(409).json({ error: "原文已变更，无法采纳此纠错" });
      }
      t.questionText = safeText(row.proposedText);
      t.adminTextEditedAt = nowIso();
      t.adminTextEditedBy = authed.id;
      t.adminTextEditedByName = authed.name;
      saveQuestionSubmissions(qdb);
      row.status = "approved";
      row.reviewedAt = nowIso();
      row.reviewedById = authed.id;
      row.reviewedByName = authed.name;
      saveQuestionCorrections(cdb);
      appendAdminAudit(req, authed, "question_correction_review", {
        id,
        action: "approve",
        targetType: "approved",
        questionId: row.questionId,
      });
      return res.json({ ok: true });
    }

    return res.status(400).json({ error: "未知纠错类型" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "审核失败" });
  }
});

app.get("/api/admin/deploy/status", (_req, res) => {
  try {
    const authed = requirePermission(_req, res, "manage_deploy");
    if (!authed) return;
    const state = loadDeployState();
    res.json({
      ...state,
      runtime: {
        pid: process.pid,
        bootTs: SERVER_BOOT_TS,
        bootIso: SERVER_BOOT_ISO,
      },
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取部署状态失败" });
  }
});

app.get("/api/admin/data-backups", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const opsConfig = loadOpsConfig();
    res.json({
      items: listDataBackups().slice(0, 50),
      defaultKeepCount: DEFAULT_DATA_BACKUP_KEEP_COUNT,
      keepCount: opsConfig.dataBackupKeepCount,
      autoBackupEnabled: opsConfig.autoBackupEnabled,
      autoBackupHour: opsConfig.autoBackupHour,
      autoBackupMinute: opsConfig.autoBackupMinute,
      lastAutoBackupDate: opsConfig.lastAutoBackupDate,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取数据备份失败" });
  }
});

app.post("/api/admin/data-backups/create", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const backup = createDataBackup();
    const opsConfig = loadOpsConfig();
    const keepCount = Math.max(
      1,
      Math.min(200, toSafeNumber(req.body?.keepCount, opsConfig.dataBackupKeepCount))
    );
    const prune = pruneDataBackups(keepCount);
    appendAdminAudit(req, authed, "data_backup_create", {
      backupId: backup.id,
      copied: backup.copied,
      keepCount,
      removedCount: prune.removedCount,
    });
    res.json({ ok: true, backup, prune, keepCount });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "创建数据备份失败" });
  }
});

app.post("/api/admin/data-backups/prune", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const opsConfig = loadOpsConfig();
    const keepCount = Math.max(
      1,
      Math.min(200, toSafeNumber(req.body?.keepCount, opsConfig.dataBackupKeepCount))
    );
    const result = pruneDataBackups(keepCount);
    appendAdminAudit(req, authed, "data_backup_prune", {
      keepCount: result.keepCount,
      removedCount: result.removedCount,
      removed: result.removed,
    });
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "清理旧备份失败" });
  }
});

app.post("/api/admin/data-backups/config/save", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const keepCount = Math.max(
      1,
      Math.min(200, toSafeNumber(req.body?.keepCount, DEFAULT_DATA_BACKUP_KEEP_COUNT))
    );
    const autoBackupEnabled = Boolean(req.body?.autoBackupEnabled);
    const autoBackupHour = Math.max(0, Math.min(23, toSafeNumber(req.body?.autoBackupHour, 3)));
    const autoBackupMinute = Math.max(0, Math.min(59, toSafeNumber(req.body?.autoBackupMinute, 0)));
    const opsConfig = saveOpsConfig({
      dataBackupKeepCount: keepCount,
      autoBackupEnabled,
      autoBackupHour,
      autoBackupMinute,
    });
    appendAdminAudit(req, authed, "data_backup_config_save", {
      keepCount: opsConfig.dataBackupKeepCount,
      autoBackupEnabled: opsConfig.autoBackupEnabled,
      autoBackupHour: opsConfig.autoBackupHour,
      autoBackupMinute: opsConfig.autoBackupMinute,
    });
    res.json({
      ok: true,
      keepCount: opsConfig.dataBackupKeepCount,
      autoBackupEnabled: opsConfig.autoBackupEnabled,
      autoBackupHour: opsConfig.autoBackupHour,
      autoBackupMinute: opsConfig.autoBackupMinute,
      lastAutoBackupDate: opsConfig.lastAutoBackupDate,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存备份保留设置失败" });
  }
});

app.post("/api/admin/data-backups/auto-run", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const result = runAutoDataBackupNowByUser(authed, req);
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "执行自动备份测试失败" });
  }
});

app.post("/api/admin/data-backups/restore", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const backupId = safeText(req.body?.backupId || "");
    const result = restoreDataBackup(backupId);
    clearReadCacheByPrefix("study:");
    clearReadCacheByPrefix("scripture:");
    appendAdminAudit(req, authed, "data_backup_restore", {
      backupId: result.backupId,
      restored: result.restored,
    });
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "恢复数据备份失败" });
  }
});

app.get("/api/admin/data-backups/download", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const backupId = safeText(req.query.backupId || "");
    const result = buildDataBackupZip(backupId);
    const fileName = `askbible-data-backup-${result.backupId}.zip`;
    appendAdminAudit(req, authed, "data_backup_download", {
      backupId: result.backupId,
      addedCount: result.addedCount,
    });
    res.download(result.zipPath, fileName, () => {
      try {
        if (fs.existsSync(result.zipPath)) fs.unlinkSync(result.zipPath);
      } catch (_err) {
        // ignore temp cleanup errors
      }
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "下载数据备份失败" });
  }
});

app.get("/api/admin/audit-log", (req, res) => {
  try {
    /* 与「部署/数据」页其它接口一致，避免仅打开后台却因未登录报 401 */
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const limit = Math.max(1, Math.min(300, toSafeNumber(req.query.limit, 80)));
    const db = loadAdminAudit();
    res.json({ items: (db.items || []).slice(0, limit) });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取审计日志失败" });
  }
});

app.get("/api/admin/remote-sync/config", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const remote = getRemoteSyncConfig();
    res.json({
      configured: Boolean(remote.baseUrl && remote.adminToken),
      baseUrl: remote.baseUrl || "",
      tokenMasked: maskRemoteSyncToken(remote.adminToken),
      note: "仅同步已发布内容、人物/插画配置、public/generated；不含账号与提问数据。",
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取远端同步配置失败" });
  }
});

app.post("/api/admin/remote-sync/config/save", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const current = getRemoteSyncConfig();
    const baseUrl = normalizeRemoteSyncBaseUrl(req.body?.baseUrl || "");
    const incomingToken = safeText(req.body?.adminToken || "");
    const adminToken = incomingToken || current.adminToken;
    if (req.body?.baseUrl && !baseUrl) {
      return res.status(400).json({ error: "请输入有效的远端站点地址" });
    }
    saveSystemSecrets({ remoteSyncBaseUrl: baseUrl, remoteSyncAdminToken: adminToken });
    appendAdminAudit(req, authed, "remote_sync_config_save", {
      baseUrl,
      hasToken: Boolean(adminToken),
    });
    res.json({
      ok: true,
      configured: Boolean(baseUrl && adminToken),
      baseUrl,
      tokenMasked: maskRemoteSyncToken(adminToken),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存远端同步配置失败" });
  }
});

app.get("/api/admin/sync/snapshot", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    res.json({ ok: true, snapshot: buildSyncSnapshot() });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "生成同步快照失败" });
  }
});

app.post("/api/admin/sync/export", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const paths = Array.isArray(req.body?.paths) ? req.body.paths : [];
    if (!paths.length) {
      return res.status(400).json({ error: "缺少 paths" });
    }
    const label = safeText(req.body?.label || "sync");
    const result = buildSyncPackageZipFromRelPaths(paths, label);
    const fileName = `askbible-sync-${label}-${Date.now()}.zip`;
    appendAdminAudit(req, authed, "remote_sync_export", {
      label,
      addedCount: result.addedCount,
    });
    res.download(result.zipPath, fileName);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "导出同步包失败" });
  }
});

app.post("/api/admin/sync/import", upload.single("package"), (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    if (!req.file) {
      return res.status(400).json({ error: "缺少同步包" });
    }
    const result = applySyncZipBuffer(
      fs.readFileSync(req.file.path),
      safeText(req.body?.source || "upload"),
      req,
      authed
    );
    try {
      fs.unlinkSync(req.file.path);
    } catch {}
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "导入同步包失败" });
  }
});

app.post("/api/admin/remote-sync/preview", async (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const remoteSnapshotPayload = await fetchRemoteSyncJson("/api/admin/sync/snapshot");
    const remoteSnapshot = remoteSnapshotPayload?.snapshot || { files: [] };
    const localSnapshot = buildSyncSnapshot();
    const diff = compareSyncSnapshots(localSnapshot, remoteSnapshot);
    res.json({
      ok: true,
      remoteBaseUrl: getRemoteSyncConfig().baseUrl,
      localGeneratedAt: localSnapshot.generatedAt,
      remoteGeneratedAt: safeText(remoteSnapshot.generatedAt || ""),
      summary: {
        localFileCount: diff.localFileCount,
        remoteFileCount: diff.remoteFileCount,
        onlyRemoteCount: diff.onlyRemote.length,
        onlyLocalCount: diff.onlyLocal.length,
        differentCount: diff.different.length,
        pullCandidateCount: diff.pullPaths.length,
        pushCandidateCount: diff.pushPaths.length,
        groupCounts: diff.groupCounts,
      },
      samples: {
        onlyRemote: diff.onlyRemote.slice(0, 20).map((x) => x.rel),
        onlyLocal: diff.onlyLocal.slice(0, 20).map((x) => x.rel),
        different: diff.different.slice(0, 20).map((x) => x.rel),
      },
      pullPaths: diff.pullPaths,
      pushPaths: diff.pushPaths,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "远端同步预检失败" });
  }
});

app.post("/api/admin/remote-sync/pull", async (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const remoteSnapshotPayload = await fetchRemoteSyncJson("/api/admin/sync/snapshot");
    const remoteSnapshot = remoteSnapshotPayload?.snapshot || { files: [] };
    const localSnapshot = buildSyncSnapshot();
    const diff = compareSyncSnapshots(localSnapshot, remoteSnapshot);
    if (!diff.pullPaths.length) {
      return res.json({ ok: true, appliedCount: 0, backupId: "", message: "本机已是最新，无需补齐。" });
    }
    const zipBuffer = await fetchRemoteSyncZip("/api/admin/sync/export", {
      label: "remote-pull",
      paths: diff.pullPaths,
    });
    const result = applySyncZipBuffer(zipBuffer, "remote-pull", req, authed);
    appendAdminAudit(req, authed, "remote_sync_pull", {
      appliedCount: result.appliedCount,
      backupId: result.backupId,
      remoteBaseUrl: getRemoteSyncConfig().baseUrl,
    });
    res.json({ ok: true, ...result, requestedCount: diff.pullPaths.length });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "远端补齐到本机失败" });
  }
});

app.post("/api/admin/remote-sync/push", async (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const remoteSnapshotPayload = await fetchRemoteSyncJson("/api/admin/sync/snapshot");
    const remoteSnapshot = remoteSnapshotPayload?.snapshot || { files: [] };
    const localSnapshot = buildSyncSnapshot();
    const diff = compareSyncSnapshots(localSnapshot, remoteSnapshot);
    if (!diff.pushPaths.length) {
      return res.json({ ok: true, pushedCount: 0, message: "线上已经与本机一致，无需推送。" });
    }
    const exportZip = buildSyncPackageZipFromRelPaths(diff.pushPaths, "remote-push");
    const buf = fs.readFileSync(exportZip.zipPath);
    const remote = getRemoteSyncConfig();
    const form = new FormData();
    form.append(
      "package",
      new Blob([buf], { type: "application/zip" }),
      path.basename(exportZip.zipPath)
    );
    form.append("source", "local-push");
    const resRemote = await fetch(`${remote.baseUrl}/api/admin/sync/import`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${remote.adminToken}`,
      },
      body: form,
    });
    const data = await resRemote.json().catch(() => ({}));
    if (!resRemote.ok) {
      throw new Error(data.error || `远端导入失败 HTTP ${resRemote.status}`);
    }
    appendAdminAudit(req, authed, "remote_sync_push", {
      pushedCount: data.appliedCount || exportZip.addedCount,
      remoteBaseUrl: remote.baseUrl,
      remoteBackupId: data.backupId || "",
    });
    res.json({
      ok: true,
      pushedCount: data.appliedCount || exportZip.addedCount,
      remoteBackupId: data.backupId || "",
      requestedCount: diff.pushPaths.length,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "本机推送到远端失败" });
  }
});

app.get("/api/admin/deploy/package-command", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const safeKind = normalizeDeployPackageKind(req.query.kind);
    const suggestedVersion = safeText(req.query.version || "") || `v${Date.now()}`;
    const packageName = `askbible-${safeKind}-${suggestedVersion}.zip`;
    const excludes = [
      "node_modules/*",
      ".git/*",
      ".cursor/*",
      "admin_data/deploy/*",
      "admin_data/system_secrets.json",
      ".env",
      ".env.*",
    ];
    if (safeKind === "full-slim") {
      excludes.push(
        "content_published/*",
        "content_builds/*",
        "data/*",
        "admin_data/jobs/*",
        "deploy-builds/*",
        "admin_data/auth.sqlite",
        "admin_data/auth.sqlite-shm",
        "admin_data/auth.sqlite-wal",
        "admin_data/analytics.sqlite",
        "admin_data/analytics.sqlite-shm",
        "admin_data/analytics.sqlite-wal",
        "admin_data/auth.db",
        "admin_data/auth/*"
      );
    }
    const cmd = `cd "${__dirname}" && zip -r "${packageName}" . ${excludes
      .map((x) => `-x "${x}"`)
      .join(" ")}`;
    res.json({ ok: true, kind: safeKind, packageName, command: cmd });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "生成打包命令失败" });
  }
});

app.get("/api/admin/deploy/package/download", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const kind = safeText(req.query.kind || "upgrade");
    const version = safeText(req.query.version || "");
    const result = buildPackageZip({ kind, version });
    const fileName = `askbible-${result.packageKind}-${result.version}.zip`;
    res.download(result.zipPath, fileName);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "生成打包文件失败" });
  }
});

app.post("/api/admin/deploy/package/download-changed", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const version = safeText(req.body?.version || "");
    const changes = Array.isArray(req.body?.changes) ? req.body.changes : [];
    if (!changes.length) {
      return res.status(400).json({ error: "缺少改动章节清单 changes" });
    }
    const result = buildChangedChaptersPackageZip({ version, changes });
    const fileName = `askbible-changed-${result.version}.zip`;
    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
    res.setHeader("X-Package-Added-Count", String(result.addedCount || 0));
    res.setHeader("X-Package-Missing-Count", String(result.missingCount || 0));
    res.setHeader(
      "X-Package-Generated-Asset-Count",
      String(result.generatedAssetCount || 0)
    );
    res.setHeader(
      "X-Package-Admin-Sync-File-Count",
      String(result.adminSyncFileCount || 0)
    );
    fs.createReadStream(result.zipPath).pipe(res);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "生成改动升级包失败" });
  }
});

app.post("/api/admin/deploy/upload", upload.single("package"), (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    if (!req.file) {
      return res.status(400).json({ error: "缺少上传文件" });
    }
    const originalName = safeText(req.file.originalname || "");
    if (!originalName.toLowerCase().endsWith(".zip")) {
      return res.status(400).json({ error: "仅支持 zip 包" });
    }
    const uploadId = `up_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const zipPath = path.join(DEPLOY_UPLOADS_DIR, `${uploadId}.zip`);
    fs.renameSync(req.file.path, zipPath);

    const releaseDir = path.join(DEPLOY_RELEASES_DIR, uploadId);
    ensureDir(releaseDir);
    const zip = new AdmZip(zipPath);
    zip.extractAllTo(releaseDir, true);

    const metaPath = path.join(releaseDir, "version.json");
    const meta = readJson(metaPath, {});
    const version = safeText(meta?.version || uploadId);

    const state = loadDeployState();
    state.uploads = [
      {
        id: uploadId,
        version,
        uploadedAt: nowIso(),
        originalName,
        zipPath,
        releaseDir,
      },
      ...(state.uploads || []),
    ].slice(0, 30);
    saveDeployState(state);
    res.json({ ok: true, uploadId, version });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "上传升级包失败" });
  }
});

app.post("/api/admin/deploy/apply", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const uploadId = safeText(req.body?.uploadId || "");
    if (!uploadId) return res.status(400).json({ error: "缺少 uploadId" });
    const state = loadDeployState();
    const item = (state.uploads || []).find((x) => safeText(x.id) === uploadId);
    if (!item) return res.status(404).json({ error: "未找到上传包" });
    const releaseDir = safeText(item.releaseDir || "");
    if (!releaseDir || !fs.existsSync(releaseDir)) {
      return res.status(400).json({ error: "发布目录不存在" });
    }

    const backupId = `bk_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const backupDir = path.join(DEPLOY_BACKUPS_DIR, backupId);
    ensureDir(backupDir);
    const manifest = [];
    const protectedPrefixes = [
      "admin_data/",
      "node_modules/",
      ".git/",
      ".cursor/",
      ".DS_Store",
    ];

    const files = walkFiles(releaseDir);
    for (const absSrc of files) {
      const rel = path.relative(releaseDir, absSrc).replaceAll("\\", "/");
      if (!rel || protectedPrefixes.some((p) => rel.startsWith(p))) continue;
      const dest = path.join(__dirname, rel);
      if (fs.existsSync(dest) && fs.statSync(dest).isFile()) {
        const backupPath = path.join(backupDir, rel);
        ensureDir(path.dirname(backupPath));
        fs.copyFileSync(dest, backupPath);
        manifest.push({ rel, hadOriginal: true });
      } else {
        manifest.push({ rel, hadOriginal: false });
      }
      ensureDir(path.dirname(dest));
      fs.copyFileSync(absSrc, dest);
    }

    writeJson(path.join(backupDir, "manifest.json"), manifest);
    state.currentVersion = safeText(item.version || uploadId);
    state.history = [
      {
        action: "apply",
        version: state.currentVersion,
        uploadId,
        backupId,
        at: nowIso(),
      },
      ...(state.history || []),
    ].slice(0, 100);
    saveDeployState(state);
    appendAdminAudit(req, authed, "deploy_apply", {
      uploadId,
      version: state.currentVersion,
      backupId,
    });
    res.json({ ok: true, version: state.currentVersion, backupId });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "应用升级失败" });
  }
});

app.post("/api/admin/deploy/rollback", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const state = loadDeployState();
    const targetBackupId =
      safeText(req.body?.backupId || "") ||
      safeText((state.history || []).find((x) => x.action === "apply")?.backupId || "");
    if (!targetBackupId) return res.status(400).json({ error: "没有可回滚备份" });
    const backupDir = path.join(DEPLOY_BACKUPS_DIR, targetBackupId);
    const manifest = readJson(path.join(backupDir, "manifest.json"), []);
    if (!Array.isArray(manifest) || !manifest.length) {
      return res.status(400).json({ error: "备份信息损坏或为空" });
    }
    for (const item of manifest) {
      const rel = safeText(item?.rel || "");
      if (!rel) continue;
      const dest = path.join(__dirname, rel);
      const backupFile = path.join(backupDir, rel);
      if (item.hadOriginal && fs.existsSync(backupFile)) {
        ensureDir(path.dirname(dest));
        fs.copyFileSync(backupFile, dest);
      } else if (fs.existsSync(dest)) {
        fs.unlinkSync(dest);
      }
    }
    state.history = [
      {
        action: "rollback",
        backupId: targetBackupId,
        at: nowIso(),
      },
      ...(state.history || []),
    ].slice(0, 100);
    saveDeployState(state);
    appendAdminAudit(req, authed, "deploy_rollback", {
      backupId: targetBackupId,
    });
    res.json({ ok: true, backupId: targetBackupId });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "回滚失败" });
  }
});

app.get("/api/admin/questions/submissions", (req, res) => {
  try {
    const authed = requirePermission(req, res, "review_questions");
    if (!authed) return;
    const status = safeText(req.query.status || "pending");
    const db = loadQuestionSubmissions();
    const items = (db.items || [])
      .filter((x) =>
        status === "all"
          ? true
          : ["pending", "approved", "rejected"].includes(status)
            ? safeText(x.status) === status
            : safeText(x.status) === "pending"
      )
      .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
    res.json({ items });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取待审问题失败" });
  }
});

app.post("/api/admin/questions/review", (req, res) => {
  try {
    const authed = requirePermission(req, res, "review_questions");
    if (!authed) return;
    const id = safeText(req.body?.id || "");
    const action = safeText(req.body?.action || "");
    if (!id) return res.status(400).json({ error: "缺少 id" });
    if (!["approve", "reject"].includes(action)) {
      return res.status(400).json({ error: "action 必须是 approve/reject" });
    }
    const db = loadQuestionSubmissions();
    const nextStatus = action === "approve" ? "approved" : "rejected";
    let updated = false;
    db.items = (db.items || []).map((item) => {
      if (safeText(item.id) !== id) return item;
      updated = true;
      return {
        ...item,
        status: nextStatus,
        reviewedAt: nowIso(),
        reviewedBy: authed.id,
        reviewedByName: authed.name,
      };
    });
    if (!updated) return res.status(404).json({ error: "未找到记录" });
    saveQuestionSubmissions(db);
    appendAdminAudit(req, authed, "question_review", {
      id,
      status: nextStatus,
    });
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "审核失败" });
  }
});

app.post("/api/admin/questions/update-text", (req, res) => {
  try {
    const authed = requirePermission(req, res, "edit_approved_question_text");
    if (!authed) return;
    const questionId = safeText(req.body?.questionId || "");
    const questionText = safeText(req.body?.questionText || "");
    if (!questionId) return res.status(400).json({ error: "缺少问题编号" });
    if (!questionText || questionText.length < 4) {
      return res.status(400).json({ error: "问题内容至少 4 个字" });
    }
    if (questionText.length > 4000) {
      return res.status(400).json({ error: "问题过长" });
    }
    const rate = checkWriteRateLimit({
      req,
      actionKey: "question_admin_text_edit",
      limit: 40,
      windowMs: 60 * 1000,
    });
    if (!rate.ok) {
      return res.status(429).json({
        error: `操作过于频繁，请 ${rate.retryAfterSec}s 后重试`,
      });
    }

    const db = loadQuestionSubmissions();
    const target = (db.items || []).find((x) => safeText(x.id) === questionId);
    if (!target) return res.status(404).json({ error: "问题不存在" });
    if (safeText(target.status) !== "approved") {
      return res.status(400).json({ error: "仅可编辑已采纳的问题" });
    }
    const prevPreview = String(target.questionText || "").slice(0, 120);
    target.questionText = questionText;
    target.adminTextEditedAt = nowIso();
    target.adminTextEditedBy = authed.id;
    target.adminTextEditedByName = authed.name;
    saveQuestionSubmissions(db);
    appendAdminAudit(req, authed, "question_text_edit", {
      questionId,
      prevPreview,
      newPreview: questionText.slice(0, 120),
    });
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "更新失败" });
  }
});

app.post("/api/admin/users/set-admin", (req, res) => {
  try {
    let authed = null;
    const qianCount =
      authDb
        .prepare("SELECT COUNT(1) as c FROM users WHERE admin_role = 'qianfuzhang'")
        .get()?.c || 0;
    if (Number(qianCount) > 0) {
      authed = requirePermission(req, res, "manage_roles");
      if (!authed) return;
    } else {
      const adminPassword = safeText(req.body?.adminPassword || "");
      if (adminPassword !== process.env.ADMIN_PASSWORD && adminPassword !== "0777") {
        return res.status(403).json({ error: "首次初始化需要管理口令" });
      }
    }
    const email = safeText(req.body?.email || "").toLowerCase();
    const roleInput = normalizeAdminRole(req.body?.role || "");
    if (!email || !email.includes("@")) {
      return res.status(400).json({ error: "邮箱格式不正确" });
    }
    const hit = authDb
      .prepare("SELECT id, name, email FROM users WHERE email = ? LIMIT 1")
      .get(email);
    if (!hit) return res.status(404).json({ error: "用户不存在" });
    authDb
      .prepare("UPDATE users SET is_admin = ?, admin_role = ?, updated_at = ? WHERE id = ?")
      .run(roleInput ? 1 : 0, roleInput, nowIso(), hit.id);
    appendAdminAudit(req, authed, "set_admin_role", {
      targetEmail: email,
      role: roleInput,
      bootstrap: !authed,
    });
    res.json({
      ok: true,
      user: {
        id: hit.id,
        name: hit.name,
        email: hit.email,
        isAdmin: Boolean(roleInput),
        adminRole: roleInput,
      },
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "设置管理员失败" });
  }
});

app.get("/api/admin/users/admin-list", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_roles");
    if (!authed) return;
    const rows = authDb
      .prepare(
        "SELECT id, name, email, created_at, updated_at, admin_role FROM users WHERE is_admin = 1 ORDER BY created_at DESC"
      )
      .all();
    res.json({
      items: (rows || []).map((x) => ({
        id: x.id,
        name: x.name,
        email: x.email,
        adminRole: normalizeAdminRole(x.admin_role || ""),
        createdAt: x.created_at || "",
        updatedAt: x.updated_at || "",
      })),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取管理员列表失败" });
  }
});

/* =========================================================
   后台初始化
   ========================================================= */
app.get("/api/admin/bootstrap", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const payload = {
      languages: loadLanguages().languages || [],
      scriptureVersions: getAllScriptureVersions(),
      contentVersions: loadContentVersions().contentVersions || [],
      published: loadPublished(),
      books: flattenBooks(),
      pointsConfig: loadPointsConfig(),
      siteChrome: loadSiteChrome(),
    };
    res.json(payload);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "后台初始化失败" });
  }
});

app.get("/api/admin/bible-primary-characters", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    const { bookLabelById, summary, primaryEntriesByBook } = buildPrimaryCharacterDirectorySummary();
    res.json({
      primaryCharactersByBook: BIBLE_PRIMARY_CHARACTERS_BY_BOOK,
      primaryEntriesByBook,
      bookLabelById,
      summary,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取主人物目录失败" });
  }
});

app.get("/api/admin/system/openai-key/status", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_rules");
    if (!authed) return;
    const secretKey = safeText(loadSystemSecrets().openaiApiKey || "");
    const envKey = safeText(process.env.OPENAI_API_KEY || "");
    const effective = envKey || secretKey;
    res.json({
      configured: Boolean(effective),
      source: envKey ? "env" : secretKey ? "system" : "none",
      masked: effective ? `sk-***${effective.slice(-4)}` : "",
      envMasked: envKey ? `sk-***${envKey.slice(-4)}` : "",
      systemMasked: secretKey ? `sk-***${secretKey.slice(-4)}` : "",
      hasEnv: Boolean(envKey),
      hasSystemSecret: Boolean(secretKey),
      systemSecretShadowed: Boolean(envKey && secretKey),
      /** 环境变量优先时，后台保存的 Key 不会参与请求 */
      envOverridesSystem: Boolean(envKey),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取密钥状态失败" });
  }
});

app.post("/api/admin/system/openai-key/save", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_rules");
    if (!authed) return;
    const apiKey = safeText(req.body?.apiKey || "");
    if (!apiKey || !apiKey.startsWith("sk-")) {
      return res.status(400).json({ error: "请输入有效的 OpenAI Key" });
    }
    saveSystemSecrets({ openaiApiKey: apiKey });
    openAiClient = null;
    openAiClientKey = "";
    appendAdminAudit(req, authed, "system_openai_key_save", { source: "system_secrets" });
    const envKey = safeText(process.env.OPENAI_API_KEY || "");
    res.json({
      ok: true,
      configured: true,
      masked: `sk-***${apiKey.slice(-4)}`,
      warning: envKey
        ? "已保存到后台，但当前进程仍优先使用环境变量 OPENAI_API_KEY（与后台可为不同 Key）。若测试生成仍 401，请到服务器/托管面板更新或删除 OPENAI_API_KEY 后重启服务。"
        : "",
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存密钥失败" });
  }
});

app.post("/api/admin/system/openai-key/clear", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_rules");
    if (!authed) return;
    saveSystemSecrets({ openaiApiKey: "" });
    openAiClient = null;
    openAiClientKey = "";
    appendAdminAudit(req, authed, "system_openai_key_clear", {});
    const envKey = safeText(process.env.OPENAI_API_KEY || "");
    res.json({
      ok: true,
      configured: Boolean(envKey),
      source: envKey ? "env" : "none",
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "清空密钥失败" });
  }
});

app.get("/api/admin/points/config", (_req, res) => {
  try {
    const authed = requirePermission(_req, res, "manage_points");
    if (!authed) return;
    res.json(loadPointsConfig());
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取成长体系配置失败" });
  }
});

app.post("/api/admin/points/config/save", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_points");
    if (!authed) return;
    const pointsConfig = req.body?.pointsConfig;
    if (!pointsConfig || typeof pointsConfig !== "object") {
      return res.status(400).json({ error: "缺少 pointsConfig" });
    }
    const saved = savePointsConfig(pointsConfig);
    res.json({ ok: true, pointsConfig: saved });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存成长体系配置失败" });
  }
});

/* =========================================================
   圣经版本管理接口
   ========================================================= */
app.get("/api/admin/scripture-versions", (_req, res) => {
  try {
    const authed = requirePermission(_req, res, "manage_rules");
    if (!authed) return;
    res.json({
      scriptureVersions: getAllScriptureVersions(),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取圣经版本失败" });
  }
});

/* =========================================================
   内容版本（读经类型 / 前台菜单可见性）
   ========================================================= */
app.get("/api/admin/content-versions", (_req, res) => {
  try {
    const authed = requirePermission(_req, res, "manage_rules");
    if (!authed) return;
    res.json(loadContentVersions());
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取内容版本失败" });
  }
});

app.post("/api/admin/content-versions/save", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_rules");
    if (!authed) return;
    const raw = req.body?.contentVersions;
    if (!Array.isArray(raw) || !raw.length) {
      return res.status(400).json({ error: "contentVersions 必须为非空数组" });
    }
    const seen = new Set();
    const normalized = raw.map((x, i) => {
      const id = safeText(x?.id || "");
      if (!id) throw new Error(`第 ${i + 1} 条缺少 id`);
      if (seen.has(id)) throw new Error(`重复的 id：${id}`);
      seen.add(id);
      return {
        id,
        label: safeText(x?.label || id),
        enabled: x?.enabled !== false,
        showInMenu: x?.showInMenu !== false,
        order: Number(x?.order) || i + 1,
      };
    });
    const menuCount = normalized.filter(
      (x) => x.enabled && x.showInMenu !== false
    ).length;
    if (!menuCount) {
      return res
        .status(400)
        .json({ error: "至少保留一个在「前台菜单显示」且已启用的版本" });
    }
    saveContentVersions({ contentVersions: normalized });
    res.json({ ok: true, contentVersions: normalized });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存内容版本失败" });
  }
});

app.post("/api/admin/scripture-version/save", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_rules");
    if (!authed) return;
    const { scriptureVersion } = req.body || {};
    if (!scriptureVersion || typeof scriptureVersion !== "object") {
      return res.status(400).json({ error: "缺少 scriptureVersion" });
    }

    const saved = upsertScriptureVersion(scriptureVersion);
    res.json({ ok: true, scriptureVersion: saved });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存圣经版本失败" });
  }
});

app.delete("/api/admin/scripture-version", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_rules");
    if (!authed) return;
    const { id } = req.query;
    if (!id) {
      return res.status(400).json({ error: "缺少 id" });
    }

    const result = deleteScriptureVersion(String(id));
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "删除圣经版本失败" });
  }
});

/* =========================================================
   规则
   ========================================================= */
app.get("/api/admin/rule", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_rules");
    if (!authed) return;
    const { version } = req.query;
    if (!version) {
      return res.status(400).json({ error: "缺少 version" });
    }

    const ruleConfig = loadRuleConfig(String(version));
    if (!ruleConfig) {
      return res.status(404).json({ error: "未找到规则" });
    }

    res.json(ruleConfig);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取规则失败" });
  }
});

app.post("/api/admin/rule/save", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_rules");
    if (!authed) return;
    const { version, ruleConfig } = req.body || {};
    if (!version || !ruleConfig) {
      return res.status(400).json({ error: "缺少 version 或 ruleConfig" });
    }

    const filePath = path.join(RULES_DIR, `${version}.json`);
    writeJson(filePath, ruleConfig);

    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "保存规则失败" });
  }
});

/* =========================================================
   单章测试
   ========================================================= */
app.post("/api/admin/test-generate", async (req, res) => {
  try {
    const { version, lang, bookId, chapter, primaryScriptureVersionId } = req.body || {};

    if (!version || !lang || !bookId || !chapter) {
      return res.status(400).json({
        error: "缺少 version / lang / bookId / chapter",
      });
    }

    const result = await generateStudyWithRuleConfig({
      versionId: String(version),
      lang: String(lang),
      bookId: String(bookId),
      chapter: Number(chapter),
      primaryScriptureVersionId: String(primaryScriptureVersionId || ""),
    });

    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: error.message || "测试生成失败",
    });
  }
});

/* =========================================================
   保存测试结果
   ========================================================= */
app.post("/api/admin/save-test-result", (req, res) => {
  try {
    requirePermission(req, res, "manage_publish");
    const { studyContent, reviewNote } = req.body || {};

    if (!studyContent || typeof studyContent !== "object") {
      return res.status(400).json({
        error: "缺少 studyContent",
      });
    }

    const result = saveStudyContentAndPublish(studyContent);
    const actor = getAuthedUserFromReq(req);
    appendAdminAudit(
      req,
      actor || { id: "", name: "", email: "", adminRole: "" },
      "study_chapter_save_publish",
      {
        bookId: result.savedContent?.bookId,
        chapter: result.savedContent?.chapter,
        version: result.savedContent?.version,
        contentLang: result.savedContent?.contentLang,
        reviewNote: safeText(reviewNote || ""),
      }
    );

    res.json({
      ok: true,
      message: "已保存并合并发布",
      buildId: result.buildId,
      savedContent: result.savedContent,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: error.message || "保存测试结果失败",
    });
  }
});

/* =========================================================
   批量任务接口
   ========================================================= */
app.post("/api/admin/job/create", (req, res) => {
  try {
    const payload = req.body || {};
    const job = createBulkJob(payload);
    runJobRunnerLoop().catch((error) => {
      console.error("手动触发任务执行器失败:", error);
    });

    res.json({
      ok: true,
      job,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "创建任务失败" });
  }
});

app.post("/api/admin/job/:id/retry-failed", (req, res) => {
  try {
    const retryJob = createRetryFailedJob(req.params.id);
    runJobRunnerLoop().catch((error) => {
      console.error("手动触发任务执行器失败:", error);
    });

    res.json({
      ok: true,
      job: retryJob,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "重跑失败章节失败" });
  }
});

app.get("/api/admin/jobs", (_req, res) => {
  try {
    const jobs = listAllJobsNewestFirst();
    res.json({ jobs });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取任务失败" });
  }
});

app.get("/api/admin/job/:id", (req, res) => {
  try {
    const job = readJob(req.params.id);
    if (!job) {
      return res.status(404).json({ error: "任务不存在" });
    }
    res.json(job);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取任务详情失败" });
  }
});

app.post("/api/admin/job/:id/cancel", (req, res) => {
  try {
    const job = readJob(req.params.id);
    if (!job) {
      return res.status(404).json({ error: "任务不存在" });
    }

    if (job.status === "completed") {
      return res.status(400).json({ error: "已完成任务不能取消" });
    }

    job.status = "cancelled";
    job.updatedAt = nowIso();
    job.finishedAt = job.finishedAt || nowIso();
    job.progressText = "任务已取消";
    job.completionSummary = "已取消";
    writeJob(job);

    res.json({ ok: true, job });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "取消任务失败" });
  }
});

app.post("/api/admin/job/:id/merge-publish-build", async (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const job = readJob(req.params.id);
    if (!job) {
      return res.status(404).json({ error: "任务不存在" });
    }
    if (job.status === "running" || job.status === "queued") {
      return res
        .status(400)
        .json({ error: "任务仍在排队或执行中，请等待结束后再合并发布" });
    }
    const body = req.body || {};
    const onlyChanged = body.onlyChanged !== false;
    const dryRun = body.dryRun === true;
    const skipReplacesExisting = body.skipReplacesExisting === true;
    const result = await mergePublishFromJobBuild(job, {
      onlyChanged,
      dryRun,
      skipReplacesExisting,
    });
    if (!dryRun && result.totalPublishedCount > 0) {
      const prefixes = new Set();
      for (const t of job.targets || []) {
        if (t?.versionId && t?.lang) {
          prefixes.add(
            `${STUDY_CONTENT_CACHE_TAG}:${String(t.versionId)}:${String(t.lang)}:`
          );
        }
      }
      prefixes.forEach((p) => clearReadCacheByPrefix(p));
    }
    appendAdminAudit(req, authed, "job_merge_publish_build", {
      jobId: job.id,
      buildId: job.buildId,
      totalPublishedCount: result.totalPublishedCount,
      dryRun,
      skipReplacesExisting,
    });
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "合并发布失败" });
  }
});

app.post("/api/admin/jobs/merge-publish-partial-builds", async (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const body = req.body || {};
    const onlyChanged = body.onlyChanged !== false;
    const dryRun = body.dryRun === true;
    const skipReplacesExisting = body.skipReplacesExisting === true;
    const result = await mergePublishAllPartialJobBuilds({
      onlyChanged,
      dryRun,
      skipReplacesExisting,
    });
    if (!dryRun && result.totalPublishedCount > 0) {
      clearReadCacheByPrefix("study:");
    }
    appendAdminAudit(req, authed, "jobs_merge_publish_partial_builds", {
      jobCount: result.jobCount,
      totalPublishedCount: result.totalPublishedCount,
      dryRun,
      skipReplacesExisting,
    });
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "批量合并发布失败" });
  }
});

/* =========================================================
   手动合并发布
   ========================================================= */
app.post("/api/admin/publish", async (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const body = req.body || {};
    const { buildId, version, lang, targets, onlyChanged, dryRun } = body;
    if (!buildId || !version || !lang || !Array.isArray(targets)) {
      return res.status(400).json({
        error: "缺少 buildId / version / lang / targets",
      });
    }

    const result = await mergePublishFromBuild({
      buildId,
      versionId: version,
      lang,
      targets,
      onlyChanged: onlyChanged !== false,
      dryRun: dryRun === true,
      skipReplacesExisting: body.skipReplacesExisting === true,
    });
    if (dryRun !== true) {
      clearReadCacheByPrefix(
        `${STUDY_CONTENT_CACHE_TAG}:${String(version)}:${String(lang)}:`
      );
    }

    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "合并发布失败" });
  }
});

/* =========================================================
   已发布内容管理接口
   ========================================================= */
app.get("/api/admin/published/overview", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const { version, lang } = req.query;
    if (!version || !lang) {
      return res.status(400).json({ error: "缺少 version 或 lang" });
    }

    const result = listPublishedOverview(String(version), String(lang));
    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取发布概览失败" });
  }
});

app.get("/api/admin/published/chapter", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const { version, lang, bookId, chapter } = req.query;
    if (!version || !lang || !bookId || !chapter) {
      return res
        .status(400)
        .json({ error: "缺少 version / lang / bookId / chapter" });
    }

    const data = readPublishedContent({
      versionId: String(version),
      lang: String(lang),
      bookId: String(bookId),
      chapter: Number(chapter),
    });

    if (!data) {
      return res.status(404).json({ error: "未找到已发布章节" });
    }

    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取已发布章节失败" });
  }
});

app.delete("/api/admin/published/chapter", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const { version, lang, bookId, chapter } = req.query;
    if (!version || !lang || !bookId || !chapter) {
      return res
        .status(400)
        .json({ error: "缺少 version / lang / bookId / chapter" });
    }

    const result = deletePublishedChapter(
      String(version),
      String(lang),
      String(bookId),
      Number(chapter)
    );
    clearReadCacheByPrefix(
      `${STUDY_CONTENT_CACHE_TAG}:${String(version)}:${String(lang)}:${String(bookId)}:${Number(chapter)}`
    );

    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "删除已发布章节失败" });
  }
});

app.post("/api/admin/published/auto-republish-chapter", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const { version, lang, bookId, chapter, onlyChanged, dryRun } = req.body || {};
    if (!version || !lang || !bookId || !chapter) {
      return res.status(400).json({
        error: "缺少 version / lang / bookId / chapter",
      });
    }

    const result = autoRepublishChapter({
      versionId: String(version),
      lang: String(lang),
      bookId: String(bookId),
      chapter: Number(chapter),
      onlyChanged: onlyChanged !== false,
      dryRun: dryRun === true,
    });
    if (dryRun !== true) {
      clearReadCacheByPrefix(
        `${STUDY_CONTENT_CACHE_TAG}:${String(version)}:${String(lang)}:${String(bookId)}:${Number(chapter)}`
      );
    }

    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "自动补发失败" });
  }
});

app.post("/api/admin/published/republish-bulk", async (req, res) => {
  let runId = "";
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const body = req.body || {};
    const { mode, version, lang, onlyChanged, dryRun } = body;
    const safeMode = safeText(mode || "all");
    const allowedModes = new Set(["all", "version", "lang", "version_lang"]);
    if (!allowedModes.has(safeMode)) {
      return res.status(400).json({ error: "mode 不正确" });
    }
    if (safeMode === "version" && !isNonEmptyString(version)) {
      return res.status(400).json({ error: "按版本发布缺少 version" });
    }
    if (safeMode === "lang" && !isNonEmptyString(lang)) {
      return res.status(400).json({ error: "按语言发布缺少 lang" });
    }
    if (
      safeMode === "version_lang" &&
      (!isNonEmptyString(version) || !isNonEmptyString(lang))
    ) {
      return res
        .status(400)
        .json({ error: "按版本+语言发布缺少 version 或 lang" });
    }

    runId = beginPublishRun("republish-bulk");
    const result = await republishBulkFromLatestBuilds({
      mode: safeMode,
      version: safeText(version),
      lang: safeText(lang),
      onlyChanged: onlyChanged !== false,
      dryRun: dryRun === true,
      runId,
      skipReplacesExisting: body.skipReplacesExisting === true,
    });
    if (dryRun !== true) {
      clearReadCacheByPrefix("study:");
    }
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    const msg = error.message || "整本发布失败";
    if (msg.includes("已手动停止")) {
      return res.status(409).json({ error: msg });
    }
    res.status(500).json({ error: msg });
  } finally {
    if (runId) endPublishRun(runId);
  }
});

app.post("/api/admin/published/auto-republish-missing-bulk", async (req, res) => {
  let runId = "";
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const { mode, version, lang, onlyChanged, dryRun } = req.body || {};
    const safeMode = safeText(mode || "all");
    const allowedModes = new Set(["all", "version", "lang", "version_lang"]);
    if (!allowedModes.has(safeMode)) {
      return res.status(400).json({ error: "mode 不正确" });
    }
    if (safeMode === "version" && !isNonEmptyString(version)) {
      return res.status(400).json({ error: "按版本查漏补发缺少 version" });
    }
    if (safeMode === "lang" && !isNonEmptyString(lang)) {
      return res.status(400).json({ error: "按语言查漏补发缺少 lang" });
    }
    if (
      safeMode === "version_lang" &&
      (!isNonEmptyString(version) || !isNonEmptyString(lang))
    ) {
      return res
        .status(400)
        .json({ error: "按版本+语言查漏补发缺少 version 或 lang" });
    }

    runId = beginPublishRun("auto-republish-missing-bulk");
    const result = await autoRepublishMissingBulkFromLatestBuilds({
      mode: safeMode,
      version: safeText(version),
      lang: safeText(lang),
      onlyChanged: onlyChanged !== false,
      dryRun: dryRun === true,
      runId,
    });
    if (dryRun !== true) {
      clearReadCacheByPrefix("study:");
    }
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    const msg = error.message || "查漏补发失败";
    if (msg.includes("已手动停止")) {
      return res.status(409).json({ error: msg });
    }
    res.status(500).json({ error: msg });
  } finally {
    if (runId) endPublishRun(runId);
  }
});

app.post("/api/admin/published/stop-current", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const stopped = requestCancelPublishRun();
    res.json({
      ok: true,
      stopped,
      running: Boolean(safeText(PUBLISH_RUN_STATE.currentRunId)),
      action: safeText(PUBLISH_RUN_STATE.currentAction),
      runId: safeText(PUBLISH_RUN_STATE.currentRunId),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "停止发布失败" });
  }
});

app.get("/admin/publish-coverage", (_req, res) => {
  res.type("html").send(`<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>发布覆盖校验</title>
  <style>
    body { font-family: -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; margin: 0; padding: 16px; background:#faf7f1; color:#3b342c; }
    .wrap { max-width: 1200px; margin: 0 auto; }
    h1 { margin: 0 0 12px; font-size: 24px; }
    .toolbar { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-bottom: 12px; }
    select, button { height: 36px; border-radius: 10px; border: 1px solid #d8ccb8; background: #fffdf8; padding: 0 12px; font-size: 14px; }
    button { cursor:pointer; font-weight:600; }
    .summary { background:#fff; border:1px solid #e8decf; border-radius:12px; padding:12px; margin-bottom:12px; line-height:1.7; }
    table { width:100%; border-collapse: collapse; background:#fff; border:1px solid #e8decf; border-radius:12px; overflow:hidden; }
    th, td { border-bottom:1px solid #f0e8db; padding:8px 10px; text-align:left; vertical-align:top; font-size:13px; }
    th { background:#f9f4ea; font-weight:700; }
    .ok { color:#1e7d32; font-weight:600; }
    .warn { color:#a05a00; font-weight:600; }
    .missing { color:#8a6f55; }
    .muted { color:#8f7e6b; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>发布覆盖校验</h1>
    <div class="toolbar">
      <label>版本 <select id="versionSelect"></select></label>
      <label>语言 <select id="langSelect"></select></label>
      <button id="reloadBtn" type="button">刷新</button>
    </div>
    <div id="summary" class="summary muted">加载中...</div>
    <table>
      <thead>
        <tr>
          <th>书卷</th>
          <th>已发布</th>
          <th>总章数</th>
          <th>覆盖率</th>
          <th>缺失章节</th>
        </tr>
      </thead>
      <tbody id="rows"></tbody>
    </table>
  </div>
  <script>
    const versionEl = document.getElementById("versionSelect");
    const langEl = document.getElementById("langSelect");
    const summaryEl = document.getElementById("summary");
    const rowsEl = document.getElementById("rows");
    const reloadBtn = document.getElementById("reloadBtn");

    function fmtMissing(arr) {
      if (!arr || !arr.length) return '<span class="ok">无</span>';
      const text = arr.length > 30 ? arr.slice(0, 30).join(", ") + " ..." : arr.join(", ");
      return '<span class="missing">' + text + '</span>';
    }

    function coverageAuthToken() {
      try {
        var q = new URLSearchParams(location.search).get("token");
        if (q) return q;
        return localStorage.getItem("bible_user_auth_token_v1") || "";
      } catch (e) { return ""; }
    }

    async function loadBootstrap() {
      var tok = coverageAuthToken();
      var res = await fetch("/api/admin/bootstrap", {
        cache: "no-store",
        headers: tok ? { Authorization: "Bearer " + tok } : {},
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "bootstrap 失败");

      const versions = (data.contentVersions || []).filter(x => x.enabled);
      const langs = (data.languages || []).filter(x => x.enabled && x.contentEnabled !== false);

      versionEl.innerHTML = versions.map(v => '<option value="' + v.id + '">' + (v.label || v.id) + "</option>").join("");
      langEl.innerHTML = langs.map(l => '<option value="' + l.id + '">' + (l.label || l.id) + "</option>").join("");
    }

    async function loadCoverage() {
      const version = versionEl.value;
      const lang = langEl.value;
      if (!version || !lang) return;

      summaryEl.textContent = "加载中...";
      rowsEl.innerHTML = "";

      const params = new URLSearchParams({ version, lang });
      var tok2 = coverageAuthToken();
      const res = await fetch("/api/admin/published/overview?" + params.toString(), {
        cache: "no-store",
        headers: tok2 ? { Authorization: "Bearer " + tok2 } : {},
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "读取覆盖率失败");

      const s = data.summary || {};
      const pct = s.totalPublishedChapters && (s.totalPublishedChapters + s.totalMissingChapters)
        ? Math.round((s.totalPublishedChapters / (s.totalPublishedChapters + s.totalMissingChapters)) * 100)
        : 0;
      summaryEl.innerHTML =
        "版本: <b>" + version + "</b> ｜ 语言: <b>" + lang + "</b><br>" +
        "有内容书卷: <b>" + (s.booksWithAnyPublished || 0) + "/" + (s.totalBooks || 0) + "</b> ｜ " +
        "已发布章节: <b class='ok'>" + (s.totalPublishedChapters || 0) + "</b> ｜ " +
        "缺失章节: <b class='warn'>" + (s.totalMissingChapters || 0) + "</b> ｜ " +
        "整体覆盖率: <b>" + pct + "%</b>";

      rowsEl.innerHTML = (data.books || []).map(book => {
        const published = Number(book.publishedCount || 0);
        const total = Number(book.totalChapters || 0);
        const percent = total ? Math.round((published / total) * 100) : 0;
        const name = (book.bookCn || book.bookEn || book.bookId) + " (" + book.bookId + ")";
        return "<tr>" +
          "<td>" + name + "</td>" +
          "<td>" + published + "</td>" +
          "<td>" + total + "</td>" +
          "<td>" + percent + "%</td>" +
          "<td>" + fmtMissing(book.missingChapters || []) + "</td>" +
          "</tr>";
      }).join("");
    }

    reloadBtn.addEventListener("click", () => loadCoverage().catch(e => {
      summaryEl.textContent = e.message || String(e);
    }));
    versionEl.addEventListener("change", () => loadCoverage().catch(e => { summaryEl.textContent = e.message || String(e); }));
    langEl.addEventListener("change", () => loadCoverage().catch(e => { summaryEl.textContent = e.message || String(e); }));

    (async () => {
      try {
        await loadBootstrap();
        await loadCoverage();
      } catch (e) {
        summaryEl.textContent = e.message || String(e);
      }
    })();
  </script>
</body>
</html>`);
});

app.get("/admin/questions-review", (req, res) => {
  const authed = getAuthedUserFromReq(req);
  if (!authed || !hasPermission(authed, "review_questions")) {
    return res
      .status(403)
      .type("html")
      .send("<!doctype html><html><body style='font-family:sans-serif;padding:20px;'>无权限访问审核页，请使用管理员账号登录。</body></html>");
  }
  res.type("html").send(`<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>好问题审核</title>
  <style>
    body { font-family: -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; margin:0; padding:16px; background:#faf7f1; color:#3b342c; }
    .wrap { max-width: 1000px; margin:0 auto; }
    h1 { margin:0 0 12px; }
    .toolbar { display:flex; gap:8px; margin-bottom:12px; }
    button, select { height:34px; border:1px solid #d8ccb8; border-radius:10px; background:#fffdf8; padding:0 10px; }
    .item { background:#fff; border:1px solid #e8decf; border-radius:12px; padding:12px; margin-bottom:10px; }
    .meta { color:#8f7e6b; font-size:12px; margin-bottom:6px; }
    .q { font-size:16px; line-height:1.6; margin-bottom:8px; }
    .actions { display:flex; gap:8px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>好问题审核</h1>
    <div class="toolbar">
      <select id="statusSel">
        <option value="pending">待审核</option>
        <option value="approved">已通过</option>
        <option value="rejected">已拒绝</option>
        <option value="all">全部</option>
      </select>
      <button id="reloadBtn" type="button">刷新</button>
    </div>
    <div id="list">加载中...</div>
  </div>
  <script>
    const token = new URLSearchParams(location.search).get("token") || "";
    const authHeaders = token ? { Authorization: "Bearer " + token } : {};
    const listEl = document.getElementById("list");
    const statusSel = document.getElementById("statusSel");
    const reloadBtn = document.getElementById("reloadBtn");
    async function review(id, action) {
      const res = await fetch("/api/admin/questions/review", {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders },
        body: JSON.stringify({ id, action })
      });
      const data = await res.json();
      if (!res.ok) { alert(data.error || "审核失败"); return; }
      await load();
    }
    async function load() {
      const status = statusSel.value;
      const res = await fetch("/api/admin/questions/submissions?status=" + encodeURIComponent(status), {
        cache: "no-store",
        headers: authHeaders
      });
      const data = await res.json();
      if (!res.ok) { listEl.textContent = data.error || "加载失败"; return; }
      const items = Array.isArray(data.items) ? data.items : [];
      if (!items.length) { listEl.textContent = "暂无记录"; return; }
      listEl.innerHTML = items.map(item => {
        const meta = [item.bookId, item.chapter ? (item.chapter + "章") : "", item.contentLang || "", item.status || ""].filter(Boolean).join(" / ");
        const canReview = item.status === "pending";
        const reasons = Array.isArray(item.moderationReasons) ? item.moderationReasons.filter(Boolean) : [];
        return '<div class="item">' +
          '<div class="meta">' + meta + ' · ' + (item.createdAt || "") + '</div>' +
          '<div class="q">' + String(item.questionText || "").replaceAll("<","&lt;").replaceAll(">","&gt;") + '</div>' +
          (item.note ? '<div class="meta">备注：' + String(item.note).replaceAll("<","&lt;").replaceAll(">","&gt;") + '</div>' : '') +
          (reasons.length ? '<div class="meta">风控：' + reasons.map(x => String(x).replaceAll("<","&lt;").replaceAll(">","&gt;")).join("；") + '</div>' : '') +
          '<div class="actions">' +
            (canReview ? '<button data-act="approve" data-id="' + item.id + '">通过</button><button data-act="reject" data-id="' + item.id + '">拒绝</button>' : '') +
          '</div>' +
        '</div>';
      }).join("");
      listEl.querySelectorAll("button[data-act]").forEach(btn => {
        btn.addEventListener("click", () => review(btn.getAttribute("data-id"), btn.getAttribute("data-act")));
      });
    }
    reloadBtn.addEventListener("click", load);
    statusSel.addEventListener("change", load);
    load();
  </script>
</body>
</html>`);
});

/* SEO：robots / 站点地图（自托管请设 PUBLIC_SITE_URL=https://你的域名 勿带末尾斜杠） */
function getPublicSiteOrigin() {
  const raw = safeText(process.env.PUBLIC_SITE_URL || "https://askbible.me");
  return raw.replace(/\/+$/, "");
}

app.get("/robots.txt", (req, res) => {
  const origin = getPublicSiteOrigin();
  res.type("text/plain; charset=utf-8");
  res.setHeader("Cache-Control", "public, max-age=3600");
  res.send(
    [
      "User-agent: *",
      "Allow: /",
      "",
      "Disallow: /api/",
      "",
      `Sitemap: ${origin}/sitemap.xml`,
      "",
    ].join("\n")
  );
});

app.get("/sitemap.xml", (req, res) => {
  const origin = getPublicSiteOrigin();
  const paths = [
    { path: "/", priority: "1.0", changefreq: "weekly" },
    { path: "/why.html", priority: "0.9", changefreq: "weekly" },
    { path: "/download.html", priority: "0.85", changefreq: "monthly" },
    { path: "/vision.html", priority: "0.75", changefreq: "monthly" },
  ];
  const lastmod = new Date().toISOString().slice(0, 10);
  const escXml = (s) =>
    String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  const urlEntries = paths
    .map(({ path: p, priority, changefreq }) => {
      const loc = p === "/" ? `${origin}/` : `${origin}${p}`;
      return (
        `  <url>\n` +
        `    <loc>${escXml(loc)}</loc>\n` +
        `    <lastmod>${lastmod}</lastmod>\n` +
        `    <changefreq>${changefreq}</changefreq>\n` +
        `    <priority>${priority}</priority>\n` +
        `  </url>`
      );
    })
    .join("\n");
  const xml =
    `<?xml version="1.0" encoding="UTF-8"?>\n` +
    `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n` +
    `${urlEntries}\n` +
    `</urlset>`;
  res.type("application/xml; charset=utf-8");
  res.setHeader("Cache-Control", "public, max-age=3600");
  res.send(xml);
});

/** 供静态页判断是否注入 LiveReload：仅 DEV_LIVE_RELOAD=1 时为 true，避免 npm start 时请求 35729 报错 */
app.get("/api/dev/livereload-status", (_req, res) => {
  res.set("Cache-Control", "no-store");
  res.json({ enabled: process.env.DEV_LIVE_RELOAD === "1" });
});

/* 管理工具单页：须在登录且具备权限时才能 GET（早于 express.static） */
(() => {
  const rootDir = __dirname;
  const adminToolHtml = [
    ["admin-hub.html", false],
    ["site-chrome.html", false],
    ["promo-edit.html", false],
    ["color-themes.html", false],
    ["admin-analytics.html", false],
    ["seo-settings.html", false],
    ["home-layout-map.html", false],
    ["video-center.html", false],
    ["illustration-admin.html", false],
    ["bible-character-designer.html", false],
    ["chapter-key-people.html", false],
    ["generated-png-thumbs.html", false],
  ];
  for (const [name, qianOnly] of adminToolHtml) {
    app.get(`/${name}`, (req, res) => {
      sendAdminToolHtmlPage(req, res, path.join(rootDir, name), qianOnly);
    });
    app.get(`/dist-capacitor/${name}`, (req, res) => {
      sendAdminToolHtmlPage(
        req,
        res,
        path.join(rootDir, "dist-capacitor", name),
        qianOnly
      );
    });
  }
})();

/* 章节插画 / 人物立绘 PNG：URL /generated/...，目录见 GENERATED_ASSETS_DIR 或 public/generated */
app.use(
  "/generated",
  express.static(CHAPTER_ILLUSTRATION_GENERATED_DIR, {
    maxAge: 0,
    etag: true,
    setHeaders(res, filePath) {
      const ext = path.extname(String(filePath || "")).toLowerCase();
      if (ext === ".png" || ext === ".jpg" || ext === ".jpeg" || ext === ".webp" || ext === ".svg") {
        res.setHeader(
          "Cache-Control",
          "no-store, no-cache, max-age=0, must-revalidate"
        );
      }
    },
  })
);

/* 静态资源放在所有 /api 路由之后，避免根目录下出现与 /api/... 冲突的路径时被 express.static 抢先返回 HTML */
app.use(
  express.static(__dirname, {
    maxAge: 0,
    etag: true,
    setHeaders(res, filePath) {
      const ext = path.extname(String(filePath || "")).toLowerCase();
      const base = path.basename(String(filePath || "")).toLowerCase();
      const noStoreExt = new Set([".html", ".js", ".css", ".webmanifest", ".json"]);
      if (noStoreExt.has(ext) || base === "sw.js") {
        res.setHeader(
          "Cache-Control",
          "no-store, no-cache, max-age=0, must-revalidate"
        );
        return;
      }
      res.setHeader("Cache-Control", "public, max-age=0, must-revalidate");
    },
  })
);

const port = Number(process.env.PORT) || 3000;
/* 默认 0.0.0.0：本机 localhost 与局域网其它设备均可访问；仅本机时用 LISTEN_HOST=127.0.0.1 */
const listenHost =
  process.env.LISTEN_HOST !== undefined && process.env.LISTEN_HOST !== ""
    ? process.env.LISTEN_HOST
    : "0.0.0.0";

/**
 * 生产环境：必须显式配置创作 JSON 与出图目录（持久卷），禁止依赖仓库内默认路径。
 * 不做「相对 __dirname 必须在树外」校验：部分平台把代码也放在数据盘前缀下，会误判。
 */
function assertProductionExternalCreativeLayout() {
  if (String(process.env.NODE_ENV || "").trim() !== "production") return;
  const genRaw = String(process.env.GENERATED_ASSETS_DIR || "").trim();
  const creRaw = String(process.env.CHARACTER_DATA_DIR || "").trim();
  const missing = [];
  if (!genRaw) missing.push("GENERATED_ASSETS_DIR");
  if (!creRaw) missing.push("CHARACTER_DATA_DIR");
  if (missing.length) {
    console.error(
      "[fatal] NODE_ENV=production 时必须设置（Render 等请在 Environment 或 blueprint 中配置）:",
      missing.join(", ")
    );
    console.error(
      "  示例: GENERATED_ASSETS_DIR=/var/data/generated_png CHARACTER_DATA_DIR=/var/data/creative_runtime_data"
    );
    process.exit(1);
  }
  if (process.platform !== "win32") {
    if (!genRaw.startsWith("/")) {
      console.error("[fatal] GENERATED_ASSETS_DIR 须为绝对路径，当前:", genRaw);
      process.exit(1);
    }
    if (!creRaw.startsWith("/")) {
      console.error("[fatal] CHARACTER_DATA_DIR 须为绝对路径，当前:", creRaw);
      process.exit(1);
    }
  } else {
    const absWin = (s) => /^[A-Za-z]:[\\/]/.test(s) || s.startsWith("\\\\");
    if (!absWin(genRaw)) {
      console.error("[fatal] GENERATED_ASSETS_DIR 须为绝对路径，当前:", genRaw);
      process.exit(1);
    }
    if (!absWin(creRaw)) {
      console.error("[fatal] CHARACTER_DATA_DIR 须为绝对路径，当前:", creRaw);
      process.exit(1);
    }
  }
  const genAbs = path.resolve(CHAPTER_ILLUSTRATION_GENERATED_DIR);
  const creativeAbs = path.resolve(CHARACTER_DATA_DIR);
  const defaultGenAbs = path.resolve(path.join(__dirname, "public", "generated"));
  const adminDataAbs = path.resolve(ADMIN_DIR);
  if (genAbs === defaultGenAbs) {
    console.error(
      "[fatal] GENERATED_ASSETS_DIR 不能指向仓库内 public/generated，请设持久卷路径:",
      genAbs
    );
    process.exit(1);
  }
  if (creativeAbs === adminDataAbs) {
    console.error(
      "[fatal] CHARACTER_DATA_DIR 不能指向仓库内 admin_data，请设独立持久目录:",
      creativeAbs
    );
    process.exit(1);
  }
}

assertProductionExternalCreativeLayout();

app.listen(port, listenHost, () => {
  console.log(`http://localhost:${port}/`);
  if (CHARACTER_DATA_DIR) {
    console.log("[creative-data] 人像与插画创作数据目录（持久卷）:", CHARACTER_DATA_DIR);
  }
  console.log("[generated-assets] 出图 PNG 目录:", CHAPTER_ILLUSTRATION_GENERATED_DIR);
  if (characterProfilesUsesSqlite()) {
    console.log(
      "[character-profiles] SQLite:",
      getCharacterProfilesDbPathForLog()
    );
  }
  console.log(
    "[routes] 插画 GPT 文案: POST /api/chapter-illustration/gpt-copy；自检 GET /api/chapter-illustration/gpt-copy-probe"
  );
  if (listenHost === "0.0.0.0") {
    console.log(
      "[listen] 0.0.0.0:" +
        port +
        "（手机/同网设备请用本机局域网 IP，例如 http://192.168.x.x:" +
        port +
        "/）"
    );
  }
  startJobRunner();
  if (process.env.DEV_LIVE_RELOAD === "1") {
    import("livereload")
      .then((mod) => {
        const lr = mod.default.createServer({
          delay: 200,
          extraExts: ["json", "webmanifest", "svg"],
          exclusions: [
            /\.git\//,
            /node_modules\//,
            /admin_data\/auth\.sqlite/,
            /admin_data\/analytics\.sqlite/,
          ],
        });
        lr.watch(__dirname);
        console.log(
          "[dev] LiveReload 已开启：修改 HTML/CSS/JS 等文件后会自动刷新浏览器（请使用 npm run dev 启动）"
        );
      })
      .catch((err) => {
        console.warn("[dev] LiveReload 启动失败:", err?.message || err);
      });
  }
});
