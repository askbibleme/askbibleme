import dotenv from "dotenv";
dotenv.config();

import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";
import bcrypt from "bcryptjs";
import Database from "better-sqlite3";
import multer from "multer";
import AdmZip from "adm-zip";
import OpenAI from "openai";
import { testamentOptions } from "./src/books.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_ROOT = process.env.DATA_ROOT
  ? path.resolve(process.env.DATA_ROOT)
  : __dirname;

const app = express();
const SERVER_BOOT_TS = Date.now();
const SERVER_BOOT_ISO = new Date(SERVER_BOOT_TS).toISOString();
app.use(express.json({ limit: "10mb" }));

/* 仅匹配「恰好 POST /api」：常见于 nginx proxy_pass 截断子路径，请求未到达 /api/article-studio/chat */
app.post("/api", (req, res) => {
  res.status(400).json({
    error:
      "请求路径不完整（只到了 /api）。正确地址为 POST /api/article-studio/chat。若使用 nginx 反代 Node，请使用：location /api/ { proxy_pass http://127.0.0.1:端口/api/; }（location 与 proxy_pass 均带末尾斜杠），并 reload nginx。",
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

const LANGUAGES_FILE = path.join(ADMIN_DIR, "languages.json");
const SCRIPTURE_VERSIONS_FILE = path.join(ADMIN_DIR, "scripture_versions.json");
const CONTENT_VERSIONS_FILE = path.join(ADMIN_DIR, "content_versions.json");
const PUBLISHED_FILE = path.join(ADMIN_DIR, "published.json");
const GLOBAL_FAVORITES_FILE = path.join(ADMIN_DIR, "global_favorites.json");
const COMMUNITY_ARTICLES_FILE = path.join(ADMIN_DIR, "community_articles.json");
const PROMO_PAGE_FILE = path.join(ADMIN_DIR, "promo_page.json");
const PROMO_PAGE_BOOTSTRAP_FILE = path.join(ADMIN_DIR, "promo_page.bootstrap.md");
const PROMO_PAGE_MAX_MARKDOWN = 400000;
const QUESTION_SUBMISSIONS_FILE = path.join(
  ADMIN_DIR,
  "question_submissions.json"
);
const QUESTION_CORRECTIONS_FILE = path.join(
  ADMIN_DIR,
  "question_text_corrections.json"
);
const POINTS_CONFIG_FILE = path.join(ADMIN_DIR, "points_config.json");
const OPS_CONFIG_FILE = path.join(ADMIN_DIR, "ops_config.json");
const AUTH_DB_FILE = path.join(ADMIN_DIR, "auth.sqlite");
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
  if (!raw || typeof raw !== "object") return { openaiApiKey: "" };
  return {
    openaiApiKey: safeText(raw.openaiApiKey || ""),
  };
}

function saveSystemSecrets(next) {
  const current = loadSystemSecrets();
  const merged = {
    ...current,
    ...(next || {}),
    openaiApiKey: safeText(next?.openaiApiKey ?? current.openaiApiKey),
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

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const READ_CACHE_TTL_MS = 60 * 1000;
const readApiCache = new Map();
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

const upload = multer({
  dest: DEPLOY_UPLOADS_DIR,
  limits: { fileSize: 1024 * 1024 * 200 },
});

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
  return { markdown: bootstrap, updatedAt: "" };
}

function savePromoPageMarkdown(markdown) {
  const payload = {
    markdown: String(markdown ?? ""),
    updatedAt: nowIso(),
  };
  writeJson(PROMO_PAGE_FILE, payload);
  return payload;
}

function sanitizeChatMessagesForOpenAi(raw) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const m of raw.slice(-48)) {
    const role = safeText(m?.role || "").toLowerCase();
    if (role !== "user" && role !== "assistant") continue;
    const content = safeText(m?.content || "").slice(0, 24000);
    if (!content) continue;
    out.push({ role, content });
  }
  return out;
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

function parseDraftJsonFromAssistant(text) {
  let t = String(text || "").trim();
  const fence = /^```(?:json)?\s*([\s\S]*?)```$/im.exec(t);
  if (fence) t = fence[1].trim();
  const start = t.indexOf("{");
  const end = t.lastIndexOf("}");
  if (start === -1 || end <= start) {
    throw new Error("模型未返回有效 JSON");
  }
  const obj = JSON.parse(t.slice(start, end + 1));
  const title = safeText(obj?.title || "").slice(0, 200);
  const body = safeText(obj?.body || "").slice(0, 50000);
  if (!title || !body) {
    throw new Error("JSON 中缺少 title 或 body");
  }
  return { title, body };
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
  const key = `study:${String(version)}:${String(lang)}:${String(
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

function shouldSkipPackageRelPath(rel, kind = "upgrade") {
  const normalized = String(rel || "").replaceAll("\\", "/");
  if (!normalized) return true;
  const commonSkips = [".git/", ".cursor/", ".DS_Store", "node_modules/"];
  if (commonSkips.some((p) => normalized.startsWith(p))) return true;
  /* 部署包永不打入密钥与本地环境（避免 zip 外流或误传仓库） */
  if (normalized === "admin_data/system_secrets.json") return true;
  if (normalized === ".env" || normalized.startsWith(".env.")) return true;
  if (kind === "upgrade") {
    const upgradeSkips = [
      "admin_data/deploy/",
      "admin_data/auth.db",
      "admin_data/auth/",
      "admin_data/global_favorites.json",
      "admin_data/community_articles.json",
      "admin_data/promo_page.json",
      "admin_data/question_submissions.json",
    ];
    if (upgradeSkips.some((p) => normalized.startsWith(p))) return true;
  }
  return false;
}

function buildPackageZip({ kind, version }) {
  const safeKind = kind === "full" ? "full" : "upgrade";
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
    zip.addLocalFile(srcPath, path.dirname(rel), path.basename(rel));
    included.push(item);
    addedCount += 1;
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
      "SELECT id, name, email, is_admin, admin_role, COALESCE(online_seconds_total, 0) AS online_seconds_total FROM users WHERE id = ? LIMIT 1"
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
      bookEn: book.en || book.cn,
      chapters: book.chapters,
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

function readPublishedContent({ versionId, lang, bookId, chapter }) {
  const filePath = getPublishedContentFilePath({
    versionId,
    lang,
    bookId,
    chapter,
  });
  return readJson(filePath, null);
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
function normalizeStudyContentForSave(input) {
  return {
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
  };
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
  const normalized = normalizeStudyContentForSave(studyContent);

  const filePath = getPublishedContentFilePath({
    versionId: normalized.version,
    lang: normalized.contentLang,
    bookId: normalized.bookId,
    chapter: normalized.chapter,
  });

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
  const targets = resolveTargetsFromPayload(payload);
  const jobId = `job_${Date.now()}`;
  const buildId = createBuildIdForJob(payload);

  const job = {
    id: jobId,
    type: "bulk_generate",
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
    type: "retry_failed",
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
  const autoPublished = job.autoPublish ? "已自动逐章合并发布" : "未自动发布";
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

      const { savedContent } = saveStudyContentToBuild(result, latestJob.buildId);

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
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "bootstrap 失败" });
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

app.get("/api/study-content", (req, res) => {
  try {
    const { version, lang, bookId, chapter } = req.query;

    if (!version || !lang || !bookId || !chapter) {
      return res.status(400).json({
        error: "缺少 version / lang / bookId / chapter",
      });
    }

    const cacheKey = `study:${String(version)}:${String(lang)}:${String(
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
      /* 200 + 标记：避免浏览器对「未发布章节」报 404 控制台噪音（非错误状态） */
      return res.json({ missing: true });
    }

    data = applyPresetQuestionCorrectionsToStudyPayload(
      data,
      String(bookId),
      Number(chapter),
      String(version),
      String(lang)
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

/** 部署自检：浏览器打开 GET /api/article-studio/ping 应看到 {"ok":true} */
app.get("/api/article-studio/ping", (_req, res) => {
  res.json({ ok: true, service: "article-studio", ts: SERVER_BOOT_ISO });
});

app.post("/api/article-studio/chat", async (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) return res.status(401).json({ error: "请先登录" });
    const cleaned = sanitizeChatMessagesForOpenAi(req.body?.messages);
    if (!cleaned.length) {
      return res.status(400).json({ error: "请至少发送一条消息" });
    }
    const system = `你是 AskBible 的中文信仰写作同伴。用户会和你讨论圣经观、生活与信仰话题。请用温暖、清晰、尊重不同传统的语气回应；避免说教口吻；可适度引用经文思路但不要编造章节号码。回复用简体中文，段落简洁。`;
    const text = await openAiChatHelper({ system, messages: cleaned });
    res.json({ message: text });
  } catch (error) {
    console.error(error);
    const msg = error?.message || "对话失败";
    if (/缺少 OPENAI|API_KEY|401|invalid/i.test(msg)) {
      return res.status(503).json({ error: "服务器未配置可用的 OpenAI Key" });
    }
    res.status(500).json({ error: msg });
  }
});

app.post("/api/article-studio/draft", async (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) return res.status(401).json({ error: "请先登录" });
    const cleaned = sanitizeChatMessagesForOpenAi(req.body?.messages);
    if (!cleaned.length) {
      return res.status(400).json({ error: "没有可整理的对话" });
    }
    const system = `你是中文信仰类短文的编辑。根据用户与助手之间的对话，整理成一篇可单独阅读的短文（非对话体）。
只输出一个 JSON 对象，不要 markdown 代码围栏，不要其它说明。格式严格为：
{"title":"标题不超过30字","body":"正文，可含换行符，300～1800字为宜，分段用\\n"}
正文语气与对话一致，去口语赘字，保留核心观点与例证。`;
    const text = await openAiChatHelper({ system, messages: cleaned });
    const draft = parseDraftJsonFromAssistant(text);
    res.json(draft);
  } catch (error) {
    console.error(error);
    const msg = error?.message || "整理失败";
    if (/缺少 OPENAI|API_KEY|401|invalid/i.test(msg)) {
      return res.status(503).json({ error: "服务器未配置可用的 OpenAI Key" });
    }
    res.status(500).json({ error: msg });
  }
});

app.post("/api/article-studio/publish", (req, res) => {
  try {
    const authed = getAuthedUserFromReq(req);
    if (!authed) return res.status(401).json({ error: "请先登录" });
    const title = safeText(req.body?.title || "").slice(0, 200);
    const body = safeText(req.body?.body || "").slice(0, 50000);
    const columnId = safeText(req.body?.columnId || "communityArticles");
    if (!PUBLISHABLE_ARTICLE_COLUMNS.has(columnId)) {
      return res.status(400).json({ error: "不支持的目标栏目" });
    }
    if (title.length < 2) {
      return res.status(400).json({ error: "标题至少 2 个字" });
    }
    if (body.length < 20) {
      return res.status(400).json({ error: "正文至少约 20 字" });
    }
    const db = loadCommunityArticles();
    const item = {
      id: `ca_${Date.now()}_${crypto.randomBytes(5).toString("hex")}`,
      columnId,
      title,
      body,
      authorId: authed.id,
      authorName: safeText(authed.name || "").slice(0, 80),
      createdAt: nowIso(),
      type: "article",
    };
    const nextItems = [item, ...(db.items || [])].slice(0, MAX_COMMUNITY_ARTICLES);
    saveCommunityArticles({ items: nextItems });
    res.json({ ok: true, item });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "发布失败" });
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
    res.json({ markdown: p.markdown, updatedAt: p.updatedAt });
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
    res.json({ markdown: p.markdown, updatedAt: p.updatedAt });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取宣传页失败" });
  }
});

/** 管理员：保存并发布宣传页 Markdown */
app.post("/api/admin/promo-page", (req, res) => {
  try {
    const authed = requireAdminUser(req, res);
    if (!authed) return;
    let md =
      typeof req.body?.markdown === "string" ? req.body.markdown : "";
    if (md.length > PROMO_PAGE_MAX_MARKDOWN) {
      md = md.slice(0, PROMO_PAGE_MAX_MARKDOWN);
    }
    const payload = savePromoPageMarkdown(md);
    appendAdminAudit(req, authed, "promo_page_save", {
      bytes: payload.markdown.length,
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
      status: "pending",
      reviewedAt: "",
      createdAt: nowIso(),
    };
    const clientMeta = readClientMeta(req);
    item.ipHash = clientMeta.ipHash;
    item.userAgent = clientMeta.userAgent;
    item.deviceId = clientMeta.deviceId;

    const db = loadQuestionSubmissions();
    db.items = [item, ...(db.items || [])];
    saveQuestionSubmissions(db);
    res.json({ ok: true, id: item.id });
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
      .filter((x) => (chapter > 0 ? toSafeNumber(x.chapter, 0) === chapter : true))
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

app.get("/api/admin/deploy/package-command", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_deploy");
    if (!authed) return;
    const kind = safeText(req.query.kind || "upgrade");
    const safeKind = kind === "full" ? "full" : "upgrade";
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
      .filter((x) => (status === "all" ? true : safeText(x.status) === status))
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
app.get("/api/admin/bootstrap", (_req, res) => {
  try {
    res.json({
      languages: loadLanguages().languages || [],
      scriptureVersions: getAllScriptureVersions(),
      contentVersions: loadContentVersions().contentVersions || [],
      published: loadPublished(),
      books: flattenBooks(),
      pointsConfig: loadPointsConfig(),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "后台初始化失败" });
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
          prefixes.add(`study:${String(t.versionId)}:${String(t.lang)}:`);
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
      clearReadCacheByPrefix(`study:${String(version)}:${String(lang)}:`);
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
      `study:${String(version)}:${String(lang)}:${String(bookId)}:${Number(chapter)}`
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
        `study:${String(version)}:${String(lang)}:${String(bookId)}:${Number(chapter)}`
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

    async function loadBootstrap() {
      const res = await fetch("/api/admin/bootstrap", { cache: "no-store" });
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
      const res = await fetch("/api/admin/published/overview?" + params.toString(), { cache: "no-store" });
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
        return '<div class="item">' +
          '<div class="meta">' + meta + ' · ' + (item.createdAt || "") + '</div>' +
          '<div class="q">' + String(item.questionText || "").replaceAll("<","&lt;").replaceAll(">","&gt;") + '</div>' +
          (item.note ? '<div class="meta">备注：' + String(item.note).replaceAll("<","&lt;").replaceAll(">","&gt;") + '</div>' : '') +
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
    { path: "/promo.html", priority: "0.9", changefreq: "weekly" },
    { path: "/download.html", priority: "0.85", changefreq: "monthly" },
    { path: "/notebook.html", priority: "0.85", changefreq: "weekly" },
    { path: "/vision.html", priority: "0.75", changefreq: "monthly" },
    { path: "/article-studio.html", priority: "0.7", changefreq: "weekly" },
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

/* 静态资源放在所有 /api 路由之后，避免根目录下出现与 /api/... 冲突的路径时被 express.static 抢先返回 HTML */
app.use(express.static(__dirname));

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`http://localhost:${port}`);
  startJobRunner();
  if (process.env.DEV_LIVE_RELOAD === "1") {
    import("livereload")
      .then((mod) => {
        const lr = mod.default.createServer({
          delay: 200,
          extraExts: ["json", "webmanifest", "svg"],
          exclusions: [/\.git\//, /node_modules\//, /admin_data\/auth\.sqlite/],
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
