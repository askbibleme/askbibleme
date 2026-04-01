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
app.use(express.static(__dirname));

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

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
const QUESTION_SUBMISSIONS_FILE = path.join(
  ADMIN_DIR,
  "question_submissions.json"
);
const POINTS_CONFIG_FILE = path.join(ADMIN_DIR, "points_config.json");
const AUTH_DB_FILE = path.join(ADMIN_DIR, "auth.sqlite");
const LEGACY_USERS_FILE = path.join(ADMIN_DIR, "users.json");
const LEGACY_USER_SESSIONS_FILE = path.join(ADMIN_DIR, "user_sessions.json");
const DEPLOY_DIR = path.join(ADMIN_DIR, "deploy");
const DEPLOY_UPLOADS_DIR = path.join(DEPLOY_DIR, "uploads");
const DEPLOY_RELEASES_DIR = path.join(DEPLOY_DIR, "releases");
const DEPLOY_BACKUPS_DIR = path.join(DEPLOY_DIR, "backups");
const DEPLOY_STATE_FILE = path.join(DEPLOY_DIR, "state.json");

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
};

function normalizeAdminRole(role) {
  const safe = safeText(role || "").toLowerCase();
  if (safe in ADMIN_ROLE_LEVEL) return safe;
  return "";
}

function hasPermission(authedUser, permissionKey) {
  const strictKeys = new Set(["review_questions", "manage_roles"]);
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
  const strictKeys = new Set(["review_questions", "manage_roles"]);
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
        new Date(Date.now() + 1000 * 60 * 60 * 24 * 30).toISOString()
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
  }));
  return data;
}

function saveQuestionSubmissions(data) {
  writeJson(QUESTION_SUBMISSIONS_FILE, data);
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
  if (kind === "upgrade") {
    const upgradeSkips = [
      "admin_data/deploy/",
      "admin_data/auth.db",
      "admin_data/auth/",
      "admin_data/global_favorites.json",
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
    .prepare("SELECT id, name, email, is_admin, admin_role FROM users WHERE id = ? LIMIT 1")
    .get(safeText(hit.user_id || ""));
  if (!user) return null;
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    adminRole: normalizeAdminRole(user.admin_role || ""),
    isAdmin:
      Number(user.is_admin || 0) === 1 ||
      Boolean(normalizeAdminRole(user.admin_role || "")),
    token,
  };
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
  const publishedContent = readJson(publishedPath, null);
  const buildHash = getStudyContentHash(buildContent);
  const publishedHash = publishedContent ? getStudyContentHash(publishedContent) : "";
  const changed = !publishedHash || buildHash !== publishedHash;
  return {
    existsInBuild: true,
    changed,
    buildHash,
    publishedHash,
    sourcePath,
    publishedPath,
    buildContent,
  };
}

function mergePublishFromBuild({
  buildId,
  versionId,
  lang,
  targets,
  onlyChanged = false,
  dryRun = false,
}) {
  let publishedCount = 0;
  let skippedCount = 0;
  const changedTargets = [];
  const skippedTargets = [];

  for (const target of targets) {
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
      });
      continue;
    }
    changedTargets.push({
      bookId: target.bookId,
      chapter: target.chapter,
      changed: isChanged,
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
      changedTargets,
      skippedTargets,
      dryRun: true,
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
    changedTargets,
    skippedTargets,
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
}) {
  if (!process.env.OPENAI_API_KEY) {
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

  const primaryScriptureVersion = getPrimaryScriptureVersionByLang(lang);
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
      response = await client.responses.create({
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
  const autoPublished = job.autoPublish ? "已自动合并发布" : "未自动发布";
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

      saveStudyContentToBuild(result, latestJob.buildId);

      latestJob.done = i + 1;
      latestJob.updatedAt = nowIso();
      latestJob.progressText = `已完成 ${latestJob.done} / ${latestJob.total}`;
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
    const touchedPairs = new Set(
      finalJob.targets.map((x) => `${x.versionId}__${x.lang}`)
    );

    for (const pair of touchedPairs) {
      const [versionId, lang] = pair.split("__");
      mergePublishFromBuild({
        buildId: finalJob.buildId,
        versionId,
        lang,
        targets: finalJob.targets,
      });
    }

    finalJob.progressText += "，并已自动合并发布";
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

function findLatestBuildForChapter({ versionId, lang, bookId, chapter }) {
  const jobs = listAllJobsNewestFirst();

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

  return null;
}

function autoRepublishChapter({
  versionId,
  lang,
  bookId,
  chapter,
  onlyChanged = false,
  dryRun = false,
}) {
  const found = findLatestBuildForChapter({ versionId, lang, bookId, chapter });

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

function republishBulkFromLatestBuilds({
  mode,
  version,
  lang,
  onlyChanged = false,
  dryRun = false,
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

  selectedPairs.forEach((pairKey) => {
    const [pairVersion, pairLang] = pairKey.split("__");
    const job = latestByPair.get(pairKey);
    if (!job) return;

    const result = mergePublishFromBuild({
      buildId: job.buildId,
      versionId: pairVersion,
      lang: pairLang,
      targets: job.targets || [],
      onlyChanged,
      dryRun,
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
  });

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

  setInterval(() => {
    runJobRunnerLoop().catch((error) => {
      console.error("runJobRunnerLoop error:", error);
    });
  }, 2000);
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

    const contentVersions = getEnabledContentVersions().map((x) => ({
      id: x.id,
      label: x.label,
    }));

    const defaultPrimary =
      scriptureVersions.find((x) => x.lang === "zh")?.id ||
      scriptureVersions[0]?.id ||
      "";

    res.json({
      uiLanguages,
      scriptureVersions,
      contentVersions,
      defaultState: {
        uiLang: "zh",
        primaryScriptureVersionId: defaultPrimary,
        secondaryScriptureVersionIds: [],
        contentVersionId: "default",
        contentLang: "zh",
      },
      testamentOptions: flattenBooks(),
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "bootstrap 失败" });
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

    const data = readPublishedContent({
      versionId: String(version),
      lang: String(lang),
      bookId: String(bookId),
      chapter: Number(chapter),
    });

    if (!data) {
      return res.status(404).json({
        error: "未找到已发布内容",
      });
    }

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
    const expiresAt = new Date(Date.now() + 1000 * 60 * 60 * 24 * 30).toISOString();
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
        isAdmin: Number(user.is_admin || 0) === 1 || Boolean(role),
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
    res.json({
      user: {
        id: authed.id,
        name: authed.name,
        email: authed.email,
        isAdmin: authed.isAdmin === true,
        adminRole: normalizeAdminRole(authed.adminRole || ""),
      },
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取登录态失败" });
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
        };
      });
    res.json({ items });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "读取已审核问题失败" });
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
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "审核失败" });
  }
});

app.post("/api/admin/users/set-admin", (req, res) => {
  try {
    const qianCount =
      authDb
        .prepare("SELECT COUNT(1) as c FROM users WHERE admin_role = 'qianfuzhang'")
        .get()?.c || 0;
    if (Number(qianCount) > 0) {
      const authed = requirePermission(req, res, "manage_roles");
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
    const { version, lang, bookId, chapter } = req.body || {};

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
    const { studyContent } = req.body || {};

    if (!studyContent || typeof studyContent !== "object") {
      return res.status(400).json({
        error: "缺少 studyContent",
      });
    }

    const result = saveStudyContentAndPublish(studyContent);

    res.json({
      ok: true,
      message: "测试结果已保存并合并发布",
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
    job.progressText = "任务已取消";
    job.completionSummary = "已取消";
    writeJob(job);

    res.json({ ok: true, job });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "取消任务失败" });
  }
});

/* =========================================================
   手动合并发布
   ========================================================= */
app.post("/api/admin/publish", (req, res) => {
  try {
    const authed = requirePermission(req, res, "manage_publish");
    if (!authed) return;
    const { buildId, version, lang, targets, onlyChanged, dryRun } = req.body || {};
    if (!buildId || !version || !lang || !Array.isArray(targets)) {
      return res.status(400).json({
        error: "缺少 buildId / version / lang / targets",
      });
    }

    const result = mergePublishFromBuild({
      buildId,
      versionId: version,
      lang,
      targets,
      onlyChanged: onlyChanged !== false,
      dryRun: dryRun === true,
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

app.post("/api/admin/published/republish-bulk", (req, res) => {
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

    const result = republishBulkFromLatestBuilds({
      mode: safeMode,
      version: safeText(version),
      lang: safeText(lang),
      onlyChanged: onlyChanged !== false,
      dryRun: dryRun === true,
    });
    if (dryRun !== true) {
      clearReadCacheByPrefix("study:");
    }
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: error.message || "整本发布失败" });
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

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`http://localhost:${port}`);
  startJobRunner();
});
