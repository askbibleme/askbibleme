/**
 * 人物立绘档案持久化：可选 SQLite，与代码/发布目录解耦（线上专用库路径，不被 git 部署覆盖）。
 * 未设置 CHARACTER_PROFILES_DB 时由 server.js 继续使用 admin_data JSON。
 */
import fs from "fs";
import path from "path";
import Database from "better-sqlite3";

let cachedDb = null;
let cachedDbPath = "";

function resolveDbPath() {
  const raw = String(process.env.CHARACTER_PROFILES_DB || "").trim();
  return raw ? path.resolve(raw) : "";
}

export function characterProfilesUsesSqlite() {
  return Boolean(resolveDbPath());
}

export function getCharacterProfilesDbPathForLog() {
  return resolveDbPath();
}

function openDb(dbPath) {
  if (cachedDb && cachedDbPath === dbPath) return cachedDb;
  if (cachedDb) {
    try {
      cachedDb.close();
    } catch (_) {
      /* ignore */
    }
    cachedDb = null;
    cachedDbPath = "";
  }
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.exec(`
    CREATE TABLE IF NOT EXISTS character_profile_rows (
      zh_name TEXT PRIMARY KEY NOT NULL,
      payload TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  cachedDb = db;
  cachedDbPath = dbPath;
  return db;
}

function getDb() {
  const p = resolveDbPath();
  if (!p) return null;
  return openDb(p);
}

/**
 * @returns {{ characters: Record<string, object> }}
 */
export function loadCharacterProfilesRootFromSqlite() {
  const db = getDb();
  if (!db) return { characters: {} };
  const rows = db.prepare("SELECT zh_name, payload FROM character_profile_rows").all();
  const characters = {};
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const zh = String(r.zh_name || "").trim();
    if (!zh) continue;
    try {
      const obj = JSON.parse(String(r.payload || "{}"));
      if (obj && typeof obj === "object") characters[zh] = obj;
    } catch (_) {
      /* skip bad row */
    }
  }
  return { characters };
}

export function countCharacterProfilesInSqlite() {
  const db = getDb();
  if (!db) return 0;
  const row = db.prepare("SELECT COUNT(1) AS c FROM character_profile_rows").get();
  return Number(row?.c) || 0;
}

/**
 * @param {{ characters?: Record<string, object> }} root
 */
export function saveCharacterProfilesRootToSqlite(root) {
  const db = getDb();
  if (!db) return;
  const chars =
    root && typeof root === "object" && root.characters && typeof root.characters === "object"
      ? root.characters
      : {};
  const now = new Date().toISOString();
  const del = db.prepare("DELETE FROM character_profile_rows");
  const ins = db.prepare(
    "INSERT INTO character_profile_rows (zh_name, payload, updated_at) VALUES (?, ?, ?)"
  );
  const tx = db.transaction(() => {
    del.run();
    for (const [zh, row] of Object.entries(chars)) {
      const key = String(zh || "").trim();
      if (!key || !row || typeof row !== "object") continue;
      ins.run(key, JSON.stringify(row), now);
    }
  });
  tx();
}

/**
 * DB 为空时从种子 JSON 导入（通常为仓库内 admin_data 模板，仅作首次填充）。
 * @returns {number} 导入条数
 */
export function migrateSeedJsonToSqliteIfEmpty(seedAbsPath, logPrefix = "[character-profiles]") {
  const db = getDb();
  if (!db) return 0;
  const n = countCharacterProfilesInSqlite();
  if (n > 0) return 0;
  let seed = { characters: {} };
  try {
    if (seedAbsPath && fs.existsSync(seedAbsPath)) {
      const raw = fs.readFileSync(seedAbsPath, "utf8");
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && parsed.characters && typeof parsed.characters === "object") {
        seed = parsed;
      }
    }
  } catch (e) {
    console.warn(logPrefix, "seed read failed:", e?.message || e);
    return 0;
  }
  const keys = Object.keys(seed.characters || {});
  if (!keys.length) return 0;
  saveCharacterProfilesRootToSqlite(seed);
  console.info(logPrefix, `migrated ${keys.length} profiles from seed JSON into SQLite`);
  return keys.length;
}

export function closeCharacterProfilesDb() {
  if (!cachedDb) return;
  try {
    cachedDb.close();
  } catch (_) {
    /* ignore */
  }
  cachedDb = null;
  cachedDbPath = "";
}
