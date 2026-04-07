/**
 * 将 bible-character-designer 生成的 ill-char-*.png 按最新时间戳合并进
 * admin_data/character_illustration_profiles.json（heroImageUrl / comparisonSheetUrl）。
 *
 * 命名规则（与 server.js generate-illustration 一致）：
 * - ill-char-{En}-he-{ts}.png  → 透明主图
 * - ill-char-{En}-cm-{ts}.png → 并列对比图
 * - ill-char-{En}--{ts}.png   → Abraham 等 8 字截断后 hero/cmp 同前缀；取最新为主图、次新为对比图
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const genDir = path.join(root, "public", "generated");
const profilesPath = path.join(root, "admin_data", "character_illustration_profiles.json");

function readProfiles() {
  const raw = fs.readFileSync(profilesPath, "utf8");
  return JSON.parse(raw);
}

function enToZhMap(characters) {
  const m = new Map();
  for (const [zh, row] of Object.entries(characters || {})) {
    const en = String(row?.englishName || "").trim();
    if (en) m.set(en.toLowerCase(), zh);
  }
  return m;
}

function scanGenerated() {
  /** @type {Map<string, { hero: number[], cmp: number[], amb: number[] }>} */
  const byEn = new Map();
  let names;
  try {
    names = fs.readdirSync(genDir);
  } catch {
    return byEn;
  }
  for (const f of names) {
    if (!f.startsWith("ill-char-") || !f.endsWith(".png")) continue;
    const url = `/generated/${f}`;
    /** @param {RegExpMatchArray} m */
    const pairFrom = (m) => {
      const t = Number(m[2]);
      return Number.isFinite(t) ? { url, t } : null;
    };
    let m = f.match(/^ill-char-([A-Za-z]+)-he-(\d+)\.png$/);
    if (m) {
      const en = m[1];
      const p = pairFrom(m);
      if (!p) continue;
      if (!byEn.has(en)) byEn.set(en, { hero: [], cmp: [], amb: [] });
      byEn.get(en).hero.push(p);
      continue;
    }
    m = f.match(/^ill-char-([A-Za-z]+)-cm-(\d+)\.png$/);
    if (m) {
      const en = m[1];
      const p = pairFrom(m);
      if (!p) continue;
      if (!byEn.has(en)) byEn.set(en, { hero: [], cmp: [], amb: [] });
      byEn.get(en).cmp.push(p);
      continue;
    }
    m = f.match(/^ill-char-([A-Za-z]+)--(\d+)\.png$/);
    if (m) {
      const en = m[1];
      const p = pairFrom(m);
      if (!p) continue;
      if (!byEn.has(en)) byEn.set(en, { hero: [], cmp: [], amb: [] });
      byEn.get(en).amb.push(p);
    }
  }
  return byEn;
}

function maxPair(arr) {
  if (!arr.length) return null;
  return arr.reduce((a, b) => (b.t > a.t ? b : a));
}

function sortDesc(arr) {
  return [...arr].sort((a, b) => b.t - a.t);
}

function main() {
  const data = readProfiles();
  const chars = data.characters && typeof data.characters === "object" ? data.characters : {};
  const enZh = enToZhMap(chars);
  const scanned = scanGenerated();
  const applied = [];

  for (const [en, buckets] of scanned) {
    const zh = enZh.get(en.toLowerCase());
    if (!zh) continue;
    const row = chars[zh];
    if (!row || typeof row !== "object") continue;

    let heroUrl = maxPair(buckets.hero)?.url || null;
    let cmpUrl = maxPair(buckets.cmp)?.url || null;

    if (buckets.amb.length) {
      const sorted = sortDesc(buckets.amb);
      if (!heroUrl) heroUrl = sorted[0]?.url || null;
      if (!cmpUrl && sorted.length >= 2) cmpUrl = sorted[1].url;
      else if (!cmpUrl && sorted.length === 1 && !heroUrl) cmpUrl = sorted[0].url;
    }

    if (heroUrl) {
      row.heroImageUrl = heroUrl;
      applied.push(`${zh}: heroImageUrl ← ${heroUrl}`);
    }
    if (cmpUrl) {
      row.comparisonSheetUrl = cmpUrl;
      applied.push(`${zh}: comparisonSheetUrl ← ${cmpUrl}`);
    }
  }

  fs.writeFileSync(profilesPath, JSON.stringify(data, null, 2) + "\n", "utf8");
  console.log("Wrote", profilesPath);
  if (applied.length) console.log(applied.join("\n"));
  else console.log("No matching ill-char-* files for profiles englishName.");
}

main();
