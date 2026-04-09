#!/usr/bin/env node
/**
 * 根据 admin_data/character_illustration_profiles.json 将旧版 ill-char-*.png
 * 重命名为 ill-char-{EnglishName}-{slot}-{timestamp}.png，并写回 JSON。
 *
 * slot: hero | p0 | p1 | p2 | sheet（sheet = 仅出现在 imageUrl/comparisonSheetUrl、且非 hero/各期）
 *
 * 用法：
 *   node scripts/rename-legacy-char-pngs.mjs              # 仅打印计划
 *   node scripts/rename-legacy-char-pngs.mjs --apply      # 执行重命名 + 写 JSON
 *   node scripts/rename-legacy-char-pngs.mjs --apply --no-dist   # 不处理 dist-capacitor
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const profilesPath = path.join(root, "admin_data", "character_illustration_profiles.json");

function parseArgs(argv) {
  let apply = false;
  let useDist = true;
  for (const a of argv) {
    if (a === "--apply") apply = true;
    if (a === "--no-dist") useDist = false;
  }
  return { apply, useDist };
}

function sanitizeEn(s) {
  return String(s || "")
    .replace(/[^a-zA-Z0-9_-]/g, "")
    .slice(0, 36) || "figure";
}

function normUrl(u) {
  return String(u || "").trim();
}

function basenameOnly(u) {
  const s = normUrl(u);
  if (!s.startsWith("/generated/")) return "";
  return path.basename(s);
}

function extractTs(base) {
  const m = String(base).match(/(\d{12,})\.png$/i);
  return m ? m[1] : String(Date.now());
}

/** @returns {string} */
function desiredBase(en, slot, ts) {
  return `ill-char-${en}-${slot}-${ts}.png`;
}

function collectDirs(useDist) {
  const dirs = [path.join(root, "public", "generated")];
  if (useDist) dirs.push(path.join(root, "dist-capacitor", "public", "generated"));
  return dirs.filter((d) => {
    try {
      return fs.statSync(d).isDirectory();
    } catch {
      return false;
    }
  });
}

function main() {
  const { apply, useDist } = parseArgs(process.argv.slice(2));
  const raw = fs.readFileSync(profilesPath, "utf8");
  const data = JSON.parse(raw);
  const characters = data.profiles?.characters || data.characters;
  if (!characters || typeof characters !== "object") {
    console.error("JSON 中无 profiles.characters");
    process.exit(1);
  }

  /** basename -> { en, slot, ts, zh } */
  const byBase = new Map();

  function claim(base, en, slot, zh) {
    if (!base || !base.startsWith("ill-char") || !base.endsWith(".png")) return;
    const want = desiredBase(en, slot, extractTs(base));
    if (base === want) return;
    const prev = byBase.get(base);
    if (prev) {
      if (prev.en === en && prev.slot === slot) return;
      console.warn(
        `冲突：${base} 已为 ${prev.zh} → ${prev.en}/${prev.slot}，忽略 ${zh} → ${en}/${slot}`
      );
      return;
    }
    byBase.set(base, { en, slot, ts: extractTs(base), zh });
  }

  for (const [zh, row] of Object.entries(characters)) {
    if (!row || typeof row !== "object") continue;
    const en = sanitizeEn(row.englishName);

    const heroB = basenameOnly(row.heroImageUrl);
    if (heroB) claim(heroB, en, "hero", zh);

    if (Array.isArray(row.periods)) {
      row.periods.forEach((p, i) => {
        const b = basenameOnly(p?.imageUrl);
        if (b) claim(b, en, "p" + i, zh);
      });
    }

    const imgB = basenameOnly(row.imageUrl);
    if (imgB && !byBase.has(imgB)) claim(imgB, en, "sheet", zh);

    const cmpB = basenameOnly(row.comparisonSheetUrl);
    if (cmpB && !byBase.has(cmpB)) claim(cmpB, en, "sheet", zh);
  }

  const plans = [];
  for (const [oldBase, meta] of byBase) {
    const newBase = desiredBase(meta.en, meta.slot, meta.ts);
    if (oldBase === newBase) continue;
    plans.push({ oldBase, newBase, ...meta });
  }

  if (!plans.length) {
    console.log("没有需要重命名的 ill-char 条目（或已全部为新格式）。");
    return;
  }

  console.log(`计划重命名 ${plans.length} 个文件名（${apply ? "将写入磁盘与 JSON" : "dry-run"}）:\n`);
  for (const p of plans) {
    console.log(`  ${p.oldBase}  →  ${p.newBase}  (${p.zh} · ${p.slot})`);
  }

  const dirs = collectDirs(useDist);
  console.log("\n目标目录:", dirs.join(", ") || "(无)");

  if (!apply) {
    console.log("\n加 --apply 执行。");
    return;
  }

  /** @type {Map<string, string>} oldBase -> newBase */
  const renameMap = new Map(plans.map((p) => [p.oldBase, p.newBase]));

  function renameInDir(genDir) {
    for (const p of plans) {
      const from = path.join(genDir, p.oldBase);
      const to = path.join(genDir, p.newBase);
      if (!fs.existsSync(from)) continue;
      if (fs.existsSync(to)) {
        console.warn(`跳过（目标已存在）: ${genDir} / ${p.newBase}`);
        continue;
      }
      const tmp = path.join(genDir, `.rename-tmp-${p.ts}-${Math.random().toString(36).slice(2)}.png`);
      fs.renameSync(from, tmp);
      fs.renameSync(tmp, to);
      console.log(`已重命名: ${genDir} / ${p.oldBase} → ${p.newBase}`);
    }
  }

  for (const d of dirs) renameInDir(d);

  let jsonOut = raw;
  for (const [oldB, newB] of renameMap) {
    const from = `/generated/${oldB}`;
    const to = `/generated/${newB}`;
    if (from === to) continue;
    if (!jsonOut.includes(from)) continue;
    jsonOut = jsonOut.split(from).join(to);
  }
  fs.writeFileSync(profilesPath, jsonOut, "utf8");
  console.log("\n已更新:", profilesPath);
}

main();
