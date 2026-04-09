#!/usr/bin/env node
/**
 * 清理「透明主图（hero）规范」之前的人物生成图：旧文件名（空 slot / he / cm / cmp）
 * 以及时间戳早于档案里最早 hero 的 ill-char（且未被 character_illustration_profiles.json 引用）。
 *
 * 不会删除：JSON 中任意字段仍指向的文件。
 *
 *   node scripts/cleanup-legacy-char-generated.mjs --dry-run
 *   node scripts/cleanup-legacy-char-generated.mjs --apply
 *   node scripts/cleanup-legacy-char-generated.mjs --apply --no-before-ts   # 只按旧 slot 删，不按时间
 *   node scripts/cleanup-legacy-char-generated.mjs --apply --before-ts=1775602363777
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

const PROFILES_PATH = path.join(
  root,
  "admin_data",
  "character_illustration_profiles.json"
);

const LEGACY_SLOTS = new Set(["he", "cm", "cmp"]);

function parseArgs(argv) {
  let apply = false;
  let withDist = true;
  let useBeforeTs = true;
  let beforeTsOverride = null;
  let dir = path.join(root, "public", "generated");
  for (const a of argv) {
    if (a === "--apply") apply = true;
    else if (a === "--dry-run" || a === "-n") apply = false;
    else if (a === "--no-dist") withDist = false;
    else if (a === "--no-before-ts") useBeforeTs = false;
    else if (a.startsWith("--before-ts=")) {
      const n = Number(a.slice(12));
      if (Number.isFinite(n) && n > 0) beforeTsOverride = n;
    } else if (a.startsWith("--dir=")) dir = path.resolve(a.slice(6));
  }
  return { apply, withDist, useBeforeTs, beforeTsOverride, dir };
}

function collectReferencedBasenames() {
  const refs = new Set();
  if (!fs.existsSync(PROFILES_PATH)) return refs;
  const walk = (node) => {
    if (node == null) return;
    if (typeof node === "string") {
      const i = node.indexOf("/generated/");
      if (i >= 0) {
        const rest = node.slice(i + "/generated/".length);
        if (rest && !rest.includes("/") && !rest.includes("..")) refs.add(rest);
      }
      return;
    }
    if (Array.isArray(node)) {
      for (const x of node) walk(x);
      return;
    }
    if (typeof node === "object") {
      for (const v of Object.values(node)) walk(v);
    }
  };
  try {
    const data = JSON.parse(fs.readFileSync(PROFILES_PATH, "utf8"));
    walk(data.characters || data);
  } catch {
    /* ignore */
  }
  return refs;
}

/** 档案里已保存的 hero 图里最小时间戳，作为「透明主图时代」起点 */
function minHeroTsFromRefs(refs) {
  let min = Infinity;
  for (const name of refs) {
    const m = name.match(/^ill-char-.+-hero-(\d{13,})\.png$/);
    if (m) min = Math.min(min, Number(m[1]));
  }
  return min === Infinity ? null : min;
}

function parseIllCharFilename(filename) {
  const m = filename.match(/^ill-char-(.+)-(\d{13,})\.png$/);
  if (!m) return null;
  const rest = m[1];
  const ts = Number(m[2]);
  const lastDash = rest.lastIndexOf("-");
  if (lastDash < 0) return null;
  const en = rest.slice(0, lastDash);
  const slot = rest.slice(lastDash + 1);
  return { en, slot, ts };
}

function shouldDeleteIllChar(name, refs, beforeTs) {
  if (refs.has(name)) return { del: false, reason: "referenced" };
  const parsed = parseIllCharFilename(name);
  if (!parsed) return { del: false, reason: "unparsed" };
  if (parsed.slot === "")
    return { del: true, reason: "empty_nameSlot(--)" };
  if (LEGACY_SLOTS.has(parsed.slot))
    return { del: true, reason: `legacy_slot:${parsed.slot}` };
  if (beforeTs != null && parsed.ts < beforeTs)
    return { del: true, reason: `before_hero_era(ts ${parsed.ts} < ${beforeTs})` };
  return { del: false, reason: "keep" };
}

function scrubJsonUrls(obj, deletedBasenames) {
  let cleared = 0;
  const walk = (node) => {
    if (node == null) return;
    if (typeof node === "string") {
      return;
    }
    if (Array.isArray(node)) {
      for (const x of node) walk(x);
      return;
    }
    if (typeof node === "object") {
      for (const k of Object.keys(node)) {
        const v = node[k];
        if (typeof v === "string") {
          const i = v.indexOf("/generated/");
          if (i >= 0) {
            const rest = v.slice(i + "/generated/".length);
            if (rest && !rest.includes("/") && deletedBasenames.has(rest)) {
              node[k] = "";
              cleared += 1;
            }
          }
        } else walk(v);
      }
    }
  };
  walk(obj);
  return cleared;
}

async function main() {
  const { apply, withDist, useBeforeTs, beforeTsOverride, dir } = parseArgs(
    process.argv.slice(2)
  );
  const distGen = path.join(root, "dist-capacitor", "public", "generated");

  const refs = collectReferencedBasenames();
  let beforeTs = beforeTsOverride;
  if (useBeforeTs && beforeTs == null) {
    beforeTs = minHeroTsFromRefs(refs);
  }
  if (!useBeforeTs) beforeTs = null;

  if (!fs.existsSync(dir)) {
    console.error("目录不存在:", dir);
    process.exit(1);
  }

  const names = fs.readdirSync(dir);
  const candidates = names.filter(
    (n) => n.startsWith("ill-char-") && n.endsWith(".png")
  );
  const toDelete = [];
  for (const name of candidates) {
    const { del, reason } = shouldDeleteIllChar(name, refs, beforeTs);
    if (del) toDelete.push({ name, reason });
  }

  console.log(
    `人物档案引用文件数: ${refs.size}\n` +
      (beforeTs != null
        ? `时间戳阈值（早于则删，且未引用）: ${beforeTs}（档案中最早 hero）\n`
        : "未启用时间戳阈值（仅旧 slot / 空 slot）\n")
  );

  if (!toDelete.length) {
    console.log("没有匹配的 legacy / 史前 ill-char:", dir);
    return;
  }

  console.log(`${apply ? "【将删除】" : "【dry-run】"} ${toDelete.length} 个\n`);
  for (const { name, reason } of toDelete) console.log(`  ${name}  (${reason})`);

  if (!apply) {
    console.log("\n加 --apply 执行删除；加 --no-before-ts 可只按旧 slot 清理。");
    return;
  }

  const basenames = new Set(toDelete.map((x) => x.name));
  let removed = 0;
  for (const { name } of toDelete) {
    try {
      fs.unlinkSync(path.join(dir, name));
      removed += 1;
    } catch (e) {
      console.warn("删除失败", name, e.message || e);
    }
  }
  console.log(`\n已删 ${removed} 个（源目录）`);

  if (withDist && fs.existsSync(distGen)) {
    let d = 0;
    for (const { name } of toDelete) {
      const p = path.join(distGen, name);
      if (fs.existsSync(p)) {
        try {
          fs.unlinkSync(p);
          d += 1;
        } catch (e) {
          console.warn("dist 删除失败", name, e.message || e);
        }
      }
    }
    console.log(`dist-capacitor 同步删除 ${d} 个同名文件`);
  }

  if (fs.existsSync(PROFILES_PATH)) {
    const raw = fs.readFileSync(PROFILES_PATH, "utf8");
    const data = JSON.parse(raw);
    const cleared = scrubJsonUrls(data, basenames);
    if (cleared > 0) {
      fs.writeFileSync(
        PROFILES_PATH,
        JSON.stringify(data, null, 2) + "\n",
        "utf8"
      );
      console.log(`character_illustration_profiles.json 已清空 ${cleared} 处 URL`);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
