#!/usr/bin/env node
/**
 * 删除 public/generated 中非透明产物：
 * - PNG：sharp metadata hasAlpha === false
 * - .jpg / .jpeg：一律删除（不透明位图）
 * - .webp：无 alpha 则删除
 *
 * 可选从 dist-capacitor/public/generated 删除同名文件。
 * 可选扫描 admin_data/character_illustration_profiles.json，把指向已删文件的 URL 置空。
 *
 *   node scripts/cleanup-nontransparent-generated.mjs --dry-run
 *   node scripts/cleanup-nontransparent-generated.mjs --apply
 *   node scripts/cleanup-nontransparent-generated.mjs --apply --no-dist
 *   node scripts/cleanup-nontransparent-generated.mjs --apply --no-json
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import sharp from "sharp";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

const DEFAULT_PROFILES = path.join(
  root,
  "admin_data",
  "character_illustration_profiles.json"
);

function parseArgs(argv) {
  let apply = false;
  let withDist = true;
  let withJson = true;
  let dir = path.join(root, "public", "generated");
  for (const a of argv) {
    if (a === "--apply") apply = true;
    else if (a === "--dry-run" || a === "-n") apply = false;
    else if (a === "--no-dist") withDist = false;
    else if (a === "--no-json") withJson = false;
    else if (a.startsWith("--dir=")) dir = path.resolve(a.slice(6));
  }
  return { apply, withDist, withJson, dir };
}

function basenameFromGeneratedRef(s) {
  if (typeof s !== "string") return null;
  const needle = "/generated/";
  const i = s.indexOf(needle);
  if (i < 0) return null;
  const rest = s.slice(i + needle.length);
  if (!rest || rest.includes("/") || rest.includes("..")) return null;
  return rest;
}

function scrubJsonUrls(obj, deletedBasenames) {
  let cleared = 0;
  function walk(node) {
    if (node === null || node === undefined) return;
    if (Array.isArray(node)) {
      for (let i = 0; i < node.length; i++) walk(node[i]);
      return;
    }
    if (typeof node === "object") {
      for (const k of Object.keys(node)) {
        const v = node[k];
        if (typeof v === "string") {
          const base = basenameFromGeneratedRef(v);
          if (base && deletedBasenames.has(base)) {
            node[k] = "";
            cleared += 1;
          }
        } else walk(v);
      }
    }
  }
  walk(obj);
  return cleared;
}

async function classifyFile(fp, name) {
  const lower = name.toLowerCase();
  if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) {
    return { delete: true, reason: "jpeg" };
  }
  if (lower.endsWith(".png") || lower.endsWith(".webp")) {
    try {
      const meta = await sharp(fs.readFileSync(fp)).metadata();
      if (!meta.hasAlpha) return { delete: true, reason: "no_alpha" };
      return { delete: false, reason: "has_alpha" };
    } catch (e) {
      return { delete: false, reason: `read_error:${e.message || e}` };
    }
  }
  return { delete: false, reason: "skip_ext" };
}

async function main() {
  const { apply, withDist, withJson, dir } = parseArgs(process.argv.slice(2));
  const distGen = path.join(root, "dist-capacitor", "public", "generated");

  if (!fs.existsSync(dir)) {
    console.error("目录不存在:", dir);
    process.exit(1);
  }

  const names = fs.readdirSync(dir);
  const toDelete = [];
  for (const name of names) {
    const fp = path.join(dir, name);
    if (!fs.statSync(fp).isFile()) continue;
    const { delete: del, reason } = await classifyFile(fp, name);
    if (del) toDelete.push({ name, reason });
  }

  if (!toDelete.length) {
    console.log("没有需要删除的非透明图:", dir);
    return;
  }

  console.log(
    `${apply ? "【将删除】" : "【dry-run】"} 共 ${toDelete.length} 个文件\n`
  );
  for (const { name, reason } of toDelete) {
    console.log(`  ${name}  (${reason})`);
  }

  const basenames = new Set(toDelete.map((x) => x.name));

  if (!apply) {
    console.log("\n加 --apply 后真正删除磁盘文件并可选更新 JSON。");
    return;
  }

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

  if (withJson && fs.existsSync(DEFAULT_PROFILES)) {
    const raw = fs.readFileSync(DEFAULT_PROFILES, "utf8");
    const data = JSON.parse(raw);
    const cleared = scrubJsonUrls(data, basenames);
    if (cleared > 0) {
      fs.writeFileSync(
        DEFAULT_PROFILES,
        JSON.stringify(data, null, 2) + "\n",
        "utf8"
      );
      console.log(`character_illustration_profiles.json 已清空 ${cleared} 处 URL`);
    } else {
      console.log("character_illustration_profiles.json 无需修改");
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
