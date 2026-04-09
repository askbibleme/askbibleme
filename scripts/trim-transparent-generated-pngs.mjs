#!/usr/bin/env node
/**
 * 批量对已有 PNG 做与 server.js /api/generate-illustration 相同的透明边裁切（Alpha trim）。
 *
 * 用法：
 *   node scripts/trim-transparent-generated-pngs.mjs --all-generated    # public + dist-capacitor 两处
 *   node scripts/trim-transparent-generated-pngs.mjs
 *   node scripts/trim-transparent-generated-pngs.mjs --dry-run
 *   node scripts/trim-transparent-generated-pngs.mjs --dir=dist-capacitor/public/generated
 *   node scripts/trim-transparent-generated-pngs.mjs --prefix=ill-char
 *
 * 环境变量（与 server 一致）：
 *   TRANSPARENT_PNG_TRIM_THRESHOLD  默认 14
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import sharp from "sharp";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

const DEFAULT_GEN_DIRS = [
  path.join(root, "public", "generated"),
  path.join(root, "dist-capacitor", "public", "generated"),
];

function parseArgs(argv) {
  let dryRun = false;
  let singleDir = null;
  let allGenerated = false;
  let prefix = "";
  for (const a of argv) {
    if (a === "--dry-run" || a === "-n") dryRun = true;
    else if (a === "--all-generated" || a === "--all") allGenerated = true;
    else if (a.startsWith("--dir=")) singleDir = path.resolve(a.slice(6));
    else if (a.startsWith("--prefix=")) prefix = String(a.slice(9) || "").trim();
  }
  if (process.env.DRY_RUN === "1") dryRun = true;

  let dirs;
  if (singleDir) dirs = [singleDir];
  else if (allGenerated) dirs = [...DEFAULT_GEN_DIRS];
  else dirs = [DEFAULT_GEN_DIRS[0]];

  return { dryRun, dirs, prefix };
}

async function trimTransparentPngBuffer(buf) {
  try {
    const meta = await sharp(buf).metadata();
    const w0 = meta.width || 0;
    const h0 = meta.height || 0;
    if (w0 < 8 || h0 < 8) return { buf, changed: false, reason: "too_small" };
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
    if (w < 32 || h < 64) return { buf, changed: false, reason: "trim_too_small" };
    if (w > w0 || h > h0) return { buf, changed: false, reason: "trim_invalid" };
    const changed = w !== w0 || h !== h0;
    return { buf: changed ? trimmed : buf, changed, w0, h0, w, h };
  } catch (e) {
    return { buf, changed: false, reason: String(e.message || e) };
  }
}

async function processDirectory(dir, dryRun, prefix) {
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch (e) {
    console.warn("跳过目录（无法读取）:", dir, e.message || e);
    return { ok: 0, skipped: 0, unchanged: 0, failed: 0, total: 0 };
  }
  const pngs = names.filter((f) => {
    if (!f.endsWith(".png")) return false;
    if (prefix && !f.startsWith(prefix)) return false;
    return true;
  });
  if (!pngs.length) {
    console.log("未找到匹配的 PNG:", dir, prefix ? `prefix=${prefix}` : "");
    return { ok: 0, skipped: 0, unchanged: 0, failed: 0, total: 0 };
  }
  console.log(
    `\n======== ${dir} ========\n文件数: ${pngs.length}\n${dryRun ? "【dry-run 不写盘】" : "将覆盖写入"}\nthreshold=${Number.isFinite(Number(process.env.TRANSPARENT_PNG_TRIM_THRESHOLD)) ? process.env.TRANSPARENT_PNG_TRIM_THRESHOLD : 14}\n`
  );
  let ok = 0;
  let skipped = 0;
  let unchanged = 0;
  let failed = 0;
  for (const name of pngs) {
    const fp = path.join(dir, name);
    let raw;
    try {
      raw = fs.readFileSync(fp);
    } catch (e) {
      console.warn("跳过（读取失败）", name, e.message || e);
      failed += 1;
      continue;
    }
    const meta = await sharp(raw).metadata();
    if (!meta.hasAlpha) {
      skipped += 1;
      continue;
    }
    const { buf, changed, w0, h0, w, h } = await trimTransparentPngBuffer(raw);
    if (!changed) {
      unchanged += 1;
      continue;
    }
    ok += 1;
    console.log(`更新 ${name}  ${w0}×${h0} → ${w}×${h}`);
    if (!dryRun) fs.writeFileSync(fp, buf);
  }
  console.log(
    `小计：已收紧 ${ok}，未变 ${unchanged}，无 alpha 跳过 ${skipped}，失败 ${failed}`
  );
  return { ok, skipped, unchanged, failed, total: pngs.length };
}

async function main() {
  const { dryRun, dirs, prefix } = parseArgs(process.argv.slice(2));
  let tOk = 0,
    tSkip = 0,
    tUn = 0,
    tFail = 0;
  for (const dir of dirs) {
    if (!fs.existsSync(dir)) {
      console.log("跳过（目录不存在）:", dir);
      continue;
    }
    const s = await processDirectory(dir, dryRun, prefix);
    tOk += s.ok;
    tSkip += s.skipped;
    tUn += s.unchanged;
    tFail += s.failed;
  }
  console.log(
    `\n======== 合计 ========\n已收紧 ${tOk}，未变 ${tUn}，无 alpha 跳过 ${tSkip}，失败 ${tFail}`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
