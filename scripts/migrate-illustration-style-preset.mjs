#!/usr/bin/env node
/**
 * 把章节插画状态中的旧 stylePreset 迁移为新的古典烛光油画 preset。
 *
 * 默认 dry-run（只打印不写入）；
 * 传 --write 才会落盘。
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const targetFile = path.join(root, "admin_data", "chapter_illustration_states.json");

const FROM = "biblical_copperplate_engraving";
const TO = "biblical_candlelit_oil_painting";
const doWrite = process.argv.includes("--write");

function nowStamp() {
  const d = new Date();
  const p = (n) => String(n).padStart(2, "0");
  return (
    d.getFullYear() +
    p(d.getMonth() + 1) +
    p(d.getDate()) +
    "-" +
    p(d.getHours()) +
    p(d.getMinutes()) +
    p(d.getSeconds())
  );
}

if (!fs.existsSync(targetFile)) {
  console.error("未找到文件：", targetFile);
  process.exit(1);
}

const raw = fs.readFileSync(targetFile, "utf8");
let data;
try {
  data = JSON.parse(raw);
} catch (e) {
  console.error("JSON 解析失败：", e?.message || e);
  process.exit(1);
}

const chapters =
  data && data.chapters && typeof data.chapters === "object" ? data.chapters : {};

let changed = 0;
let total = 0;
for (const [key, st] of Object.entries(chapters)) {
  if (!st || typeof st !== "object") continue;
  total += 1;
  if (
    String(st.stylePreset || "") === FROM ||
    String(st.stylePreset || "") === "biblical_semi_real_character"
  ) {
    st.stylePreset = TO;
    changed += 1;
  }
  if (!st.stylePreset) {
    st.stylePreset = TO;
    changed += 1;
  }
  chapters[key] = st;
}

console.log(`[migrate] scanned: ${total}`);
console.log(`[migrate] changed: ${changed}`);
console.log(`[migrate] mode: ${doWrite ? "write" : "dry-run"}`);

if (!doWrite) {
  console.log("未写入。要落盘请执行：node scripts/migrate-illustration-style-preset.mjs --write");
  process.exit(0);
}

const backup = `${targetFile}.bak-${nowStamp()}`;
fs.copyFileSync(targetFile, backup);
fs.writeFileSync(targetFile, JSON.stringify(data, null, 2) + "\n", "utf8");
console.log("[migrate] backup:", backup);
console.log("[migrate] wrote:", targetFile);
