#!/usr/bin/env node
/**
 * 将静态页同步到 dist-capacitor（cap sync 前执行：npm run cap:copy-web）
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const dest = path.join(root, "dist-capacitor");

const topFiles = [
  "index.html",
  "article-studio.html",
  "notebook.html",
  "blank-page.html",
  "styles.css",
  "main.js",
  "pwa-install-hint.js",
  "manifest.webmanifest",
  "sw.js",
];

fs.mkdirSync(dest, { recursive: true });

for (const f of topFiles) {
  const src = path.join(root, f);
  if (!fs.existsSync(src)) continue;
  fs.copyFileSync(src, path.join(dest, f));
  console.log("copy", f);
}

const dirs = ["assets"];
for (const d of dirs) {
  const src = path.join(root, d);
  const out = path.join(dest, d);
  if (!fs.existsSync(src)) continue;
  fs.cpSync(src, out, { recursive: true });
  console.log("copy", d + "/");
}

console.log("dist-capacitor 已更新（若仍 404，请部署含 /api/article-studio 的 server.js）。");
