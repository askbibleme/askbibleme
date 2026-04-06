#!/usr/bin/env node
/**
 * 与 server.js 内 buildPackageZip({ kind: "full-slim" }) 规则对齐：
 * 整站代码 + admin_data 配置等，不含经文发布目录、圣经源数据、生成构建、任务与 SQLite。
 *
 * 用法: node scripts/build-deploy-full-slim-zip.mjs [版本标签，可选]
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import AdmZip from "adm-zip";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

/** 须与 server.js shouldSkipPackageRelPath(rel, "full-slim") 保持一致 */
function shouldSkipPackageRelPath(rel, kind = "full-slim") {
  const normalized = String(rel || "").replaceAll("\\", "/");
  if (!normalized) return true;
  const commonSkips = [".git/", ".cursor/", ".DS_Store", "node_modules/"];
  if (commonSkips.some((p) => normalized.startsWith(p))) return true;
  if (normalized === "admin_data/system_secrets.json") return true;
  if (normalized === ".env" || normalized.startsWith(".env.")) return true;
  if (kind === "full-slim") {
    const slimSkips = [
      "content_published/",
      "content_builds/",
      "data/",
      "admin_data/jobs/",
      "deploy-builds/",
      "admin_data/deploy/",
    ];
    if (slimSkips.some((p) => normalized.startsWith(p))) return true;
    if (
      normalized.startsWith("admin_data/auth.sqlite") ||
      normalized.startsWith("admin_data/analytics.sqlite")
    ) {
      return true;
    }
    if (normalized === "admin_data/auth.db" || normalized.startsWith("admin_data/auth/")) {
      return true;
    }
  }
  return false;
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

const versionArg = process.argv[2] || `v${Date.now()}`;
const safeVersion =
  String(versionArg).replace(/[^\w.-]+/g, "_") || `v${Date.now()}`;
const outDir = path.join(root, "deploy-builds");
fs.mkdirSync(outDir, { recursive: true });
const zipPath = path.join(outDir, `askbible-full-slim-${safeVersion}.zip`);

const zip = new AdmZip();
const rootFiles = walkFiles(root);
let addedCount = 0;

for (const absPath of rootFiles) {
  const rel = path.relative(root, absPath).replaceAll("\\", "/");
  if (shouldSkipPackageRelPath(rel, "full-slim")) continue;
  if (rel.startsWith("admin_data/deploy/uploads/")) continue;
  if (rel.startsWith("deploy-builds/")) continue;
  zip.addLocalFile(absPath, path.dirname(rel), path.basename(rel));
  addedCount += 1;
}

zip.addFile(
  "version.json",
  Buffer.from(
    JSON.stringify(
      {
        version: safeVersion,
        packageKind: "full-slim",
        generatedAt: new Date().toISOString(),
      },
      null,
      2
    ),
    "utf8"
  )
);

zip.writeZip(zipPath);
console.log("已生成:", zipPath);
console.log("打包文件数:", addedCount + 1, "（含 version.json）");
console.log("");
console.log(
  "说明：不含 content_published、data/、content_builds、admin_data/jobs、auth/analytics SQLite；上传应用后线上 admin_data 与 node_modules 仍受保护不覆盖。"
);
