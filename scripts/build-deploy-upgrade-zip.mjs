#!/usr/bin/env node
/**
 * 与 server.js 内 buildPackageZip({ kind: "upgrade" }) 规则对齐，
 * 生成本地 upgrade zip，供线上 POST /api/admin/deploy/upload + /apply 使用。
 *
 * 用法: node scripts/build-deploy-upgrade-zip.mjs [版本标签，可选]
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import AdmZip from "adm-zip";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

function shouldSkipPackageRelPath(rel, kind = "upgrade") {
  const normalized = String(rel || "").replaceAll("\\", "/");
  if (!normalized) return true;
  const commonSkips = [".git/", ".cursor/", ".DS_Store", "node_modules/"];
  if (commonSkips.some((p) => normalized.startsWith(p))) return true;
  if (normalized === "admin_data/system_secrets.json") return true;
  if (normalized === ".env" || normalized.startsWith(".env.")) return true;
  if (kind === "upgrade") {
    const upgradeSkips = [
      "admin_data/deploy/",
      "admin_data/auth.db",
      "admin_data/auth/",
      "admin_data/global_favorites.json",
      "admin_data/community_articles.json",
      "admin_data/promo_page.json",
      "admin_data/question_submissions.json",
      /* 与 server.js shouldSkipPackageRelPath(rel, "upgrade") 一致 */
      "content_published/",
      "content_builds/",
      "data/",
      "chapter_videos/",
      "dist-capacitor/",
      "admin_data/jobs/",
      "deploy-builds/",
    ];
    if (upgradeSkips.some((p) => normalized.startsWith(p))) return true;
    if (
      normalized.startsWith("admin_data/auth.sqlite") ||
      normalized.startsWith("admin_data/analytics.sqlite")
    ) {
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
const zipPath = path.join(outDir, `askbible-upgrade-${safeVersion}.zip`);

const zip = new AdmZip();
const rootFiles = walkFiles(root);
let addedCount = 0;

for (const absPath of rootFiles) {
  const rel = path.relative(root, absPath).replaceAll("\\", "/");
  if (shouldSkipPackageRelPath(rel, "upgrade")) continue;
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
        packageKind: "upgrade",
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
  "下一步：打开线上读经页 → 管理面板 →「部署」→ 选择此 zip → 上传 → 应用升级（需部署权限）。"
);
console.log(
  "注意：应用升级不会覆盖服务器上的 admin_data/ 与 node_modules/；站点配置请在线上后台保存。"
);
