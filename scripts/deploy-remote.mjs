#!/usr/bin/env node
/**
 * 向线上站点上传升级 zip 并立即「应用升级」。
 *
 * 用法:
 *   ASKBIBLE_DEPLOY_URL="https://你的域名" ASKBIBLE_ADMIN_TOKEN="会话token" \
 *     node scripts/deploy-remote.mjs deploy-builds/askbible-upgrade-xxx.zip
 *
 * 或在项目根目录 .env 中配置 ASKBIBLE_DEPLOY_URL、ASKBIBLE_ADMIN_TOKEN 后执行 npm run deploy:remote -- …
 *
 * TOKEN：与后台页面请求里 Authorization: Bearer … 相同（管理员登录后的会话 token）。
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
dotenv.config({ path: path.join(root, ".env") });

const base = String(process.env.ASKBIBLE_DEPLOY_URL || "")
  .trim()
  .replace(/\/$/, "");
const token = String(process.env.ASKBIBLE_ADMIN_TOKEN || "").trim();
const zipArg = process.argv[2];

if (!base || !token || !zipArg) {
  console.error(`
用法:
  ASKBIBLE_DEPLOY_URL="https://你的域名" ASKBIBLE_ADMIN_TOKEN="Bearer后的token" \\
    node scripts/deploy-remote.mjs deploy-builds/askbible-upgrade-xxx.zip

或在项目根目录:
  npm run deploy:remote -- deploy-builds/askbible-upgrade-xxx.zip
`);
  process.exit(1);
}

const zipPath = path.isAbsolute(zipArg) ? zipArg : path.join(root, zipArg);
if (!fs.existsSync(zipPath)) {
  console.error("找不到 zip:", zipPath);
  process.exit(1);
}

const buf = fs.readFileSync(zipPath);
const name = path.basename(zipPath);
const form = new FormData();
form.append("package", new Blob([buf], { type: "application/zip" }), name);

console.log("上传:", name, "→", base);
const up = await fetch(`${base}/api/admin/deploy/upload`, {
  method: "POST",
  headers: { Authorization: "Bearer " + token },
  body: form,
});
const upJson = await up.json().catch(() => ({}));
if (!up.ok) {
  console.error("上传失败:", up.status, upJson.error || upJson);
  process.exit(1);
}
const uploadId = upJson.uploadId;
if (!uploadId) {
  console.error("上传响应缺少 uploadId:", upJson);
  process.exit(1);
}
console.log("上传成功 uploadId:", uploadId, "version:", upJson.version || "");

console.log("应用升级…");
const ap = await fetch(`${base}/api/admin/deploy/apply`, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    Authorization: "Bearer " + token,
  },
  body: JSON.stringify({ uploadId }),
});
const apJson = await ap.json().catch(() => ({}));
if (!ap.ok) {
  console.error("应用失败:", ap.status, apJson.error || apJson);
  process.exit(1);
}
console.log("应用成功 version:", apJson.version || "", "backupId:", apJson.backupId || "");
console.log("");
console.log(
  "提示：应用升级不会覆盖线上 admin_data/ 与 node_modules/。若需更新 character_illustration_profiles.json，请在服务器上单独替换或通过后台保存。"
);
