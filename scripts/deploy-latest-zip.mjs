#!/usr/bin/env node
/**
 * 自动选用 deploy-builds 里修改时间最新的 askbible-upgrade-*.zip，调用 deploy-remote.mjs 上传并应用。
 * 需 .env 中 ASKBIBLE_DEPLOY_URL、ASKBIBLE_ADMIN_TOKEN（与 scripts/deploy-remote.mjs 相同）。
 */
import { spawnSync } from "child_process";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
dotenv.config({ path: path.join(root, ".env") });

const dir = path.join(root, "deploy-builds");
if (!fs.existsSync(dir)) {
  console.error("缺少目录 deploy-builds，请先执行: npm run build:deploy-zip -- 你的版本标签");
  process.exit(1);
}

const files = fs
  .readdirSync(dir)
  .filter((f) => f.startsWith("askbible-upgrade-") && f.endsWith(".zip"));
if (!files.length) {
  console.error("deploy-builds 下没有 askbible-upgrade-*.zip，请先 npm run build:deploy-zip");
  process.exit(1);
}

const sorted = files
  .map((f) => ({
    name: f,
    mtime: fs.statSync(path.join(dir, f)).mtimeMs,
  }))
  .sort((a, b) => b.mtime - a.mtime);

const zipRel = path.join("deploy-builds", sorted[0].name);
const zipAbs = path.join(root, zipRel);

console.log("使用最新包:", zipRel, "\n");

const r = spawnSync(process.execPath, [path.join(__dirname, "deploy-remote.mjs"), zipAbs], {
  cwd: root,
  stdio: "inherit",
  env: process.env,
});
process.exit(r.status ?? 1);
