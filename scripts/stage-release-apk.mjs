#!/usr/bin/env node
/**
 * 将 Android Studio 打好的 release APK 复制到网站可下载目录，并写入 version.json。
 * 前置：Android Studio → Build → Build Bundle(s) / APK(s) → Build APK(s) → release。
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const gradlePath = path.join(root, "android/app/build.gradle");
const apkIn = path.join(
  root,
  "android/app/build/outputs/apk/release/app-release.apk"
);
const dlDir = path.join(root, "downloads");
const apkOut = path.join(dlDir, "askbible-release.apk");
const versionJsonPath = path.join(dlDir, "version.json");

function readGradleVersions() {
  if (!fs.existsSync(gradlePath)) {
    return { versionCode: 1, versionName: "1.0" };
  }
  const text = fs.readFileSync(gradlePath, "utf8");
  const vc = text.match(/versionCode\s+(\d+)/);
  const vn = text.match(/versionName\s+"([^"]+)"/);
  return {
    versionCode: vc ? Number(vc[1]) : 1,
    versionName: vn ? vn[1] : "1.0",
  };
}

if (!fs.existsSync(apkIn)) {
  console.error(
    "未找到 release APK：\n  " +
      apkIn +
      "\n请先在 Android Studio 中构建 release APK（非 AAB）。"
  );
  process.exit(1);
}

fs.mkdirSync(dlDir, { recursive: true });
fs.copyFileSync(apkIn, apkOut);

const { versionCode, versionName } = readGradleVersions();
const meta = {
  file: "askbible-release.apk",
  versionName,
  versionCode,
  updatedAt: new Date().toISOString(),
};

let note = "";
if (fs.existsSync(versionJsonPath)) {
  try {
    const prev = JSON.parse(fs.readFileSync(versionJsonPath, "utf8"));
    if (typeof prev.note === "string" && prev.note.trim()) note = prev.note.trim();
  } catch (_) {}
}
if (note) meta.note = note;

fs.writeFileSync(
  versionJsonPath,
  JSON.stringify(meta, null, 2) + "\n",
  "utf8"
);

console.log("已复制 APK →", apkOut);
console.log("已写入元数据 →", versionJsonPath);
console.log("部署时请把 downloads/askbible-release.apk 与 downloads/version.json 一并上传到站点根目录。");
