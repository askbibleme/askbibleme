#!/usr/bin/env node
/**
 * 从 SVG（默认 assets/icons/icon-maskable.svg）生成：
 * - Web/PWA：assets/icons/icon-{16,32,180,192,512}.png（66/108 安全边 + 底色 #4A443F，与 refresh-app-icons.sh 一致）
 * - Android：mipmap-* 下 ic_launcher_foreground.png（满版）与 ic_launcher / ic_launcher_round（留边），与 generate-android-launcher-icons.sh 一致
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import sharp from "sharp";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const SRC =
  process.argv[2] ||
  path.join(ROOT, "assets", "icons", "icon-maskable.svg");
const OUT_ICONS = path.join(ROOT, "assets", "icons");
const RES = path.join(ROOT, "android", "app", "src", "main", "res");
const IOS_APPICON = path.join(
  ROOT,
  "ios",
  "App",
  "App",
  "Assets.xcassets",
  "AppIcon.appiconset"
);

/** 与 scripts/refresh-app-icons.sh / Android 脚本中的垫色一致 */
const PAD = { r: 0x4a, g: 0x44, b: 0x3f, alpha: 1 };

function assertSrc() {
  if (!fs.existsSync(SRC)) {
    console.error("缺少源 SVG:", SRC);
    process.exit(1);
  }
}

/**
 * 内容缩至约 66/108，再垫成 size×size（与 sips 流程等价）
 */
function paddedSquarePipeline(size) {
  const inner = Math.max(1, Math.floor((size * 66) / 108));
  const padTop = Math.floor((size - inner) / 2);
  const padLeft = Math.floor((size - inner) / 2);
  const padBottom = size - inner - padTop;
  const padRight = size - inner - padLeft;
  return sharp(SRC)
    .resize(inner, inner)
    .extend({
      top: padTop,
      bottom: padBottom,
      left: padLeft,
      right: padRight,
      background: PAD,
    })
    .png();
}

async function writeWebIcons() {
  fs.mkdirSync(OUT_ICONS, { recursive: true });
  const sizes = [16, 32, 180, 192, 512];
  for (const s of sizes) {
    const out = path.join(OUT_ICONS, `icon-${s}.png`);
    await paddedSquarePipeline(s).toFile(out);
    console.log("write", path.relative(ROOT, out));
  }
}

async function writeIosAppIcon() {
  if (!fs.existsSync(IOS_APPICON)) {
    console.log("跳过 iOS AppIcon（未找到 AppIcon.appiconset）");
    return;
  }
  const out = path.join(IOS_APPICON, "AppIcon-512@2x.png");
  await paddedSquarePipeline(1024).toFile(out);
  console.log("write", path.relative(ROOT, out));
}

async function writeAndroidMipmaps() {
  const densities = [
    ["mdpi", 108, 48],
    ["hdpi", 162, 72],
    ["xhdpi", 216, 96],
    ["xxhdpi", 324, 144],
    ["xxxhdpi", 432, 192],
  ];
  for (const [d, fg, lg] of densities) {
    const dir = path.join(RES, `mipmap-${d}`);
    fs.mkdirSync(dir, { recursive: true });
    const fgPath = path.join(dir, "ic_launcher_foreground.png");
    await sharp(SRC).resize(fg, fg).png().toFile(fgPath);
    console.log("write", path.relative(ROOT, fgPath));

    const inner = Math.max(1, Math.floor((lg * 66) / 108));
    const padTop = Math.floor((lg - inner) / 2);
    const padLeft = Math.floor((lg - inner) / 2);
    const padBottom = lg - inner - padTop;
    const padRight = lg - inner - padLeft;
    const launcherPath = path.join(dir, "ic_launcher.png");
    await sharp(SRC)
      .resize(inner, inner)
      .extend({
        top: padTop,
        bottom: padBottom,
        left: padLeft,
        right: padRight,
        background: PAD,
      })
      .png()
      .toFile(launcherPath);
    const roundPath = path.join(dir, "ic_launcher_round.png");
    fs.copyFileSync(launcherPath, roundPath);
    console.log("write", path.relative(ROOT, launcherPath));
  }
}

async function main() {
  assertSrc();
  console.log("源:", SRC);
  await writeWebIcons();
  await writeIosAppIcon();
  if (fs.existsSync(path.join(ROOT, "android", "app", "src", "main", "res"))) {
    await writeAndroidMipmaps();
  } else {
    console.log("跳过 Android mipmap（未找到 android/app/src/main/res）");
  }
  console.log("完成。");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
