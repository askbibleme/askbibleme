/**
 * sharp 的预编译 .node 偶发未解压（空 lib/），插画上传等会报错。
 * postinstall 时若检测到缺失则尝试补装当前平台的 @img/sharp-* 包。
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { execSync } from "child_process";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");

function runtimePlatformArch() {
  let libc = "";
  try {
    const d = require("detect-libc");
    if (d.isNonGlibcLinuxSync()) libc = d.familySync();
  } catch (_) {
    /* sharp 未装完时可能没有 detect-libc */
  }
  return `${process.platform}${libc}-${process.arch}`;
}

function nativeBinaryPresent(runtime) {
  const libDir = path.join(root, "node_modules", "@img", `sharp-${runtime}`, "lib");
  try {
    return fs.readdirSync(libDir).some((f) => f.endsWith(".node"));
  } catch {
    return false;
  }
}

async function sharpLoads() {
  try {
    const { default: sharp } = await import("sharp");
    await sharp({
      create: { width: 1, height: 1, channels: 3, background: "#000" },
    })
      .png()
      .toBuffer();
    return true;
  } catch {
    return false;
  }
}

async function main() {
  const sharpRoot = path.join(root, "node_modules", "sharp");
  if (!fs.existsSync(sharpRoot)) return;

  const runtime = runtimePlatformArch();
  const imgName = `@img/sharp-${runtime}`;

  if (nativeBinaryPresent(runtime) && (await sharpLoads())) return;

  let ver;
  try {
    const sharpPkg = JSON.parse(
      fs.readFileSync(path.join(sharpRoot, "package.json"), "utf8")
    );
    ver = sharpPkg.optionalDependencies?.[imgName];
  } catch {
    return;
  }
  if (!ver) return;

  const imgDir = path.join(root, "node_modules", "@img", `sharp-${runtime}`);
  console.warn(`[postinstall] sharp: repairing native addon ${imgName}@${ver} ...`);
  try {
    fs.rmSync(imgDir, { recursive: true, force: true });
    execSync(`npm install --no-save --include=optional ${imgName}@${ver}`, {
      cwd: root,
      stdio: "inherit",
    });
  } catch {
    console.warn(
      "[postinstall] sharp: repair failed. Try: npm run fix:sharp"
    );
    return;
  }

  if (!(await sharpLoads())) {
    console.warn("[postinstall] sharp: still not loading after repair.");
  }
}

await main();
