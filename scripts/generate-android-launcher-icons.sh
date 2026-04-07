#!/usr/bin/env bash
# 从 assets/icons/source-app-icon.png 生成 Android mipmap 启动器图标。
# 自适应前景：满版位图 + ic_launcher_foreground_inset.xml（21dp 安全边）。
# 旧版 ic_launcher / round：按 66/108 比例缩小后居中垫色，避免贴边裁切。
set -eo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/assets/icons/source-app-icon.png"
if [[ ! -f "$SRC" ]]; then
  echo "缺少 $SRC"
  exit 1
fi
RES="$ROOT/android/app/src/main/res"
PAD_HEX="4A443F"

gen_for_density() {
  local d="$1" fg="$2" lg="$3"
  local dir="$RES/mipmap-${d}"
  mkdir -p "$dir"
  # 自适应前景层：满画布（安全区由 drawable/ic_launcher_foreground_inset.xml 处理）
  sips -z "$fg" "$fg" "$SRC" --out "$dir/ic_launcher_foreground.png"
  # 旧版启动器图标：内容缩至约 66/108，再垫成正方形
  local inner=$(( lg * 66 / 108 ))
  [[ "$inner" -lt 1 ]] && inner=1
  sips -z "$inner" "$inner" "$SRC" --out "$dir/_tmp_launcher.png"
  sips -p "$lg" "$lg" --padColor "$PAD_HEX" "$dir/_tmp_launcher.png" --out "$dir/ic_launcher.png"
  cp "$dir/ic_launcher.png" "$dir/ic_launcher_round.png"
  rm -f "$dir/_tmp_launcher.png"
}

gen_for_density mdpi 108 48
gen_for_density hdpi 162 72
gen_for_density xhdpi 216 96
gen_for_density xxhdpi 324 144
gen_for_density xxxhdpi 432 192

echo "已更新 mipmap（前景满版+XML inset；旧版图标已留安全边）"
