#!/usr/bin/env bash
# 用 source-app-icon.png 生成网站/PWA PNG；180/192/512 按 66/108 留边（maskable 安全区）。
# 16/32 同比例略缩，避免 favicon 贴边。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="${1:-$ROOT/assets/icons/source-app-icon.png}"
OUT="$ROOT/assets/icons"
PAD_HEX="4A443F"
if [[ ! -f "$SRC" ]]; then
  echo "缺少源图: $SRC"
  exit 1
fi

write_icon_padded() {
  local s="$1"
  local inner=$(( s * 66 / 108 ))
  [[ "$inner" -lt 1 ]] && inner=1
  local tmp="$OUT/_tmp_icon_${s}.png"
  sips -z "$inner" "$inner" "$SRC" --out "$tmp"
  sips -p "$s" "$s" --padColor "$PAD_HEX" "$tmp" --out "$OUT/icon-${s}.png"
  rm -f "$tmp"
}

MASTER="$OUT/_master-1024.png"
sips -z 1024 1024 "$SRC" --out "$MASTER"

write_icon_padded 180
write_icon_padded 192
write_icon_padded 512

for s in 16 32; do
  inner=$(( s * 66 / 108 ))
  [[ "$inner" -lt 1 ]] && inner=1
  tmp="$OUT/_tmp_icon_${s}.png"
  sips -z "$inner" "$inner" "$MASTER" --out "$tmp"
  sips -p "$s" "$s" --padColor "$PAD_HEX" "$tmp" --out "$OUT/icon-${s}.png"
  rm -f "$tmp"
done

rm -f "$MASTER"
echo "已写入 icon-{16,32,180,192,512}.png（66/108 安全边 + 底色 #${PAD_HEX}）"
