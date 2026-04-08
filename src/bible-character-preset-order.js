/**
 * 读经页「本章人员」立绘排序：按全本预设名单的叙事先后编号，而非本章 keyPeople 出现顺序。
 * 名单与 bible-character-designer.html 中 CHARACTER_PRESET_BY_BOOK 一致；日后追加经卷时请同步该 HTML 与本文件。
 */

/** 创世记核心人物：顺序即圣经叙事先后（与设计器 GENESIS_CORE_CHARACTERS 一致） */
export const GENESIS_CORE_CHARACTERS = [
  "亚当",
  "夏娃",
  "该隐",
  "亚伯",
  "挪亚",
  "亚伯拉罕",
  "撒拉",
  "罗得",
  "夏甲",
  "以实玛利",
  "以撒",
  "利百加",
  "以扫",
  "雅各",
  "拉班",
  "利亚",
  "拉结",
  "犹大",
  "约瑟",
  "便雅悯",
  "法老",
  "波提乏",
  "波提乏的妻子",
];

/** 经卷顺序 → 卷内姓名顺序；全书编号 = 按数组拼接后的下标 */
export const CHARACTER_PRESET_BY_BOOK = [
  {
    bookId: "GEN",
    bookNameZh: "创世记",
    names: GENESIS_CORE_CHARACTERS.slice(),
  },
];

function buildBibleCharacterRosterOrderIndex() {
  const m = Object.create(null);
  let i = 0;
  for (const book of CHARACTER_PRESET_BY_BOOK) {
    const names = Array.isArray(book.names) ? book.names : [];
    for (const raw of names) {
      const zh = String(raw || "").trim();
      if (!zh || Object.prototype.hasOwnProperty.call(m, zh)) continue;
      m[zh] = i++;
    }
  }
  return m;
}

export const BIBLE_CHARACTER_ROSTER_ORDER_INDEX = buildBibleCharacterRosterOrderIndex();

/** 有全书编号者按编号升序；其余按中文 locale 排在后面 */
export function compareZhNamesByBibleRosterOrder(a, b) {
  const sa = String(a || "").trim();
  const sb = String(b || "").trim();
  const ia = BIBLE_CHARACTER_ROSTER_ORDER_INDEX[sa];
  const ib = BIBLE_CHARACTER_ROSTER_ORDER_INDEX[sb];
  const ha = ia !== undefined;
  const hb = ib !== undefined;
  if (ha && hb) return ia - ib;
  if (ha && !hb) return -1;
  if (!ha && hb) return 1;
  return sa.localeCompare(sb, "zh-Hans-CN");
}
