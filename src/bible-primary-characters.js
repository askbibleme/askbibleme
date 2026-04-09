const PRIMARY_CHARACTERS_BY_BOOK_RAW = {
  GEN: ["亚当", "夏娃", "亚伯拉罕", "撒拉", "以撒", "利百加", "雅各", "约瑟"],
  EXO: ["摩西", "亚伦", "米利暗", "法老"],
  LEV: ["摩西", "亚伦"],
  NUM: ["摩西", "亚伦", "米利暗", "约书亚", "迦勒", "巴兰"],
  DEU: ["摩西", "约书亚"],
  JOS: ["约书亚", "喇合", "迦勒"],
  JDG: ["底波拉", "基甸", "参孙"],
  RUT: ["路得", "拿俄米", "波阿斯"],
  "1SA": ["撒母耳", "扫罗", "大卫", "约拿单"],
  "2SA": ["大卫", "押沙龙", "拿单"],
  "1KI": ["所罗门", "以利亚", "亚哈", "耶洗别"],
  "2KI": ["以利沙", "希西家", "约西亚"],
  "1CH": ["大卫"],
  "2CH": ["所罗门", "罗波安", "亚撒", "约沙法", "希西家", "约西亚"],
  EZR: ["以斯拉", "所罗巴伯"],
  NEH: ["尼希米", "以斯拉"],
  EST: ["以斯帖", "末底改", "哈曼"],
  JOB: ["约伯"],
  PSA: ["大卫"],
  PRO: ["所罗门"],
  ECC: ["所罗门"],
  SNG: ["所罗门", "书拉密女"],
  ISA: ["以赛亚", "希西家"],
  JER: ["耶利米", "巴录", "西底家"],
  LAM: ["耶利米"],
  EZK: ["以西结"],
  DAN: ["但以理", "尼布甲尼撒", "伯沙撒"],
  HOS: ["何西阿", "歌篾"],
  JOL: ["约珥"],
  AMO: ["阿摩司"],
  OBA: ["俄巴底亚"],
  JON: ["约拿"],
  MIC: ["弥迦"],
  NAM: ["那鸿"],
  HAB: ["哈巴谷"],
  ZEP: ["西番雅"],
  HAG: ["哈该", "所罗巴伯", "约书亚大祭司"],
  ZEC: ["撒迦利亚", "约书亚大祭司", "所罗巴伯"],
  MAL: ["玛拉基"],
  MAT: ["耶稣", "马利亚", "约瑟", "约瑟夫", "施洗约翰", "彼得", "雅各布"],
  MRK: ["耶稣", "彼得", "施洗约翰"],
  LUK: ["耶稣", "马利亚", "撒迦利亚", "伊利莎白", "施洗约翰", "彼得"],
  JHN: ["耶稣", "施洗约翰", "彼得", "马大", "马利亚", "拉撒路"],
  ACT: ["彼得", "司提反", "腓利", "保罗", "巴拿巴", "雅各"],
  ROM: ["保罗"],
  "1CO": ["保罗"],
  "2CO": ["保罗"],
  GAL: ["保罗"],
  EPH: ["保罗"],
  PHP: ["保罗"],
  COL: ["保罗"],
  "1TH": ["保罗"],
  "2TH": ["保罗"],
  "1TI": ["保罗", "提摩太"],
  "2TI": ["保罗", "提摩太"],
  TIT: ["保罗", "提多"],
  PHM: ["保罗", "腓利门", "阿尼西母"],
  HEB: [],
  JAS: ["雅各", "雅各布"],
  "1PE": ["彼得"],
  "2PE": ["彼得"],
  "1JN": ["约翰"],
  "2JN": ["约翰"],
  "3JN": ["约翰"],
  JUD: ["犹大"],
  REV: ["约翰", "耶稣"],
};

export const BIBLE_PRIMARY_CHARACTERS_BY_BOOK = Object.freeze(
  Object.fromEntries(
    Object.entries(PRIMARY_CHARACTERS_BY_BOOK_RAW).map(([bookId, names]) => [
      bookId,
      Object.freeze([...names]),
    ])
  )
);

export const BIBLE_CHARACTER_PRIMARY_BOOK_BY_ZH = Object.freeze(
  Object.entries(BIBLE_PRIMARY_CHARACTERS_BY_BOOK).reduce((acc, [bookId, names]) => {
    for (const zhName of names) {
      if (!acc[zhName]) acc[zhName] = bookId;
    }
    return acc;
  }, {})
);
