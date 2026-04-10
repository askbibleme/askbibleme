const SPECIAL_IDENTITY_RULES = Object.freeze([
  Object.freeze({
    bookId: "GEN",
    displayNameZh: "法老",
    profileKey: "法老（约瑟时代）",
    note: "约瑟进入埃及时期的埃及王",
  }),
  Object.freeze({
    bookId: "EXO",
    displayNameZh: "法老",
    profileKey: "法老（出埃及记）",
    note: "摩西出埃及时期的埃及王",
  }),
  Object.freeze({
    bookId: "GEN",
    displayNameZh: "亚比米勒",
    profileKey: "亚比米勒（族长时代）",
    note: "亚伯拉罕、以撒叙事中的非利士王号",
  }),
  Object.freeze({
    bookId: "JDG",
    displayNameZh: "亚比米勒",
    profileKey: "亚比米勒（士师时代）",
    note: "基甸之子亚比米勒",
  }),
  Object.freeze({
    bookId: "MAT",
    displayNameZh: "希律",
    chapterFrom: 1,
    chapterTo: 2,
    profileKey: "希律（大帝）",
    note: "耶稣降生时期的希律大帝",
  }),
  Object.freeze({
    bookId: "LUK",
    displayNameZh: "希律",
    chapterFrom: 1,
    chapterTo: 2,
    profileKey: "希律（大帝）",
    note: "施洗约翰出生背景中的希律大帝时期",
  }),
  Object.freeze({
    bookId: "MAT",
    displayNameZh: "希律",
    chapterFrom: 3,
    chapterTo: 28,
    profileKey: "希律（安提帕）",
    note: "福音书中审问施洗约翰、耶稣时期多指希律安提帕",
  }),
  Object.freeze({
    bookId: "MRK",
    displayNameZh: "希律",
    profileKey: "希律（安提帕）",
    note: "马可福音中的希律多指安提帕",
  }),
  Object.freeze({
    bookId: "LUK",
    displayNameZh: "希律",
    chapterFrom: 3,
    chapterTo: 23,
    profileKey: "希律（安提帕）",
    note: "路加福音后段多指希律安提帕",
  }),
  Object.freeze({
    bookId: "ACT",
    displayNameZh: "希律",
    profileKey: "希律（亚基帕一世）",
    note: "使徒行传早段迫害教会的希律亚基帕一世",
  }),
  Object.freeze({
    bookId: "LUK",
    displayNameZh: "凯撒",
    chapterFrom: 2,
    chapterTo: 2,
    profileKey: "凯撒（奥古斯都）",
    note: "路加福音耶稣降生背景中的凯撒奥古斯都",
  }),
  Object.freeze({
    bookId: "ACT",
    displayNameZh: "凯撒",
    profileKey: "凯撒（罗马皇帝）",
    note: "使徒行传中保罗上诉语境下的罗马皇帝称号",
  }),
]);

const SPECIAL_IDENTITIES_BY_PROFILE_KEY = Object.freeze(
  Object.fromEntries(
    SPECIAL_IDENTITY_RULES.map((rule) => [
      rule.profileKey,
      Object.freeze({
        bookId: rule.bookId,
        profileKey: rule.profileKey,
        displayNameZh: rule.displayNameZh,
      }),
    ])
  )
);

function matchSpecialIdentityRule(bookId, rawName, chapter = null) {
  const bid = String(bookId || "").trim().toUpperCase();
  const name = String(rawName || "").trim();
  const ch = Number(chapter);
  for (const rule of SPECIAL_IDENTITY_RULES) {
    if (String(rule.bookId || "").trim().toUpperCase() !== bid) continue;
    if (String(rule.displayNameZh || "").trim() !== name) continue;
    const from = Number(rule.chapterFrom);
    const to = Number(rule.chapterTo);
    if (Number.isFinite(from) && Number.isFinite(to) && Number.isFinite(ch)) {
      if (ch < from || ch > to) continue;
    }
    return rule;
  }
  return null;
}

export function resolveCharacterIdentity(bookId, rawName, chapter = null) {
  const name = String(rawName || "").trim();
  const bid = String(bookId || "").trim().toUpperCase();
  if (!name) {
    return { profileKey: "", displayNameZh: "", sourceBookId: bid };
  }
  const byKey = SPECIAL_IDENTITIES_BY_PROFILE_KEY[name];
  if (byKey) {
    return {
      profileKey: byKey.profileKey,
      displayNameZh: byKey.displayNameZh,
      sourceBookId: byKey.bookId || bid,
    };
  }
  const special = matchSpecialIdentityRule(bid, name, chapter);
  if (special && typeof special === "object") {
    return {
      profileKey: String(special.profileKey || name).trim(),
      displayNameZh: String(special.displayNameZh || name).trim(),
      sourceBookId: bid,
    };
  }
  return { profileKey: name, displayNameZh: name, sourceBookId: bid };
}

export function displayNameForProfileKey(profileKey, sourceBookId = "") {
  return resolveCharacterIdentity(sourceBookId, profileKey).displayNameZh || String(profileKey || "").trim();
}

export function buildRelatedBookIdsByProfile(primaryByBook) {
  const out = Object.create(null);
  Object.entries(primaryByBook || {}).forEach(([bookId, names]) => {
    (Array.isArray(names) ? names : []).forEach((rawName) => {
      const identity = resolveCharacterIdentity(bookId, rawName);
      const key = String(identity.profileKey || "").trim();
      const bid = String(bookId || "").trim().toUpperCase();
      if (!key || !bid) return;
      if (!out[key]) out[key] = [];
      if (out[key].indexOf(bid) === -1) out[key].push(bid);
    });
  });
  return out;
}

export function buildPrimaryCharacterEntriesByBook(primaryByBook) {
  return Object.fromEntries(
    Object.entries(primaryByBook || {}).map(([bookId, names]) => [
      bookId,
      (Array.isArray(names) ? names : []).map((rawName) => {
        const identity = resolveCharacterIdentity(bookId, rawName);
        return Object.freeze({
          profileKey: String(identity.profileKey || "").trim(),
          displayNameZh: String(identity.displayNameZh || rawName || "").trim(),
          sourceBookId: String(identity.sourceBookId || bookId || "").trim().toUpperCase(),
        });
      }),
    ])
  );
}

export function listSpecialIdentityRules() {
  return SPECIAL_IDENTITY_RULES.map((rule) => ({
    bookId: String(rule.bookId || "").trim().toUpperCase(),
    displayNameZh: String(rule.displayNameZh || "").trim(),
    profileKey: String(rule.profileKey || "").trim(),
    chapterFrom:
      Number.isFinite(Number(rule.chapterFrom)) ? Number(rule.chapterFrom) : null,
    chapterTo:
      Number.isFinite(Number(rule.chapterTo)) ? Number(rule.chapterTo) : null,
    note: String(rule.note || "").trim(),
  }));
}
