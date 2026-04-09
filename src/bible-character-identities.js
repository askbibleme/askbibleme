const SPECIAL_IDENTITY_RULES = Object.freeze([
  Object.freeze({
    bookId: "GEN",
    displayNameZh: "法老",
    profileKey: "法老（约瑟时代）",
  }),
  Object.freeze({
    bookId: "EXO",
    displayNameZh: "法老",
    profileKey: "法老（出埃及记）",
  }),
  Object.freeze({
    bookId: "GEN",
    displayNameZh: "亚比米勒",
    profileKey: "亚比米勒（族长时代）",
  }),
  Object.freeze({
    bookId: "JDG",
    displayNameZh: "亚比米勒",
    profileKey: "亚比米勒（士师时代）",
  }),
  Object.freeze({
    bookId: "MAT",
    displayNameZh: "希律",
    chapterFrom: 1,
    chapterTo: 2,
    profileKey: "希律（大帝）",
  }),
  Object.freeze({
    bookId: "LUK",
    displayNameZh: "希律",
    chapterFrom: 1,
    chapterTo: 2,
    profileKey: "希律（大帝）",
  }),
  Object.freeze({
    bookId: "MAT",
    displayNameZh: "希律",
    chapterFrom: 3,
    chapterTo: 28,
    profileKey: "希律（安提帕）",
  }),
  Object.freeze({
    bookId: "MRK",
    displayNameZh: "希律",
    profileKey: "希律（安提帕）",
  }),
  Object.freeze({
    bookId: "LUK",
    displayNameZh: "希律",
    chapterFrom: 3,
    chapterTo: 23,
    profileKey: "希律（安提帕）",
  }),
  Object.freeze({
    bookId: "ACT",
    displayNameZh: "希律",
    profileKey: "希律（亚基帕一世）",
  }),
  Object.freeze({
    bookId: "LUK",
    displayNameZh: "凯撒",
    chapterFrom: 2,
    chapterTo: 2,
    profileKey: "凯撒（奥古斯都）",
  }),
  Object.freeze({
    bookId: "ACT",
    displayNameZh: "凯撒",
    profileKey: "凯撒（罗马皇帝）",
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
