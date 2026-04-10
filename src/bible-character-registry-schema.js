/**
 * 人物档案「稳定规则」：分级、身份类型、完成度、高风险易混名清单。
 * 与 character_illustration_profiles.json 行内可选字段对齐；缺省时由 server 在加载时补全/推导。
 */
import { listSpecialIdentityRules } from "./bible-character-identities.js";

export const CHARACTER_TIER = Object.freeze({
  /** 核心：须完整档 + 主图等 */
  CORE: "core",
  /** 次要：可先简档 */
  SECONDARY: "secondary",
});

export const IDENTITY_TYPE = Object.freeze({
  /** 同一人多卷共用同一档案（bookIds 多卷） */
  SHARED: "shared",
  /** 同称号不同人：如法老、希律 — 多 profileKey 共用 displayNameZh */
  TITLE_SPLIT: "title_split",
  /** 单卷或单叙事线内唯一指称 */
  SINGLE_BOOK: "single_book",
});

/** 加载/保存时写入 row.profileCompletionStatus */
export const PROFILE_COMPLETION_STATUS = Object.freeze({
  UNPROFILED: "unprofiled",
  PROFILED: "profiled",
  HAS_COPY: "has_copy",
  HAS_HERO: "has_hero",
  COMPLETE: "complete",
});

function s(v) {
  return String(v ?? "").trim();
}

/**
 * 次要人物最低字段（人工维护检查清单；不自动校验）
 * displayNameZh / bookIds / identityType 等可由系统推导或另存
 */
export const SECONDARY_PROFILE_MIN_FIELDS = Object.freeze([
  "displayNameZh",
  "englishName",
  "sourceBookId",
  "identityNoteZh",
  "needsHeroImage",
]);

let _rulesCache = null;
function getSpecialRules() {
  if (!_rulesCache) _rulesCache = listSpecialIdentityRules();
  return _rulesCache;
}

/**
 * 高风险易混：编辑时优先核对是否需拆档或共用主图。
 * displayNameZh 为经文里常见称呼；notes 为操作提示，非教义断言。
 */
export const BIBLE_HIGH_RISK_CONFUSABLE_NAMES = Object.freeze([
  Object.freeze({
    displayNameZh: "法老",
    group: "title",
    notes: "按叙事时期拆档；约瑟时代与出埃及为不同君主。",
  }),
  Object.freeze({
    displayNameZh: "亚比米勒",
    group: "name_collision",
    notes: "族长叙事中的非利士王号 vs 士师记基甸之子。",
  }),
  Object.freeze({
    displayNameZh: "希律",
    group: "dynasty",
    notes: "大帝 / 安提帕 / 亚基帕等须分档；福音与使徒行传按章节规则映射。",
  }),
  Object.freeze({
    displayNameZh: "凯撒",
    group: "title",
    notes: "奥古斯都与使徒时代「该撒」语境可能不同，勿混脸。",
  }),
  Object.freeze({
    displayNameZh: "亚哈随鲁",
    group: "identity",
    notes: "以斯帖记王名，学术上常与波斯王挂钩；勿与别卷同名混用。",
  }),
  Object.freeze({
    displayNameZh: "便哈达",
    group: "name_collision",
    notes: "亚兰诸王常见名，多指不同人；按书卷叙事分档。",
  }),
  Object.freeze({
    displayNameZh: "约兰",
    group: "name_collision",
    notes: "犹大 / 以色列同名王；须用书卷 + 国别区分。",
  }),
  Object.freeze({
    displayNameZh: "约阿施",
    group: "name_collision",
    notes: "犹大与以色列各有一位；勿共用档案。",
  }),
]);

export function suggestInternalKey(profileKey, englishName) {
  const en = s(englishName);
  if (en) {
    const key = en
      .toLowerCase()
      .replace(/[`'’]/g, "")
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "");
    if (key) return key.slice(0, 64);
  }
  const pk = s(profileKey);
  if (/^[a-z][a-z0-9_]*$/i.test(pk)) return pk.toLowerCase().slice(0, 64);
  return "";
}

export function deriveCharacterTier(row) {
  const role = s(row?.characterRoleZh);
  if (role === "主人物") return CHARACTER_TIER.CORE;
  return CHARACTER_TIER.SECONDARY;
}

export function deriveIdentityType(profileKey, row) {
  const pk = s(profileKey);
  const rules = getSpecialRules();
  const hit = rules.find((r) => s(r.profileKey) === pk);
  if (hit) {
    const sameTitle = rules.filter((r) => s(r.displayNameZh) === s(hit.displayNameZh));
    if (sameTitle.length > 1) return IDENTITY_TYPE.TITLE_SPLIT;
  }
  const bids = Array.isArray(row?.bookIds)
    ? [...new Set(row.bookIds.map((x) => s(x).toUpperCase()).filter(Boolean))]
    : [];
  if (bids.length > 1) return IDENTITY_TYPE.SHARED;
  return IDENTITY_TYPE.SINGLE_BOOK;
}

export function deriveProfileCompletionStatus(row) {
  const en = s(row?.englishName);
  const copy =
    s(row?.scripturePersonalityZh) &&
    s(row?.appearanceEn);
  const img = s(row?.heroImageUrl || row?.imageUrl);
  const sheet = s(row?.comparisonSheetUrl);
  if (!en) return PROFILE_COMPLETION_STATUS.UNPROFILED;
  if (!copy) return PROFILE_COMPLETION_STATUS.PROFILED;
  if (!img) return PROFILE_COMPLETION_STATUS.HAS_COPY;
  if (!sheet) return PROFILE_COMPLETION_STATUS.HAS_HERO;
  return PROFILE_COMPLETION_STATUS.COMPLETE;
}

export function deriveNeedsHeroImage(row) {
  return deriveCharacterTier(row) === CHARACTER_TIER.CORE;
}

/**
 * 合并进档案行的默认/推导字段（仅填补缺项或刷新可推导状态）
 */
export function buildRegistryDefaultPatch(profileKey, row) {
  const patch = {};
  const r = row && typeof row === "object" ? row : {};
  if (!s(r.internalKey)) {
    const ik = suggestInternalKey(profileKey, r.englishName);
    if (ik) patch.internalKey = ik;
  }
  if (!s(r.characterTier)) {
    patch.characterTier = deriveCharacterTier(r);
  }
  if (!s(r.identityType)) {
    patch.identityType = deriveIdentityType(profileKey, r);
  }
  if (r.needsHeroImage === undefined || r.needsHeroImage === null) {
    patch.needsHeroImage = deriveNeedsHeroImage(r);
  }
  const nextStatus = deriveProfileCompletionStatus(r);
  if (s(r.profileCompletionStatus) !== nextStatus) {
    patch.profileCompletionStatus = nextStatus;
  }
  return patch;
}
