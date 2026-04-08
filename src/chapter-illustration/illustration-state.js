import { STYLE_PRESET_ENGRAVING } from "./style-preset.js";

export function defaultChapterIllustrationState(overrides = {}) {
  return {
    bookName: "",
    chapterNumber: "",
    versionId: "",
    lang: "",
    bookId: "",
    theme: "",
    summary: "",
    chapterType: "narrative",
    sceneDescription: "",
    sceneDescriptionZh: "",
    /** 英文场景（sceneDescription）的中文意译，供编辑对照；与 illustrationBriefZh（创意概括）不同 */
    sceneEnglishZh: "",
    /** 插画管理页：中文关键词（与 sceneDescriptionZh 一并保存） */
    keywordsZh: "",
    /**
     * 插画管理页：人物库参考 [{ zhName, slotIndex }]，slotIndex 0=根时期，1+=periods[i-1]
     */
    characterRefSelections: [],
    prompt: "",
    imageUrl: "",
    localPath: "",
    confidence: 0,
    warning: null,
    warningZh: null,
    transparentBackground: false,
    overlayOpacity: 85,
    stylePreset: STYLE_PRESET_ENGRAVING.id,
    sceneVariant: 0,
    analysis: null,
    selection: null,
    /** 出图 prompt 用，跨章角色外观锁定（与 character_illustration_profiles.json 一致） */
    characterAppearanceLines: [],
    ...overrides,
  };
}

export function stateStorageKey({ versionId, lang, bookId, chapter }) {
  return `${String(versionId || "")}:${String(lang || "")}:${String(bookId || "")}:${String(chapter ?? "")}`;
}

export function mergeChapterIllustrationState(prev, patch) {
  return { ...defaultChapterIllustrationState(), ...prev, ...patch };
}
