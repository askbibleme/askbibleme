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
    prompt: "",
    imageUrl: "",
    localPath: "",
    confidence: 0,
    warning: null,
    warningZh: null,
    transparentBackground: true,
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
