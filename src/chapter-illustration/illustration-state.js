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
    prompt: "",
    imageUrl: "",
    localPath: "",
    confidence: 0,
    warning: null,
    transparentBackground: true,
    overlayOpacity: 85,
    stylePreset: STYLE_PRESET_ENGRAVING.id,
    sceneVariant: 0,
    analysis: null,
    selection: null,
    ...overrides,
  };
}

export function stateStorageKey({ versionId, lang, bookId, chapter }) {
  return `${String(versionId || "")}:${String(lang || "")}:${String(bookId || "")}:${String(chapter ?? "")}`;
}

export function mergeChapterIllustrationState(prev, patch) {
  return { ...defaultChapterIllustrationState(), ...prev, ...patch };
}
