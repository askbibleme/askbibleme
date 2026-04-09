export { STYLE_PRESET_ENGRAVING } from "./style-preset.js";
export {
  defaultChapterIllustrationState,
  mergeChapterIllustrationState,
  stateStorageKey,
} from "./illustration-state.js";
export {
  buildChapterPayloadFromPublished,
  themeToFlatString,
  sanitizeChapterKeyPeopleArray,
  mergeKeyPeopleListsMany,
} from "./chapter-payload.js";
export { analyzeChapterForIllustration } from "./chapter-analysis.js";
export { selectBestSceneForChapter } from "./scene-selector.js";
export { generateSceneDescription } from "./scene-description.js";
export { generateSceneDescriptionZh } from "./scene-description-zh.js";
export { generateIllustrationPrompt } from "./prompt-generator.js";
export {
  buildCharacterLockLines,
  buildCharacterLockLinesForRefSelections,
  appearanceEnForSlot,
  periodLabelZhForSlot,
  DEFAULT_ENGLISH_NAME_BY_ZH,
  sanitizeCharacterFigurePortraitSlotByZh,
  resolveChapterRosterPortrait,
} from "./character-appearance.js";

import { buildChapterPayloadFromPublished } from "./chapter-payload.js";
import { analyzeChapterForIllustration } from "./chapter-analysis.js";
import { selectBestSceneForChapter } from "./scene-selector.js";
import { generateSceneDescription } from "./scene-description.js";
import { generateSceneDescriptionZh } from "./scene-description-zh.js";
import { generateIllustrationPrompt } from "./prompt-generator.js";
import { defaultChapterIllustrationState, mergeChapterIllustrationState } from "./illustration-state.js";
import { themeToFlatString } from "./chapter-payload.js";
import { buildCharacterLockLines } from "./character-appearance.js";

/**
 * Run stages 2–4: analysis → selection → one English scene sentence.
 */
export function runScenePipelineFromPublishedData(publishedJson, meta, options = {}) {
  const payload = buildChapterPayloadFromPublished(publishedJson, meta, {
    globalKeyPeople: options.globalKeyPeople,
  });
  const analysis = analyzeChapterForIllustration(payload);
  const alternateIndex = Math.max(0, Number(options.alternateIndex || 0) || 0);
  const profilesRoot = options.profilesRoot || null;
  const selection = selectBestSceneForChapter(analysis, {
    alternateIndex,
    chapterPayload: payload,
    profilesRoot,
  });
  const sceneDescription = generateSceneDescription(selection, payload, {
    profilesRoot,
  });
  const sceneDescriptionZh = generateSceneDescriptionZh(selection, payload, {
    profilesRoot,
  });

  return {
    payload,
    analysis,
    selection,
    sceneDescription,
    sceneDescriptionZh,
    confidence: selection.confidence,
    warning: selection.warning,
    warningZh: selection.warningZh,
    chapterTypeZh: analysis.chapterTypeZh,
  };
}

export function runPromptFromSceneDescription(sceneDescription, renderOpts = {}) {
  const lines = Array.isArray(renderOpts.characterAppearanceLines)
    ? renderOpts.characterAppearanceLines
    : [];
  return generateIllustrationPrompt({
    sceneDescription,
    stylePreset: renderOpts.stylePreset,
    transparentBackground: renderOpts.transparentBackground === true,
    composition: renderOpts.composition,
    characterAppearanceLines: lines,
  });
}

export function buildIllustrationSpecFromPipeline(body, sceneDescription) {
  const theme = themeToFlatString(body?.theme);
  return {
    book: String(body?.book || "").trim(),
    chapter: String(body?.chapter ?? ""),
    theme,
    scene: String(sceneDescription || "").trim(),
    composition: String(body?.composition || "").trim() || "single focal point",
    mood: String(body?.mood || "").trim() || "calm, spacious, peaceful",
    style: "classical biblical candlelit oil painting",
    stylePreset: String(body?.stylePreset || "biblical_candlelit_oil_painting"),
    transparent: body?.transparentBackground === true,
    overlayOpacity:
      body?.overlayOpacity != null ? Number(body.overlayOpacity) : 100,
  };
}

/** Regenerate scene: bump alternate index. */
export function regenerateScene(currentState, publishedJson, options = {}) {
  const nextVariant = (Number(currentState.sceneVariant) || 0) + 1;
  const meta = {
    versionId: currentState.versionId,
    lang: currentState.lang,
    bookId: currentState.bookId,
    chapter: currentState.chapterNumber,
  };
  const profilesRoot = options.profilesRoot || null;
  const run = runScenePipelineFromPublishedData(publishedJson, meta, {
    alternateIndex: nextVariant,
    profilesRoot,
    globalKeyPeople: options.globalKeyPeople,
  });
  const characterAppearanceLines = buildCharacterLockLines(
    run.payload.keyPeople,
    profilesRoot,
    6
  );
  return mergeChapterIllustrationState(currentState, {
    sceneVariant: nextVariant,
    sceneDescription: run.sceneDescription,
    sceneDescriptionZh: run.sceneDescriptionZh,
    chapterType: run.analysis.chapterType,
    analysis: run.analysis,
    selection: run.selection,
    confidence: run.confidence,
    warning: run.warning,
    warningZh: run.warningZh,
    characterAppearanceLines,
    prompt: "",
    imageUrl: "",
    localPath: "",
  });
}

export function regeneratePrompt(currentState) {
  if (!String(currentState.sceneDescription || "").trim()) {
    return { error: "No scene to build from. Generate a scene first." };
  }
  const prompt = runPromptFromSceneDescription(currentState.sceneDescription, {
    transparentBackground: currentState.transparentBackground,
    composition: "single focal point",
    stylePreset: currentState.stylePreset,
    characterAppearanceLines: currentState.characterAppearanceLines,
  });
  return mergeChapterIllustrationState(currentState, { prompt });
}

export function stateFromPipelineRun(
  body,
  publishedJson,
  run,
  extra = {},
  profilesRoot = null
) {
  const payload = run.payload;
  const characterAppearanceLines = buildCharacterLockLines(
    payload.keyPeople,
    profilesRoot,
    6
  );
  return defaultChapterIllustrationState({
    bookName: payload.bookName,
    bookId: payload.bookId,
    chapterNumber: payload.chapterNumber,
    versionId: payload.versionId,
    lang: payload.lang,
    theme: payload.themeFlat,
    summary: payload.summary,
    chapterType: run.analysis.chapterType,
    sceneDescription: run.sceneDescription,
    sceneDescriptionZh: run.sceneDescriptionZh,
    confidence: run.confidence,
    warning: run.warning,
    warningZh: run.warningZh,
    transparentBackground: body?.transparentBackground === true,
    overlayOpacity: Number(body?.overlayOpacity) || 85,
    stylePreset: String(body?.stylePreset || "biblical_candlelit_oil_painting"),
    analysis: run.analysis,
    selection: run.selection,
    sceneVariant: Number(extra.sceneVariant || 0) || 0,
    characterAppearanceLines,
    ...extra,
  });
}
