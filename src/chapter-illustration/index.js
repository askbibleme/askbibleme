export { STYLE_PRESET_ENGRAVING } from "./style-preset.js";
export {
  defaultChapterIllustrationState,
  mergeChapterIllustrationState,
  stateStorageKey,
} from "./illustration-state.js";
export {
  buildChapterPayloadFromPublished,
  themeToFlatString,
} from "./chapter-payload.js";
export { analyzeChapterForIllustration } from "./chapter-analysis.js";
export { selectBestSceneForChapter } from "./scene-selector.js";
export { generateSceneDescription } from "./scene-description.js";
export { generateIllustrationPrompt } from "./prompt-generator.js";

import { buildChapterPayloadFromPublished } from "./chapter-payload.js";
import { analyzeChapterForIllustration } from "./chapter-analysis.js";
import { selectBestSceneForChapter } from "./scene-selector.js";
import { generateSceneDescription } from "./scene-description.js";
import { generateIllustrationPrompt } from "./prompt-generator.js";
import { defaultChapterIllustrationState, mergeChapterIllustrationState } from "./illustration-state.js";
import { themeToFlatString } from "./chapter-payload.js";

/**
 * Run stages 2–4: analysis → selection → one English scene sentence.
 */
export function runScenePipelineFromPublishedData(publishedJson, meta, options = {}) {
  const payload = buildChapterPayloadFromPublished(publishedJson, meta);
  const analysis = analyzeChapterForIllustration(payload);
  const alternateIndex = Math.max(0, Number(options.alternateIndex || 0) || 0);
  const selection = selectBestSceneForChapter(analysis, { alternateIndex });
  const sceneDescription = generateSceneDescription(selection, payload);

  return {
    payload,
    analysis,
    selection,
    sceneDescription,
    confidence: selection.confidence,
    warning: selection.warning,
  };
}

export function runPromptFromSceneDescription(sceneDescription, renderOpts = {}) {
  return generateIllustrationPrompt({
    sceneDescription,
    stylePreset: renderOpts.stylePreset,
    transparentBackground: renderOpts.transparentBackground !== false,
    composition: renderOpts.composition,
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
    style: "antique engraving",
    stylePreset: String(body?.stylePreset || "biblical_copperplate_engraving"),
    transparent: body?.transparentBackground !== false,
    overlayOpacity:
      body?.overlayOpacity != null ? Number(body.overlayOpacity) : 100,
  };
}

/** Regenerate scene: bump alternate index. */
export function regenerateScene(currentState, publishedJson) {
  const nextVariant = (Number(currentState.sceneVariant) || 0) + 1;
  const meta = {
    versionId: currentState.versionId,
    lang: currentState.lang,
    bookId: currentState.bookId,
    chapter: currentState.chapterNumber,
  };
  const run = runScenePipelineFromPublishedData(publishedJson, meta, {
    alternateIndex: nextVariant,
  });
  return mergeChapterIllustrationState(currentState, {
    sceneVariant: nextVariant,
    sceneDescription: run.sceneDescription,
    chapterType: run.analysis.chapterType,
    analysis: run.analysis,
    selection: run.selection,
    confidence: run.confidence,
    warning: run.warning,
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
  });
  return mergeChapterIllustrationState(currentState, { prompt });
}

export function stateFromPipelineRun(body, publishedJson, run, extra = {}) {
  const payload = run.payload;
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
    confidence: run.confidence,
    warning: run.warning,
    transparentBackground: body?.transparentBackground !== false,
    overlayOpacity: Number(body?.overlayOpacity) || 85,
    stylePreset: String(body?.stylePreset || "biblical_copperplate_engraving"),
    analysis: run.analysis,
    selection: run.selection,
    sceneVariant: Number(extra.sceneVariant || 0) || 0,
    ...extra,
  });
}
