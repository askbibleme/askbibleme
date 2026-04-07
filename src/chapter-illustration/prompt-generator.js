import { STYLE_PRESET_ENGRAVING } from "./style-preset.js";

/**
 * Stage 5 — full engraving prompt from concrete scene description.
 */
export function generateIllustrationPrompt(config) {
  const sceneDescription = String(config?.sceneDescription || "").trim();
  const transparent = config?.transparentBackground !== false;
  const composition =
    String(config?.composition || "").trim() || "single focal point";
  const stylePreset = config?.stylePreset || STYLE_PRESET_ENGRAVING.id;

  const visual =
    sceneDescription +
    ", expanded into one frozen tableau with clear sky, ground plane, and readable figures";

  const outputBlock = transparent
    ? "isolated illustration,\ntransparent background,\nPNG,\nalpha channel"
    : "isolated illustration,\nopaque background,\nPNG";

  const lines = [
    "a biblical scene in open land,",
    visual + ",",
    "the moment is still and calm,",
    "",
    composition + ",",
    "single focal point,",
    "balanced composition,",
    "clear foreground, midground, background,",
    "",
    "STYLE (" + stylePreset + "):",
    "antique biblical copperplate engraving style,",
    "hand-engraved linework, precise controlled strokes,",
    "black ink only,",
    "monochrome line art,",
    "fine cross-hatching,",
    "subtle stippling allowed,",
    "lines only, no fill,",
    "no gradients,",
    "no painterly effects,",
    "no sketch looseness,",
    "no modern illustration style,",
    "",
    "NEGATIVE:",
    "no text, no letters, no symbols,",
    "no modern objects,",
    "no modern clothing,",
    "no crowd,",
    "no multiple scenes,",
    "no exaggerated motion,",
    "no comic style,",
    "no cinematic effects,",
    "",
    "OUTPUT:",
    outputBlock + ",",
    "no paper texture inside the artwork,",
  ];

  return lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}
