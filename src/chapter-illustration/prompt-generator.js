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
  const characterLines = Array.isArray(config?.characterAppearanceLines)
    ? config.characterAppearanceLines.map((x) => String(x || "").trim()).filter(Boolean)
    : [];

  const visual =
    sceneDescription +
    ", one frozen story moment with readable faces, hands, and props as named, " +
    "figures and objects drawn with convincing real-world proportion and weight";

  const outputBlock = transparent
    ? "isolated illustration,\ntransparent background,\nPNG,\nalpha channel"
    : "isolated illustration,\nopaque background,\nPNG";

  const charBlock =
    characterLines.length > 0
      ? [
          "",
          "CHARACTER CONSISTENCY (same look in every chapter for this project):",
          ...characterLines.map((line) => "- " + line),
          "",
          "Render each named person exactly matching the descriptions above.",
          "Ancient Near Eastern biblical-era garments only (wool/linen tunics, mantles, cloaks, sashes, veils as fitting) — never medieval European, Renaissance, or modern dress. When multiple named people appear, differentiate drapery, layering, and garment silhouette between them so costumes are not copy-pasted clones; period-plausible variety, not identical default robes on every figure.",
        ]
      : [];

  const lines = [
    "a biblical scene in open land,",
    visual + ",",
    "literal narrative illustration of this exact beat, not an abstract allegory,",
    "the moment is still and calm,",
    "",
    composition + ",",
    "single focal point,",
    "balanced composition,",
    "clear foreground, midground, background,",
    "",
    "STYLE (" + stylePreset + "):",
    "museum-quality antique biblical copperplate engraving,",
    "European master-printmaker fineness in the lineage of Dürer and Doré,",
    "hand-engraved linework: razor-sharp edges, crisp silhouettes, high micro-contrast,",
    "dense fine parallel hatching and cross-hatching to model form, never mushy or smeared,",
    "every fold of fabric, facial feature, hand, weapon, and ground texture resolved with tight deliberate strokes,",
    "black ink only, monochrome line art,",
    "subtle stippling allowed for tone,",
    "lines only, no flat airbrush fill,",
    "no soft gradients, no wash,",
    "no painterly blur, no impressionist looseness,",
    "no sketchy scribble or simplified blob shapes,",
    "no modern flat vector or digital softness,",
    "photographically sharp at print scale: if enlarged, detail remains legible,",
    "",
    "NEGATIVE:",
    "no text, no letters, no symbols,",
    "no modern objects,",
    "no modern clothing,",
    "no anonymous silhouettes when the scene names specific people,",
    "no multiple unrelated scenes in one frame,",
    "no blur, no soft focus, no dreamy haze, no foggy atmosphere,",
    "no low resolution, no pixelation, no muddy shading,",
    "no motion blur,",
    "no comic style,",
    "no cinematic lens effects, depth-of-field blur, or bokeh,",
    "",
    "OUTPUT:",
    outputBlock + ",",
    "no paper texture inside the artwork,",
    ...charBlock,
  ];

  return lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}
