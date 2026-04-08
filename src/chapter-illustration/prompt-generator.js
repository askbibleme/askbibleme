import { STYLE_PRESET_ENGRAVING } from "./style-preset.js";

const STYLE_LOCK_GLOBAL = `
semi-realistic biblical illustration,
historically grounded,
ancient Near Eastern clothing (tunic, shawl, sandals),
linen and wool fabric texture,
earth-tone color palette (low saturation),
soft natural lighting,
calm and restrained emotional tone,
no dramatic action
`
  .trim()
  .replace(/\s+/g, " ");

const CONSTRAINTS_GLOBAL = `
no modern elements,
no fantasy armor or decorative fantasy styling,
no bright saturated colors,
no cinematic lighting,
no exaggerated expressions or action,
no anime / cartoon / stylized fantasy,
no sci-fi effects,
single clear narrative moment only,
no symbolic abstraction,
not crowded
`
  .trim()
  .replace(/\s+/g, " ");

function compositionForMode(mode, fallbackComposition) {
  const m = String(mode || "").trim().toLowerCase();
  const custom = String(fallbackComposition || "").trim();
  if (custom) return custom;
  if (m === "banner" || m === "wide") {
    return "wide horizontal layout, balanced left-right narrative placement, large breathing space, not crowded";
  }
  return "single clear narrative moment, centered and balanced layout, readable full-body figures when character-focused";
}

/**
 * Stage 5 — full engraving prompt from concrete scene description.
 */
export function generateIllustrationPrompt(config) {
  const sceneDescription = String(config?.sceneDescription || "").trim();
  const transparent = config?.transparentBackground !== false;
  const composition = compositionForMode(config?.compositionMode, config?.composition);
  const stylePreset = config?.stylePreset || STYLE_PRESET_ENGRAVING.id;
  const characterLines = Array.isArray(config?.characterAppearanceLines)
    ? config.characterAppearanceLines.map((x) => String(x || "").trim()).filter(Boolean)
    : [];

  const charBlock =
    characterLines.length > 0
      ? [
          "",
          "CHARACTER CONSISTENCY (same look in every chapter for this project):",
          ...characterLines.map((line) => "- " + line),
          "",
          "Render each named person exactly matching the descriptions above.",
          "Ancient Near Eastern biblical-era garments only (wool/linen tunics, mantles, cloaks, sashes, veils as fitting) — never medieval European, Renaissance, or modern dress. When multiple named people appear, differentiate drapery, layering, and garment silhouette between them so costumes are not copy-pasted clones; period-plausible variety, not identical default robes on every figure.",
          "When multiple standing adults appear, keep natural height variation (adult women typically shorter than adult men in the same scene unless the narrative specifies otherwise); do not equalize everyone to the same silhouette height.",
          "Costume: primeval Adam/Eve-era figures in simple animal-hide dress only; from later narrative layers use era- and office-appropriate garb (priestly, royal, wealthy patriarch, poor or mourning) per the beat — never one generic costume for every named person.",
        ]
      : [];

  const outputBlock = transparent
    ? "PNG, transparent background (alpha channel), all non-subject pixels must be fully transparent (alpha 0), no rectangular backdrop fill"
    : "PNG, opaque plain light beige or light gray background";

  const lines = [
    "[SCENE]",
    sceneDescription || "single biblical narrative moment, calm and restrained",
    "",
    "[STYLE]",
    STYLE_LOCK_GLOBAL + ",",
    "background rule: clean, plain, light background (beige or light gray), avoid clutter unless explicitly required by scene,",
    "character rule: modest and simple clothing, natural standing/resting pose, full body when character-focused,",
    "",
    "[COMPOSITION]",
    composition + ",",
    "",
    "[CONSTRAINTS]",
    CONSTRAINTS_GLOBAL + ",",
    "no text, letters, symbols, watermark or logo",
    ...(transparent
      ? [
          "transparent mode required: isolate subject cleanly, no painted sky/wall/ground panel, no beige paper block, no solid background tint",
        ]
      : []),
    "",
    "STYLE PRESET TAG: " + stylePreset,
    "OUTPUT: " + outputBlock,
    ...charBlock,
  ];

  return lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}
