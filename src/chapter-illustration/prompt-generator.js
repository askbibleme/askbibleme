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
not crowded,
no frame, no border, no circular vignette, no medallion composition, no poster panel, no sticker-like cutout,
no picture-inside-a-frame look, no art-print-with-mat, no faux wood frame, gallery mount, or inner card inset around the scene,
one continuous painted artwork on the picture plane — not a composited photo pasted inside a decorative window or frame device,
no inner rectangle that reads as a matted print, card inset, or picture hung on a wall within the image,
outer bounds: prefer soft atmospheric fade of paint into transparency; if cropping is tight, avoid slicing through primary faces, feet, or seated supports with a straight hard line
`
  .trim()
  .replace(/\s+/g, " ");

function compositionForMode(mode, fallbackComposition, transparent) {
  const m = String(mode || "").trim().toLowerCase();
  const custom = String(fallbackComposition || "").trim();
  if (custom) return custom;
  const tp = transparent !== false;
  if (m === "banner" || m === "wide") {
    const edge = tp
      ? "include modest environment (ground, air, distance) as one painting; let distant areas soften and fade into transparency at the canvas bounds — avoid a hard straight cut along the bottom that severs feet, bench, or ground"
      : "edges should feel intentionally composed (natural closure) rather than harsh crop cutting through main figures";
    return tp
      ? `wide horizontal layout, one cohesive painted scene (single canvas), balanced left-right narrative placement, breathing space, not crowded; ${edge}`
      : `wide horizontal layout, one cohesive painted scene, balanced left-right narrative placement, breathing space, not crowded; ${edge}`;
  }
  return tp
    ? "single clear narrative moment, centered and balanced layout, readable full-body figures when character-focused; environment may continue as soft paint that fades to transparency at the edges — not a harsh panel chop"
    : "single clear narrative moment, centered and balanced layout, readable full-body figures when character-focused";
}

/**
 * Stage 5 — full engraving prompt from concrete scene description.
 */
export function generateIllustrationPrompt(config) {
  const sceneDescription = String(config?.sceneDescription || "").trim();
  const transparent = config?.transparentBackground !== false;
  const composition = compositionForMode(
    config?.compositionMode,
    config?.composition,
    config?.transparentBackground !== false
  );
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
    ? "PNG, transparent background (alpha channel): figures plus coherent environment paint together; outer zones fade to full alpha so the reader's page shows through — not empty flat white behind cut-out people"
    : "PNG, opaque plain light beige or light gray background";

  const styleBackgroundLine = transparent
    ? "visual form: ONE unified narrative painting — modest biblical-era setting (ground, distance, sky haze) when the story needs it; NOT a photograph inside a frame, NOT a bordered inset; NOT sticker cut-outs on blank white"
    : "background rule: clean, plain, light background (beige or light gray), avoid clutter unless explicitly required by scene";

  const styleBackgroundLine2 = transparent
    ? "edge transparency: let environment and atmosphere soften and dissolve toward the canvas edges into full alpha (painterly falloff) — seamless blend with chapter paper; forbid only a flat fake cream rectangle or studio sheet filling the frame behind figures"
    : "";

  const lines = [
    "[SCENE]",
    sceneDescription || "single biblical narrative moment, calm and restrained",
    "",
    "[STYLE]",
    STYLE_LOCK_GLOBAL + ",",
    styleBackgroundLine + ",",
    ...(styleBackgroundLine2 ? [styleBackgroundLine2 + ","] : []),
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
          "TRANSPARENT (merged into CONSTRAINTS): narrative paint (people + coherent environment) uses opacity; true alpha 0 only where nothing is painted — chapter paper shows through. Soft environmental fade into transparency at the outer bounds is REQUIRED for natural page blend",
          "include setting: ground plane, rocks, distant land or soft sky tone as the scene needs — avoid floating figures on empty white void; environment should read as one oil-style painting, not separate cut-out layers",
          "edge treatment: atmospheric / painterly fade to full transparency at top and sides; at bottom extend ground or shadow gently and let it dissolve into alpha — do NOT use a hard horizontal slice through feet, bench, or torsos; do NOT leave disconnected random texture scraps",
          "distant figures: if a person appears at the edge, show enough body mass or merge into haze — avoid a head-only floater with hard cut",
          "forbidden: faux cream paper or flat white filling the whole canvas behind subjects; decorative picture-frame shapes, mat lines, inner card insets, sticker ovals, drop-shadow plates",
          "allowed: soft edge vignette into transparency (environment dissolving to alpha) — this is NOT the same as a fake circular moon-disc backdrop behind one figure",
        ]
      : []),
    "",
    "STYLE PRESET TAG: " + stylePreset,
    "OUTPUT: " + outputBlock,
    ...charBlock,
  ];

  return lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}
