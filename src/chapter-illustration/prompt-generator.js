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

/** Shared constraints except outer-edge rule (depends on opaque vs transparent). */
const CONSTRAINTS_GLOBAL_BASE = `
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
no inner rectangle that reads as a matted print, card inset, or picture hung on a wall within the image
`
  .trim()
  .replace(/\s+/g, " ");

function outerBoundsConstraint(transparent) {
  if (transparent) {
    return "outer bounds: slow wide atmospheric fade — opacity falls off gradually through mist, sand, ground haze, or sky wash over a generous outer band; do NOT pinch transparency tight around figure silhouettes; if cropping is tight, avoid slicing through primary faces, feet, or seated supports with a straight hard line";
  }
  return "outer bounds: environment is painted to all edges — sky, ground, distant land, architecture, or soft haze must fill the frame; no empty corners; if cropping is tight, avoid slicing through primary faces, feet, or seated supports with a straight hard line";
}

function compositionForMode(mode, fallbackComposition, transparent) {
  const m = String(mode || "").trim().toLowerCase();
  const custom = String(fallbackComposition || "").trim();
  if (custom) return custom;
  const tp = transparent === true;
  if (m === "banner" || m === "wide") {
    const edge = tp
      ? "include modest environment (ground, air, distance) as one painting; let the SETTING dissolve slowly into transparency at the canvas bounds over a wide outer band — warm beige / sand / parchment haze may carry the fusion; avoid a fast tight edge hugging bodies or a hard straight cut along the bottom that severs feet, bench, or ground"
      : "include a full modest biblical environment (ground, sky, distance, walls or landscape) as one continuous opaque painting edge to edge — warm earth tones, sand, parchment haze, soft blue-gray sky, or dim plaster; avoid empty pure-white void behind figures; corners and margins must read as painted setting, not blank studio";
    return tp
      ? `wide horizontal layout, one cohesive painted scene (single canvas), balanced left-right narrative placement, breathing space, not crowded; ${edge}`
      : `wide horizontal layout, one cohesive painted scene (single canvas), balanced left-right narrative placement, breathing space, not crowded; ${edge}`;
  }
  return tp
    ? "single clear narrative moment, centered and balanced layout, readable full-body figures when character-focused; environment continues as soft paint with SLOW fade to transparency at the edges — wide gentle falloff, not a harsh panel chop at the figures"
    : "single clear narrative moment, centered and balanced layout, readable full-body figures when character-focused; full environmental background continues to the frame — painted atmosphere and setting, not figures on empty white";
}

/**
 * Stage 5 — full engraving prompt from concrete scene description.
 * transparentBackground === true opts into transparent PNG + alpha-edge rules; default product is opaque with setting.
 */
export function generateIllustrationPrompt(config) {
  const sceneDescription = String(config?.sceneDescription || "").trim();
  const transparent = config?.transparentBackground === true;
  const composition = compositionForMode(
    config?.compositionMode,
    config?.composition,
    transparent
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
    ? "PNG, transparent background (alpha channel): figures plus coherent environment paint together; outer zones use a SLOW GRADIENT to full alpha so the reader's page shows through — not empty flat white or a fast mask hugging silhouettes"
    : "PNG, opaque: fully painted scene with coherent environmental background filling the entire canvas (ground, sky, distance, architecture, dust or mist as the narrative needs). NOT a transparency cutout. Avoid a flat empty pure-white (#FFFFFF) or near-white studio void as the dominant backdrop — prefer warm parchment beige, sand, ochre haze, soft gray-blue sky, twilight violet, or dim interior plaster. Pure white or near-white is ONLY allowed as intentional light depiction (sun disk edge, narrow sunbeam, lamp glow, divine radiance, lightning glare) — not as the global empty background wash";

  const styleBackgroundLine = transparent
    ? "visual form: ONE unified narrative painting — modest biblical-era setting (ground, distance, sky haze) when the story needs it; NOT a photograph inside a frame, NOT a bordered inset; NOT sticker cut-outs on blank white — allow soft warm beige / sand / parchment atmospheric wash in mid-to-outer zones for environmental fusion before fade to alpha"
    : "visual form: ONE unified narrative painting with a complete environmental background (indoor or outdoor) — modest biblical-era setting; NOT a photograph inside a frame, NOT a bordered inset; NOT sticker cut-outs on blank white; the whole image plane should read as oil-style scene painting with real space behind and around figures";

  const styleBackgroundLine2 = transparent
    ? "edge transparency: environment and atmosphere must dissolve SLOWLY toward the canvas edges into full alpha (wide painterly falloff, like oil glaze thinning out) — seamless blend with chapter paper; forbid a uniform flat cream or white studio card filling the entire frame edge-to-edge behind subjects (that reads as a sticker plate); graduated beige/sand haze in outer bands is encouraged for fusion"
    : "background color rule: forbid dominant empty pure-white or blank paper-white void; keep tonal warmth or atmospheric color in walls, sky, sand, shadow, and haze. Bright or white patches are acceptable only where they read clearly as light sources or lit mist, not as unused canvas";

  const lines = [
    "[SCENE]",
    sceneDescription || "single biblical narrative moment, calm and restrained",
    "",
    "[STYLE]",
    STYLE_LOCK_GLOBAL + ",",
    styleBackgroundLine + ",",
    styleBackgroundLine2 + ",",
    "character rule: modest and simple clothing, natural standing/resting pose, full body when character-focused,",
    "",
    "[COMPOSITION]",
    composition + ",",
    "",
    "[CONSTRAINTS]",
    CONSTRAINTS_GLOBAL_BASE + ",",
    outerBoundsConstraint(transparent) + ",",
    "no text, letters, symbols, watermark or logo",
    ...(transparent
      ? [
          "TRANSPARENT (merged into CONSTRAINTS): narrative paint (people + coherent environment) uses opacity; true alpha 0 only where nothing is painted — chapter paper shows through. A SLOW, WIDE environmental fade into transparency at the outer bounds is REQUIRED (roughly 15–40% of frame depth from each edge may carry gradual falloff — not a thin ring around people)",
          "include setting: ground plane, rocks, distant land, soft sky, or warm sand/beige mist as the scene needs — avoid floating figures on empty pure-white void; environment should read as one oil-style painting, not separate cut-out layers",
          "fusion path (critical): transition to transparency through the ENVIRONMENT (haze, dust, sand, distant ground, sky wash) — do NOT let opacity drop fastest at character contours; figures stay solid where the story needs them; the outer atmosphere carries a soft warm beige / parchment tone that then thins to alpha",
          "edge treatment: gentle long-gradient fade at top, sides, and bottom — extend ground or shadow and let it dissolve slowly into alpha; do NOT use a hard horizontal slice through feet, bench, or torsos; do NOT leave disconnected random texture scraps",
          "distant figures: if a person appears at the edge, show enough body mass or merge into haze — avoid a head-only floater with hard cut",
          "forbidden: edge-to-edge flat white OR flat cream studio sweep with no gradient; decorative picture-frame shapes, mat lines, inner card insets, sticker ovals, drop-shadow plates",
          "allowed: soft wide vignette into transparency (environment dissolving to alpha over a generous band); soft warm beige atmospheric base in outer zones for page fusion — NOT the same as a fake circular moon-disc backdrop plate behind one figure",
        ]
      : [
          "OPAQUE ENVIRONMENT (merged into CONSTRAINTS): every pixel of the image is painted scene — sky, terrain, architecture, shadow, or atmospheric haze; no alpha cutout look, no empty white rectangle behind subjects",
          "include full setting: ground, horizon, ceiling, or room corners as appropriate — figures belong inside a real space, not on a blank field",
          "forbidden: dominant flat pure-white or default digital canvas as the main backdrop; decorative picture-frame shapes, mat lines, inner card insets, sticker ovals, drop-shadow plates",
          "light exception: bright white or near-white MAY appear only where it clearly depicts light itself (sun rim, beam, fire, lamp, glory, lightning) — not as substitute for an undescribed background",
        ]),
    "",
    "STYLE PRESET TAG: " + stylePreset,
    "OUTPUT: " + outputBlock,
    ...charBlock,
  ];

  return lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}
