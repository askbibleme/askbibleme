/**
 * Master biblical illustration preset based on the approved "Style 1" direction.
 * Keep this unified with chapter illustration prompts and character portrait generation.
 */
export const STYLE_PRESET_OIL_MASTER = {
  id: "biblical_candlelit_oil_painting",
  name: "biblical_candlelit_oil_painting",
  lineQuality: "painterly_soft_edges",
  shading: "dramatic_chiaroscuro",
  colorMode: "warm_earth_tone_low_key",
  fillMode: "aged_oil_painting",
  textureMode: "soft_canvas_grain",
  era: "ancient_biblical",
  renderingGoal: "unified_narrative_character_painting",
  exclusions: [
    "modern_objects",
    "typography",
    "crowd_scene",
    "anime_style",
    "cartoon_style",
    "fantasy_armor",
    "clean_vector_edges",
    "bright_fantasy_colors",
    "glossy_digital_concept_art",
    "sci_fi_fx",
  ],
};

/**
 * Backward-compatible export name used across the existing illustration pipeline.
 */
export const STYLE_PRESET_ENGRAVING = STYLE_PRESET_OIL_MASTER;
