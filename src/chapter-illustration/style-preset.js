/**
 * Global engraving style preset — all chapters share this base unless overridden later.
 */
export const STYLE_PRESET_ENGRAVING = {
  id: "biblical_copperplate_engraving",
  name: "biblical_copperplate_engraving",
  lineQuality: "precise_controlled",
  shading: "cross_hatching",
  colorMode: "black_ink_monochrome",
  fillMode: "no_fill",
  textureMode: "transparent_background_clean",
  era: "ancient_biblical",
  exclusions: [
    "modern_objects",
    "typography",
    "crowd_scene",
    "comic_style",
    "painterly_style",
    "cinematic_lighting",
  ],
};
