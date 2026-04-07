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
  /** 目标：古典版画中「可放大细看」的写实精细线刻，非朦胧示意 */
  renderingGoal: "fine_realistic_engraving",
  exclusions: [
    "modern_objects",
    "typography",
    "crowd_scene",
    "comic_style",
    "painterly_style",
    "cinematic_lighting",
    "blur",
    "soft_focus",
    "low_detail",
  ],
};
