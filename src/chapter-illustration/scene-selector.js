/**
 * Stage 3 — pick exactly one scene; supports variant index for regeneration.
 */

export function selectBestSceneForChapter(analysisResult, options = {}) {
  const { candidateScenes = [], chapterType, difficultyLevel } = analysisResult;
  const variant = Math.max(0, Number(options.alternateIndex || 0) || 0);
  const list = candidateScenes.length
    ? candidateScenes
    : [{ id: "fallback", scene: "generic_open_land", priority: 1, reason: "fallback" }];

  const idx = variant % list.length;
  const picked = list[idx];

  let confidence = 0.72;
  let warning = null;

  if (chapterType === "genealogy") {
    confidence = 0.38;
    warning =
      "This chapter has weak narrative structure; a simple fallback scene was used.";
  } else if (chapterType === "law" || chapterType === "discourse") {
    confidence = 0.55;
    warning =
      "This chapter is mostly teaching or law; the scene is a representative still moment.";
  } else if (difficultyLevel === "high") {
    confidence = 0.45;
    warning = "Low-confidence scene selection; you may edit the description before generating.";
  } else if (picked.priority < 50) {
    confidence = 0.58;
    warning = null;
  }

  const anchorObject = mapSceneToAnchor(picked.scene);
  const { characters, location, action } = inferRoles(
    picked.scene,
    analysisResult
  );

  return {
    selectedScene: picked.scene,
    reason: picked.reason,
    characters,
    location,
    action,
    anchorObject,
    candidateId: picked.id,
    /** 与请求 alternateIndex 对应的候选下标 */
    variantIndex: variant,
    candidatePickIndex: idx,
    confidence,
    warning,
  };
}

function mapSceneToAnchor(scene) {
  const m = {
    well_discovery: "stone well",
    altar_stillness: "stone altar",
    covenant_open_sky: "open sky horizon",
    creation_light_waters: "waters and light",
    dream_ladder: "stone ground and sky",
    bush_encounter: "desert bush",
    sea_path: "sea shore",
    generic_open_land: "open ground",
    law_camp: "encampment",
    poetry_landscape: "hills and sky",
    genealogy_fallback: "tent and scroll",
  };
  if (typeof scene === "string" && scene.startsWith("segment_")) return "story moment";
  return m[scene] || "landscape anchor";
}

function inferRoles(scene, _analysis) {
  if (scene === "well_discovery") {
    return {
      characters: "one or two men and servants",
      location: "open country beside a well",
      action: "standing and looking toward uncovered water",
    };
  }
  if (scene === "creation_light_waters") {
    return {
      characters: "no human figures",
      location: "waters and horizon",
      action: "still moment as light divides the deep",
    };
  }
  if (scene === "bush_encounter") {
    return {
      characters: "one robed man",
      location: "rocky desert ground",
      action: "kneeling or standing before a low bush",
    };
  }
  if (scene === "altar_stillness") {
    return {
      characters: "one or two figures",
      location: "open high place",
      action: "still posture near a rough stone altar",
    };
  }
  if (scene === "covenant_open_sky") {
    return {
      characters: "one or two small figures",
      location: "open plain beneath wide sky",
      action: "standing still beneath open heavens",
    };
  }
  if (scene === "dream_ladder") {
    return {
      characters: "one sleeping figure",
      location: "stony ground under night sky",
      action: "lying while a stairway meets the sky",
    };
  }
  if (scene === "sea_path") {
    return {
      characters: "a line of people in the distance",
      location: "shore and divided waters",
      action: "still crossing moment frozen mid-step",
    };
  }
  return {
    characters: "one or two robed figures",
    location: "open biblical hills",
    action: "standing still in quiet conversation or prayer",
  };
}
