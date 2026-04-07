/**
 * Stage 3 — pick exactly one scene; supports variant index for regeneration.
 */

import {
  englishNameForPerson,
} from "./character-appearance.js";

const SCENE_KEY_LABEL_ZH = {
  well_discovery: "井边",
  creation_light_waters: "创造·光与水",
  bush_encounter: "荆棘火焰",
  altar_stillness: "祭坛",
  covenant_open_sky: "立约·旷野天幕",
  dream_ladder: "梦中之梯",
  sea_path: "过海",
  poetry_landscape: "诗歌意境",
  law_camp: "营中律例",
  genealogy_fallback: "家谱·帐幕",
  generic_open_land: "旷野路途",
};

export function selectBestSceneForChapter(analysisResult, options = {}) {
  const { candidateScenes = [], chapterType, difficultyLevel } = analysisResult;
  const profilesRoot = options.profilesRoot || null;
  const variant = Math.max(0, Number(options.alternateIndex || 0) || 0);
  const list = candidateScenes.length
    ? candidateScenes
    : [{ id: "fallback", scene: "generic_open_land", priority: 1, reason: "fallback" }];

  const idx = variant % list.length;
  const picked = list[idx];

  let confidence = 0.72;
  let warning = null;
  let warningZh = null;

  if (chapterType === "genealogy") {
    confidence = 0.38;
    warning =
      "This chapter has weak narrative structure; a simple fallback scene was used.";
    warningZh = "本章以家谱为主，叙事性弱，已使用简化的备用画面。";
  } else if (chapterType === "law" || chapterType === "discourse") {
    confidence = 0.55;
    warning =
      "This chapter is mostly teaching or law; the scene is a representative still moment.";
    warningZh = "本章多为教导或律例，画面为象征性的静止瞬间，非具体情节特写。";
  } else if (difficultyLevel === "high") {
    confidence = 0.45;
    warning = "Low-confidence scene selection; you may edit the description before generating.";
    warningZh = "场景匹配置信度偏低，建议在出图前手动修改英文场景描述。";
  } else if (picked.priority < 50) {
    confidence = 0.58;
    warning = null;
    warningZh = null;
  }

  const anchorObject = mapSceneToAnchor(picked.scene);
  const payload = options.chapterPayload || null;
  const { characters, location, action } = inferRoles(
    picked.scene,
    analysisResult,
    payload,
    profilesRoot
  );

  const sceneLabelZh =
    typeof picked.scene === "string" && picked.scene.startsWith("segment_")
      ? "按段落标题择景"
      : SCENE_KEY_LABEL_ZH[picked.scene] || "其他";

  return {
    selectedScene: picked.scene,
    reason: picked.reason,
    sceneLabelZh,
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
    warningZh,
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

function namedFiguresFromPayload(payload, profilesRoot) {
  const people = payload?.keyPeople;
  if (!Array.isArray(people) || people.length === 0) return "";
  const parts = people
    .slice(0, 4)
    .map((z) => englishNameForPerson(String(z).trim(), profilesRoot))
    .filter(Boolean);
  if (parts.length === 0) return "";
  if (parts.length === 1) return parts[0];
  if (parts.length === 2) return `${parts[0]} and ${parts[1]}`;
  return `${parts.slice(0, -1).join(", ")}, and ${parts[parts.length - 1]}`;
}

function inferRoles(scene, _analysis, payload, profilesRoot) {
  const named = namedFiguresFromPayload(payload, profilesRoot);

  if (scene === "well_discovery") {
    return {
      characters: named || "one or two men and servants",
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
      characters: named || "one robed man",
      location: "rocky desert ground",
      action: "kneeling or standing before a low bush",
    };
  }
  if (scene === "altar_stillness") {
    return {
      characters: named || "one or two figures",
      location: "open high place",
      action: "still posture near a rough stone altar",
    };
  }
  if (scene === "covenant_open_sky") {
    return {
      characters: named || "one or two small figures",
      location: "open plain beneath wide sky",
      action: "standing still beneath open heavens",
    };
  }
  if (scene === "dream_ladder") {
    return {
      characters: named || "one sleeping figure",
      location: "stony ground under night sky",
      action: "lying while a stairway meets the sky",
    };
  }
  if (scene === "sea_path") {
    return {
      characters: named || "a line of people in the distance",
      location: "shore and divided waters",
      action: "still crossing moment frozen mid-step",
    };
  }
  return {
    characters: named || "one or two robed figures",
    location: "open biblical hills",
    action: "standing still in quiet conversation or prayer",
  };
}
