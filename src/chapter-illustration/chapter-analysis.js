/**
 * Stage 2 — infer chapter shape and candidate visual hooks (heuristic, no LLM).
 */

function inferChapterType(themeFlat, summary, storyUnits) {
  const t = `${themeFlat}\n${summary}\n${storyUnits.map((u) => u.title).join("\n")}`;

  if (/家谱|生养|后裔|活到.*岁|年纪|代下|历代志上.*家谱/.test(t))
    return "genealogy";
  if (/律例|典章|条例|不可|应当|祭司献|洁净条例/.test(t)) return "law";
  if (/耶和华如此说|万军之耶和华|预言|先见/.test(t)) return "prophecy";
  if (/七印|七号|兽|大龙|巴比伦大城/.test(t)) return "apocalyptic";
  if (/诗(\s|$)|诗歌|上行之诗/.test(t)) return "poetry";
  if (/讲论|教训|说道|辩论|演讲/.test(t) && storyUnits.length < 2)
    return "discourse";
  if (/比喻|天国好比|又说/.test(t)) return "mixed";
  return "narrative";
}

function extractVisualAnchors(text) {
  const anchors = [];
  const keys = [
    ["井", "well"],
    ["坛", "altar"],
    ["会幕", "tent"],
    ["殿", "temple"],
    ["海", "sea"],
    ["河", "river"],
    ["山", "mountain"],
    ["旷野", "wilderness"],
    ["光", "light"],
    ["彩虹", "rainbow"],
    ["梦", "dream"],
    ["梯", "ladder"],
    ["羊", "flock"],
    ["约柜", "ark"],
    ["船", "ship"],
  ];
  for (const [zh, en] of keys) {
    if (text.includes(zh) && !anchors.includes(en)) anchors.push(en);
  }
  return anchors.slice(0, 8);
}

function buildCandidateScenes(payload, chapterType) {
  const { themeFlat, summary, storyUnits, keyPeople, keyLocations } = payload;
  const candidates = [];
  const text = `${themeFlat} ${summary}`;

  const push = (scene, priority, reason) => {
    candidates.push({
      id: `c_${candidates.length}`,
      scene,
      priority,
      reason,
      source: "heuristic",
    });
  };

  if (/井/.test(text) || /well/i.test(text)) {
    push(
      "well_discovery",
      95,
      "discovery moment at a well"
    );
  }
  if (/祭|坛/.test(text)) {
    push("altar_stillness", 88, "altar or offering anchor");
  }
  if (/彩虹|立约|约/.test(text)) {
    push("covenant_open_sky", 85, "covenant or sign in open air");
  }
  if (/创造|光|空虚混沌|深渊|起初/.test(text)) {
    push("creation_light_waters", 90, "creation — light over deep");
  }
  if (/梦|梯|伯特利/.test(text)) {
    push("dream_ladder", 82, "dream or visionary stairway");
  }
  if (/摩西|荆棘|火焰/.test(text)) {
    push("bush_encounter", 92, "call or encounter at bush");
  }
  if (/过海|红海|分开/.test(text)) {
    push("sea_path", 80, "sea or crossing moment");
  }

  for (let i = 0; i < storyUnits.length && i < 4; i += 1) {
    const title = storyUnits[i].title;
    if (!title) continue;
    push(
      `segment_${i}`,
      70 - i * 5,
      `story unit: ${title.slice(0, 40)}`
    );
  }

  if (chapterType === "genealogy") {
    push("genealogy_fallback", 25, "weak narrative — camp lineage tableau");
  }
  if (chapterType === "law") {
    push("law_camp", 40, "representative camp or priestly stillness");
  }
  if (chapterType === "poetry") {
    push("poetry_landscape", 55, "single concrete image from poetic text");
  }

  push("generic_open_land", 30, "fallback — figure in open biblical land");

  candidates.sort((a, b) => b.priority - a.priority);
  return candidates;
}

function scoreDifficulty(chapterType, candidateCount) {
  if (chapterType === "genealogy") return "high";
  if (chapterType === "law" || chapterType === "discourse") return "medium";
  if (candidateCount < 2) return "medium";
  return "low";
}

const CHAPTER_TYPE_LABEL_ZH = {
  genealogy: "家谱",
  law: "律例",
  prophecy: "预言",
  apocalyptic: "启示异象",
  poetry: "诗歌",
  discourse: "讲论",
  mixed: "比喻与叙述",
  narrative: "叙事",
};

export function analyzeChapterForIllustration(chapterPayload) {
  const { themeFlat, summary, storyUnits } = chapterPayload;
  const chapterType = inferChapterType(
    themeFlat,
    summary,
    storyUnits
  );
  const chapterTypeZh =
    CHAPTER_TYPE_LABEL_ZH[chapterType] || chapterType || "未知";
  const combined = [
    themeFlat,
    summary,
    ...storyUnits.map((u) => u.title),
  ].join(" ");
  const visualAnchors = extractVisualAnchors(combined);
  const candidateScenes = buildCandidateScenes(chapterPayload, chapterType);
  const difficultyLevel = scoreDifficulty(
    chapterType,
    candidateScenes.length
  );

  return {
    chapterType,
    chapterTypeZh,
    storyUnits,
    candidateScenes,
    visualAnchors,
    difficultyLevel,
  };
}
