/**
 * Stage 4 — concrete English scene text (drawable, naming people & place when known).
 */

import {
  buildFiguresLeadPhrase,
  englishNameForPerson,
  locationsToEnglish,
} from "./character-appearance.js";

const MAX_SCENE_WORDS = 52;
const MIN_SCENE_WORDS = 10;

function countWords(s) {
  return String(s || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

function clampSentence(s) {
  let out = String(s || "").replace(/\s+/g, " ").trim();
  let words = out.split(/\s+/).filter(Boolean);
  if (words.length > MAX_SCENE_WORDS + 2) {
    words = words.slice(0, MAX_SCENE_WORDS);
    out = words.join(" ") + ".";
  }
  if (countWords(out) < MIN_SCENE_WORDS) {
    out =
      out +
      " Every pose and object is specific to this story beat, not a generic biblical vignette.";
    words = out.split(/\s+/).filter(Boolean);
    if (words.length > MAX_SCENE_WORDS) {
      out = words.slice(0, MAX_SCENE_WORDS).join(" ") + ".";
    }
  }
  return out;
}

/** 仅在句首补充地名（人物已由 inferRoles / 模板中的 characters 体现，避免重复） */
function prefixLocationWhenMissing(sentence, chapterPayload) {
  const locs = locationsToEnglish(chapterPayload?.keyLocations || []);
  if (locs.length === 0) return sentence;
  const s = String(sentence || "").trim();
  if (/^at\s+[a-z]/i.test(s)) return s;
  const locPhrase =
    locs.length === 1 ? locs[0] : locs.slice(0, 2).join(" and ");
  return `At ${locPhrase}, ${s}`.replace(/\s+,/g, ",").replace(/,\s*,/g, ",");
}

export function generateSceneDescription(
  selectedSceneData,
  chapterPayload,
  options = {}
) {
  const profilesRoot = options.profilesRoot || null;
  const scene = selectedSceneData.selectedScene;
  const { characters, location, action } = selectedSceneData;
  const { storyUnits, themeFlat } = chapterPayload;

  let sentence = "";

  if (typeof scene === "string" && scene.startsWith("segment_")) {
    const i = parseInt(scene.replace("segment_", ""), 10) || 0;
    const title = storyUnits[i]?.title || themeFlat.slice(0, 80);
    sentence = segmentTitleToEnglish(
      title,
      characters,
      location,
      action,
      chapterPayload,
      profilesRoot
    );
  } else {
    sentence = templateForSceneKey(
      scene,
      characters,
      location,
      action,
      themeFlat
    );
  }

  if (!sentence || countWords(sentence) < 8) {
    const who =
      buildFiguresLeadPhrase(chapterPayload?.keyPeople || [], profilesRoot, 2) ||
      "Two identifiable figures in biblical dress";
    sentence = `${who} stand in ${location}, ${action}, in one frozen story moment with clear props.`;
  }

  sentence = prefixLocationWhenMissing(sentence, chapterPayload);
  return clampSentence(sentence);
}

function templateForSceneKey(scene, characters, location, action, themeFlat) {
  const t = {
    well_discovery: `${characters} beside a stone well in open country while servants kneel at the rim as water shows below, ${location} behind them.`,
    creation_light_waters: `Dark waters lie still beneath a wide sky as a soft radiance divides the deep, no figures, only horizon and calm air above the void.`,
    bush_encounter: `${characters} on rocky ground before a low desert bush as a quiet flame-like brightness rests within the branches without consuming them.`,
    altar_stillness: `${characters} in still posture beside a rough stone altar on open high ground while the camp lies small and far in the haze behind.`,
    covenant_open_sky: `${characters} on a wide plain beneath an open sky, motionless, facing the horizon where light meets earth in a single clear band.`,
    dream_ladder: `${characters} on stony ground under night sky, lying while a narrow stairway rises into soft light above.`,
    sea_path: `${characters} at the shore as divided waters form a still corridor, frozen mid-crossing with raised staffs and visible sand underfoot.`,
    poetry_landscape: `${characters} face quiet hills and an empty sky, hands at rest, as morning light touches the ridge in one gentle line.`,
    law_camp: `A robed teacher and listeners sit on simple ground, tents low behind, everyone still, scrolls closed, windless air over the camp.`,
    genealogy_fallback: `An elder sits beside a tent flap with a small scroll on his knee, names implied by stillness, camp smoke a thin engraved line.`,
    generic_open_land: `${characters} pause on a path through open land, one clear gesture between them, props and ground readable, not a symbolic silhouette.`,
  };
  if (t[scene]) return t[scene];
  if (/井|well/i.test(themeFlat)) {
    return t.well_discovery;
  }
  return `${characters} in ${location}, ${action}, in one literal story moment with recognizable props and no text or symbols.`;
}

function segmentTitleToEnglish(
  title,
  characters,
  location,
  action,
  chapterPayload,
  profilesRoot
) {
  const zh = String(title || "").trim();
  if (/创造|起初|混沌|光/.test(zh)) {
    return templateForSceneKey(
      "creation_light_waters",
      characters,
      location,
      action,
      ""
    );
  }
  if (/井|水/.test(zh)) {
    return templateForSceneKey("well_discovery", characters, location, action, "");
  }
  if (/祭|坛/.test(zh)) {
    return templateForSceneKey("altar_stillness", characters, location, action, "");
  }
  if (/梦|梯|天/.test(zh)) {
    return templateForSceneKey("dream_ladder", characters, location, action, "");
  }

  const people = chapterPayload?.keyPeople || [];
  const namesEn = people
    .slice(0, 4)
    .map((z) => englishNameForPerson(String(z).trim(), profilesRoot))
    .filter(Boolean);
  const who =
    namesEn.length > 0
      ? namesEn.join(" and ")
      : String(characters || "").trim() || "Named biblical figures";

  const beat = zh.slice(0, 56);
  return `${who} in ${location}: a single still frame of this episode — ${beat} — ${action}, concrete gestures and setting, no lettering.`;
}
