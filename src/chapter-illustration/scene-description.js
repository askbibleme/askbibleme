/**
 * Stage 4 — one concrete English sentence (12–30 words), drawable.
 */

function countWords(s) {
  return String(s || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

function clampSentence(s) {
  let out = String(s || "").replace(/\s+/g, " ").trim();
  let words = out.split(/\s+/).filter(Boolean);
  if (words.length > 32) {
    words = words.slice(0, 30);
    out = words.join(" ") + ".";
  }
  if (words.length < 10) {
    out =
      out +
      " in still air with distant hills under a clear sky band.";
    words = out.split(/\s+/).filter(Boolean);
    if (words.length > 32) out = words.slice(0, 30).join(" ") + ".";
  }
  return out;
}

export function generateSceneDescription(selectedSceneData, chapterPayload) {
  const scene = selectedSceneData.selectedScene;
  const { characters, location, action } = selectedSceneData;
  const { storyUnits, themeFlat } = chapterPayload;

  let sentence = "";

  if (typeof scene === "string" && scene.startsWith("segment_")) {
    const i = parseInt(scene.replace("segment_", ""), 10) || 0;
    const title = storyUnits[i]?.title || themeFlat.slice(0, 60);
    sentence = segmentTitleToEnglish(title, characters, location, action);
  } else {
    sentence = templateForSceneKey(scene, characters, location, action, themeFlat);
  }

  if (!sentence || countWords(sentence) < 8) {
    sentence = `Two robed figures stand on open ground in quiet stillness, ${action}, with ${location} behind them.`;
  }

  return clampSentence(sentence);
}

function templateForSceneKey(scene, characters, location, action, themeFlat) {
  const t = {
    well_discovery: `A man stands beside a stone well in open country while two servants kneel at the rim as water begins to show below, ${location} stretching behind them.`,
    creation_light_waters: `Dark waters lie still beneath a wide sky as a soft radiance divides the deep, no figures, only horizon and calm air above the void.`,
    bush_encounter: `A robed man stands on rocky ground before a low desert bush as a quiet flame-like brightness rests within the branches without consuming them.`,
    altar_stillness: `One figure stands in still posture beside a rough stone altar on open high ground while the camp lies small and far in the haze behind.`,
    covenant_open_sky: `Two small figures stand on a wide plain beneath an open sky, motionless, facing the horizon where light meets earth in a single clear band.`,
    dream_ladder: `A single sleeper lies on stony ground as a narrow stairway rises into night air, meeting soft light above without crowding the frame.`,
    sea_path: `A distant line of people moves through parted waters while the shores stand like walls of still water, frozen in one calm crossing instant.`,
    poetry_landscape: `One lone figure faces quiet hills and an empty sky, hands at rest, as morning light touches the ridge in a single gentle line.`,
    law_camp: `A robed teacher sits on simple ground before a few listeners, tents low behind, everyone still, scrolls closed, windless air over the camp.`,
    genealogy_fallback: `An elder sits beside a tent flap with a small scroll on his knee, names implied by stillness, camp smoke a thin engraved line.`,
    generic_open_land: `Two robed travelers pause on a path through open land, one hand raised slightly, distant hills and empty sky completing the scene.`,
    sea_path: `Figures stand at the shore's edge as still water opens into a narrow path, everyone motionless in one frozen moment before the crossing.`,
  };
  if (t[scene]) return t[scene];
  if (/井|well/i.test(themeFlat)) {
    return t.well_discovery;
  }
  return `Figures in biblical dress stand in ${location}, ${action}, with ${characters} kept readable and few.`;
}

function segmentTitleToEnglish(title, characters, location, action) {
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
  return `Several robed figures gather in ${location} in a hushed tableau, ${action}, suggesting ${zh.slice(0, 20)} without showing text or symbols.`;
}
