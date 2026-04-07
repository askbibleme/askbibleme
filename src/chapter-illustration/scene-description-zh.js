/**
 * 与 generateSceneDescription 平行的中文场景说明，仅供后台阅读；出图仍用英文句。
 */

import { englishNameForPerson } from "./character-appearance.js";

function peopleZhList(payload) {
  const p = payload?.keyPeople;
  if (!Array.isArray(p) || p.length === 0) return "";
  return p
    .slice(0, 5)
    .map((x) => String(x || "").trim())
    .filter(Boolean)
    .join("、");
}

function locsZhList(payload) {
  const p = payload?.keyLocations;
  if (!Array.isArray(p) || p.length === 0) return "";
  return p
    .slice(0, 4)
    .map((x) => String(x || "").trim())
    .filter(Boolean)
    .join("、");
}

function templateZh(scene, payload, profilesRoot) {
  const who = peopleZhList(payload);
  const loc = locsZhList(payload);
  const locIn = loc ? `在${loc}，` : "";
  const whoLead = who ? `${who}` : "人物";

  const t = {
    well_discovery: `${locIn}${whoLead}立于石井旁，仆人在井口俯身，刚见水面，背景为开阔乡野。`,
    creation_light_waters: `深渊之上、穹苍之下，光与水分界，无人物，只有水平线与静谧虚空。`,
    bush_encounter: `${locIn}${whoLead}在旷野岩石地上，面对低矮荆棘丛，其中有光焰状亮光却不烧毁灌木。`,
    altar_stillness: `${locIn}${whoLead}在露天高处石坛边肃立，远处营帐渺小，空气凝滞。`,
    covenant_open_sky: `${locIn}${whoLead}站在广阔平原上仰望天空，天际一道清晰的光带，人物静止。`,
    dream_ladder: `${locIn}${whoLead}卧于石地，夜空中一道阶梯向上延伸，顶端有柔和亮光。`,
    sea_path: `${locIn}${whoLead}在岸边，海水向两侧分开形成通道，众人静止于渡海瞬间，脚下沙土可见。`,
    poetry_landscape: `${locIn}${whoLead}面向远山与长空，双手垂放，晨光轻抚山脊。`,
    law_camp: `营中空地，师长席地而坐，少数听众围坐，帐幕低矮，卷轴合上，无风。`,
    genealogy_fallback: `长者坐于帐门旁，膝上小卷，暗示谱系，远处一缕细烟。`,
    generic_open_land: `${locIn}${whoLead}在旷野小径上驻足，二人之间有明确手势交流，地面与道具可辨，非抽象剪影。`,
  };
  if (t[scene]) return t[scene];
  if (/井/.test(String(payload?.themeFlat || ""))) return t.well_discovery;
  return `${locIn}${whoLead}处于${loc || "开阔地"}，为本章叙事中某一具体瞬间，姿态与器物清晰，画面中无任何文字。`;
}

function segmentZh(i, payload) {
  const title =
    payload?.storyUnits?.[i]?.title ||
    String(payload?.themeFlat || "").slice(0, 80);
  const loc = locsZhList(payload);
  const who = peopleZhList(payload);
  const locPart = loc ? `场景地点涉及：${loc}。` : "";
  const whoPart = who ? `出场人物：${who}。` : "";
  const enNames = (payload?.keyPeople || [])
    .slice(0, 4)
    .map((z) => englishNameForPerson(String(z).trim(), null))
    .filter(Boolean)
    .join("、");
  const enPart = enNames ? `（英文名对照：${enNames}）` : "";
  return `${locPart}${whoPart}本段画面定格在段落主题「${title}」所描述的一刻；人物动作与道具具体，不出现任何文字或符号。${enPart}`;
}

/**
 * @param {object} selectedSceneData — 与 generateSceneDescription 相同
 * @param {object} chapterPayload
 * @param {{ profilesRoot?: object }} options
 */
export function generateSceneDescriptionZh(
  selectedSceneData,
  chapterPayload,
  options = {}
) {
  const profilesRoot = options.profilesRoot || null;
  const scene = selectedSceneData?.selectedScene;
  const payload = chapterPayload || {};

  if (typeof scene === "string" && scene.startsWith("segment_")) {
    const i = parseInt(scene.replace("segment_", ""), 10) || 0;
    return segmentZh(i, payload);
  }

  const zh = String(payload.themeFlat || "").trim();
  if (/创造|起初|混沌|光/.test(zh)) {
    return templateZh("creation_light_waters", payload, profilesRoot);
  }
  if (/井|水/.test(zh) && !scene) {
    return templateZh("well_discovery", payload, profilesRoot);
  }

  return templateZh(scene, payload, profilesRoot);
}
