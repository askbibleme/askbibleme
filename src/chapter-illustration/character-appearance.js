/**
 * 角色外观档案（与 admin_data/character_illustration_profiles.json 对应）及地名英文化。
 * 用于场景句与出图 prompt 中的跨章一致性描述。
 */

/** 无档案时的默认英文称呼（仍比 "robed figures" 具体） */
export const DEFAULT_ENGLISH_NAME_BY_ZH = {
  以撒: "Isaac",
  亚伯拉罕: "Abraham",
  亚伯兰: "Abraham",
  摩西: "Moses",
  大卫: "David",
  扫罗: "Saul",
  约瑟: "Joseph",
  雅各: "Jacob",
  以扫: "Esau",
  挪亚: "Noah",
  闪: "Shem",
  含: "Ham",
  雅弗: "Japheth",
  他拉: "Terah",
  哈兰: "Haran",
  亚当: "Adam",
  夏娃: "Eve",
  约伯: "Job",
  但以理: "Daniel",
  以利亚: "Elijah",
  以利沙: "Elisha",
  彼得: "Peter",
  保罗: "Paul",
  马利亚: "Mary",
  耶稣: "Jesus",
  基督: "Christ",
  撒拉: "Sarah",
  撒莱: "Sarah",
  撒母耳: "Samuel",
  罗得: "Lot",
  夏甲: "Hagar",
  以实玛利: "Ishmael",
  参孙: "Samson",
  喇合: "Rahab",
  路得: "Ruth",
  波阿斯: "Boaz",
  所罗门: "Solomon",
  利百加: "Rebekah",
  拉结: "Rachel",
  利亚: "Leah",
  拉班: "Laban",
  辟拉: "Bilhah",
  悉帕: "Zilpah",
  便雅悯: "Benjamin",
  犹大: "Judah",
  流便: "Reuben",
  麦基洗德: "Melchizedek",
  法老: "Pharaoh",
  波提乏: "Potiphar",
  波提非拉: "Potiphera",
};

const LOCATION_ZH_TO_EN = {
  旷野: "the wilderness",
  埃及: "Egypt",
  迦南: "the land of Canaan",
  伯特利: "Bethel",
  示剑: "Shechem",
  耶路撒冷: "Jerusalem",
  巴比伦: "Babylon",
  西奈: "Mount Sinai",
  红海: "the sea shore",
  约旦河: "the Jordan river",
  井: "the well",
  坛: "the altar",
  会幕: "the tabernacle courtyard",
  圣殿: "the temple courts",
  橄榄山: "the Mount of Olives",
  伯利恒: "Bethlehem",
  拿撒勒: "Nazareth",
  加利利: "Galilee",
};

function normalizeProfileRoot(raw) {
  if (!raw || typeof raw !== "object") return { characters: {} };
  const ch = raw.characters;
  if (ch && typeof ch === "object" && !Array.isArray(ch)) return { characters: ch };
  return { characters: {} };
}

export function getCharacterEntry(profilesRoot, zhName) {
  const key = String(zhName || "").trim();
  if (!key) return null;
  const { characters } = normalizeProfileRoot(profilesRoot);
  const entry = characters[key];
  if (!entry || typeof entry !== "object") return null;
  return entry;
}

export function englishNameForPerson(zhName, profilesRoot) {
  const entry = getCharacterEntry(profilesRoot, zhName);
  const fromProfile = String(entry?.englishName || "").trim();
  if (fromProfile) return fromProfile;
  return DEFAULT_ENGLISH_NAME_BY_ZH[String(zhName).trim()] || "";
}

/**
 * 供最终 prompt 使用的「角色锁定」行（完整外观，跨章复用）。
 */
export function buildCharacterLockLines(keyPeople, profilesRoot, maxPeople = 4) {
  const names = Array.isArray(keyPeople) ? keyPeople : [];
  const lines = [];
  const seen = new Set();
  for (const zh of names) {
    if (lines.length >= maxPeople) break;
    const z = String(zh || "").trim();
    if (!z || seen.has(z)) continue;
    seen.add(z);
    const entry = getCharacterEntry(profilesRoot, z);
    const en = englishNameForPerson(z, profilesRoot) || z;
    const app = String(entry?.appearanceEn || "").trim();
    if (app) {
      lines.push(`${en}: ${app}`);
    } else if (en && en !== z) {
      lines.push(
        `${en}: same recognizable face and body type as in other chapters of this Bible project; ancient Near Eastern biblical clothing; age-appropriate for the narrative`
      );
    }
  }
  return lines;
}

export function locationsToEnglish(keyLocations) {
  const locs = Array.isArray(keyLocations) ? keyLocations : [];
  const out = [];
  const seen = new Set();
  for (const loc of locs) {
    const z = String(loc || "").trim();
    if (!z) continue;
    const en = LOCATION_ZH_TO_EN[z];
    if (en && !seen.has(en)) {
      seen.add(en);
      out.push(en);
    }
  }
  return out;
}

/**
 * 场景句首：点名人物（英文）+ 极简可辨特征（档案优先）。
 */
export function buildFiguresLeadPhrase(keyPeople, profilesRoot, maxPeople = 3) {
  const names = Array.isArray(keyPeople) ? keyPeople.slice(0, maxPeople) : [];
  if (names.length === 0) return "";
  const parts = [];
  for (const zh of names) {
    const z = String(zh || "").trim();
    if (!z) continue;
    const en = englishNameForPerson(z, profilesRoot);
    const entry = getCharacterEntry(profilesRoot, z);
    const short =
      String(entry?.shortSceneTagEn || "").trim() ||
      (String(entry?.appearanceEn || "").trim().slice(0, 72) || "");
    if (en && short) parts.push(`${en}, ${short.replace(/\.$/, "")}`);
    else if (en) parts.push(`${en} in period-appropriate ancient biblical dress`);
  }
  if (parts.length === 0) return "";
  if (parts.length === 1) return parts[0];
  if (parts.length === 2) return `${parts[0]} and ${parts[1]}`;
  return `${parts.slice(0, -1).join(", ")}, and ${parts[parts.length - 1]}`;
}
