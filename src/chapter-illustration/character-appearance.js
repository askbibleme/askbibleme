/**
 * 角色外观档案（与 admin_data/character_illustration_profiles.json 对应）及地名英文化。
 * 用于场景句与出图 prompt 中的跨章一致性描述。
 *
 * 档案内各时期 `imageUrl` / `heroImageUrl` 可为透明全身参考图；章节插画当前仅把外观写入英文 prompt 锁脸。
 * 可选 `statureClass`（根对象为第一时期，其余在 `periods[i]`）：`child` | `youth` | `adult` | `elder`，供前端并列时按年龄缩放参考高度（见 `layoutScaleHintForStature`）。
 * 可选 `heroRosterHeight`（正数，约 0.35–2.5，默认 1）：圣经人物设计器主图横排的相对身高系数（女性略矮、巨人更高），与自动留白补偿相乘。
 * 若将来图片 API 支持参考图输入，可在此层按叙事时期选用对应 `periods[i].imageUrl` 做模板。
 *
 * 产品意图：人物为 AI 生成诠释性插画（非史实照片）。在构图允许时，主人物宜有镇定、庄重的眼神交流感，
 * 便于放在文章或展陈中与读者「对话」；具体由全站出图前缀与下方英文锁定行共同约束。
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
  波提乏的妻子: "Potiphar's wife",
  该隐: "Cain",
  亚伯: "Abel",
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

/** 与人物设计器 `statureClass` 一致；用于前端底对齐并列时的相对身高系数（非像素，仅提示）。 */
export const BCD_STATURE_LAYOUT_SCALE = {
  child: 0.56,
  youth: 0.78,
  adult: 1,
  elder: 0.96,
};

const VALID_STATURE = new Set(["child", "youth", "adult", "elder"]);

/**
 * @param {number} slotIndex 0 = 第一时期（根上字段），1+ 对应 `periods[slotIndex - 1]`
 */
export function statureClassForSlot(entry, slotIndex) {
  const si = Number(slotIndex) || 0;
  if (!entry || typeof entry !== "object") return "adult";
  if (si === 0) {
    const s = String(entry.statureClass || "").trim().toLowerCase();
    return VALID_STATURE.has(s) ? s : "adult";
  }
  const periods = Array.isArray(entry.periods) ? entry.periods : [];
  const p = periods[si - 1];
  if (!p || typeof p !== "object") return "adult";
  const s = String(p.statureClass || "").trim().toLowerCase();
  return VALID_STATURE.has(s) ? s : "adult";
}

export function layoutScaleHintForStature(statureClass) {
  const k = String(statureClass || "").trim().toLowerCase();
  if (VALID_STATURE.has(k)) return BCD_STATURE_LAYOUT_SCALE[k];
  return BCD_STATURE_LAYOUT_SCALE.adult;
}

/**
 * 合并第一时期与可选的额外时期（periods[]）外观描述，供出图「锁定」一行使用。
 */
export function appearanceTextForPromptLock(entry) {
  if (!entry || typeof entry !== "object") return "";
  const parts = [];
  const base = String(entry.appearanceEn || "").trim();
  const pl0 = String(entry.periodLabelZh || "").trim();
  if (base) {
    parts.push(pl0 ? `[${pl0}] ${base}` : base);
  }
  const extras = Array.isArray(entry.periods) ? entry.periods : [];
  for (const ex of extras) {
    if (!ex || typeof ex !== "object") continue;
    const a = String(ex.appearanceEn || "").trim();
    if (!a) continue;
    const lb = String(ex.labelZh || "").trim();
    parts.push(lb ? `[${lb}] ${a}` : a);
  }
  const joined = parts.join(" | ");
  if (parts.length > 1) {
    return `Same person, unified facial identity across life stages (not different actors): ${joined}`;
  }
  return joined;
}

/**
 * slotIndex：0 = 根档案（第一时期）；1 = periods[0]；2 = periods[1]。
 */
export function appearanceEnForSlot(entry, slotIndex) {
  if (!entry || typeof entry !== "object") return "";
  const si = Math.max(0, Number(slotIndex) || 0);
  if (si === 0) return String(entry.appearanceEn || "").trim();
  const periods = Array.isArray(entry.periods) ? entry.periods : [];
  const p = periods[si - 1];
  if (!p || typeof p !== "object") return String(entry.appearanceEn || "").trim();
  const a = String(p.appearanceEn || "").trim();
  return a || String(entry.appearanceEn || "").trim();
}

export function periodLabelZhForSlot(entry, slotIndex) {
  if (!entry || typeof entry !== "object") return "";
  const si = Math.max(0, Number(slotIndex) || 0);
  if (si === 0) return String(entry.periodLabelZh || "").trim();
  const periods = Array.isArray(entry.periods) ? entry.periods : [];
  const p = periods[si - 1];
  return p && typeof p === "object" ? String(p.labelZh || "").trim() : "";
}

const STATURE_SCENE_EN = {
  child:
    "visibly younger — child or young-adolescent facial proportions and softer features (same bone structure as adult portraits of this person, not a different actor)",
  youth:
    "younger face — late-teen to twenties maturity, not a small child (keep recognizable identity vs. elder portraits)",
  adult: "mature adult face and build in narrative prime",
  elder:
    "older — deeper lines, greyer hair or beard as appropriate; same underlying facial structure and identity as younger project portraits, not a different person",
};

/**
 * 插画管理页：从人物设计库勾选「本场景」要锁脸的人物与时期（年龄阶段），写入出图 prompt。
 */
export function buildCharacterLockLinesForRefSelections(
  selections,
  profilesRoot,
  maxPeople = 6
) {
  const raw = Array.isArray(selections) ? selections : [];
  const lines = [];
  const seen = new Set();
  for (const row of raw) {
    if (lines.length >= maxPeople) break;
    if (!row || typeof row !== "object") continue;
    const zh = String(row.zhName || row.nameZh || "").trim();
    if (!zh || seen.has(zh)) continue;
    seen.add(zh);
    const slotIndex = Math.max(0, Number(row.slotIndex) || 0);
    const entry = getCharacterEntry(profilesRoot, zh);
    const en = englishNameForPerson(zh, profilesRoot) || zh;
    const app = appearanceEnForSlot(entry, slotIndex).trim();
    const st = entry ? statureClassForSlot(entry, slotIndex) : "adult";
    const ageLine = STATURE_SCENE_EN[st] || STATURE_SCENE_EN.adult;
    const lb = periodLabelZhForSlot(entry, slotIndex);
    const persEn = entry ? String(entry.scripturePersonalityEn || "").trim() : "";
    const lockParts = [];
    if (app) {
      lockParts.push(
        lb
          ? `Life-stage label [${lb}]: ${app}`
          : app
      );
    }
    lockParts.push(`Age/stature for THIS image: ${ageLine}`);
    if (persEn) lockParts.push(`Scripture-based demeanor: ${persEn}`);
    const lockBody = lockParts.join(" ");
    if (lockBody.trim()) {
      lines.push(
        `${en}: Match the project's existing reference art for this character at this life stage — same facial identity (eyes, nose, jaw, brows, hairline) as the library portrait; ${lockBody}`
      );
    } else if (en && en !== zh) {
      lines.push(
        `${en}: same recognizable face as other chapters in this Bible project; ${ageLine}; ancient Near Eastern dress suited to the narrative`
      );
    }
  }
  if (lines.length > 1) {
    lines.unshift(
      "Distinct faces within this scene (mandatory): Each named figure below must be visually distinguishable — unique facial structure; do NOT reuse one generic face for different people."
    );
  }
  if (lines.length >= 1) {
    lines.unshift(
      "Reference portraits: The descriptions below come from the project character library (generated reference art). Honor them for facial consistency; when the scene implies a different age than the library sheet, still keep the SAME identity — adjust wrinkles, hair color, and skin texture, not bone structure."
    );
  }
  return lines;
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
    const app = appearanceTextForPromptLock(entry).trim();
    const persEn = String(entry?.scripturePersonalityEn || "").trim();
    const lockParts = [];
    if (app) lockParts.push(app);
    if (persEn) lockParts.push(`Scripture-based temperament/demeanor: ${persEn}`);
    const lockBody = lockParts.join(" ");
    if (lockBody) {
      lines.push(`${en}: ${lockBody}`);
    } else if (en && en !== z) {
      lines.push(
        `${en}: same recognizable face and body type as in other chapters of this Bible project; ancient Near Eastern biblical-era clothing suited to narrative phase and social standing — primeval pre-Cain figures (Adam/Eve): animal skins only; later figures: match office and wealth (priest, king, prosperous household, or humble/poor as the text implies) with period-plausible garments — not the same default costume as every other character, and not medieval or modern dress; age-appropriate for the narrative`
      );
    }
  }
  if (lines.length > 1) {
    lines.unshift(
      "Distinct faces within this scene (mandatory): Each named figure below must be visually distinguishable from every other person in the same frame — unique facial bone structure, nose, eyes, brows, jaw, hair/beard pattern, stature, and age; do NOT reuse one generic template face for multiple different people. When adult men and women appear together, preserve believable standing-height difference (women typically shorter than men of the same setting); do not stretch all figures to one uniform height."
    );
  }
  if (lines.length >= 1) {
    lines.unshift(
      "Cross-roster distinctiveness (mandatory): Primary named figure(s) below belong to a project with many biblical characters. Each must look like a specific individual, NOT a generic interchangeable face that could stand in for Abraham, Moses, or another unrelated roster portrait. Honor locked appearance text when provided; otherwise infer clearly differentiated facial structure (bone shape, nose, eyes, brows, jaw, ears, hairline, beard pattern, wrinkles, stature, age) within believable ancient Near Eastern regional diversity."
    );
    lines.unshift(
      "AI-generated interpretive illustration (not a historical photograph). Viewer engagement (intentional): for focal named figures whose faces read clearly, prefer calm dignified eye contact toward the viewer when the narrative moment allows — present alongside text or display; never vacant, wall-eyed, or aggressively glaring."
    );
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
    else if (en) {
      parts.push(
        `${en} in period-appropriate ancient biblical dress suited to narrative standing (wealthy or honored figures: dignified layered garments, not undifferentiated drab sackcloth)`
      );
    }
  }
  if (parts.length === 0) return "";
  if (parts.length === 1) return parts[0];
  if (parts.length === 2) return `${parts[0]} and ${parts[1]}`;
  return `${parts.slice(0, -1).join(", ")}, and ${parts[parts.length - 1]}`;
}
