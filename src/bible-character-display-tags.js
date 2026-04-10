/**
 * 前台展示用人物标签：短词、无全角括号、结构化便于 i18n（英 → 西等）。
 */

const MAX_TAGS = 6;
const MAX_TAG_ZH = 14;
const MAX_TAG_EN = 48;
const MAX_SLUG_LEN = 40;

function s(v) {
  return String(v ?? "").trim();
}

/** 中文标签：去掉任意位置的全角/半角括号（模型或旧数据常带 （）/() 包裹） */
export function stripZhTagWrappers(t) {
  return s(t)
    .replace(/[()（）]/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

export function splitDisplayLabelLine(line) {
  const raw = s(line);
  if (!raw) return [];
  if (raw.includes("·")) {
    return raw
      .split(/\s*·\s*/)
      .map((x) => stripZhTagWrappers(x))
      .filter(Boolean);
  }
  return raw
    .split(/[,，、；;|／/\n]+/)
    .map((x) => stripZhTagWrappers(x))
    .filter(Boolean);
}

function slugFromEn(en) {
  const t = s(en)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!t) return "";
  return t.slice(0, MAX_SLUG_LEN);
}

function slugFromZh(zh, index) {
  let n = 0;
  const str = s(zh);
  for (let i = 0; i < str.length; i++) {
    n = (n * 31 + str.charCodeAt(i)) >>> 0;
  }
  return `z${index}_${n.toString(36)}`.slice(0, MAX_SLUG_LEN);
}

/**
 * @param {unknown} obj
 * @returns {{ slug: string, zh: string, en: string } | null}
 */
export function normalizeIdentityTagEntry(obj, index = 0) {
  if (obj == null) return null;
  if (typeof obj === "string") {
    const zh = stripZhTagWrappers(obj).slice(0, MAX_TAG_ZH);
    if (!zh) return null;
    return {
      slug: slugFromZh(zh, index),
      zh,
      en: "",
    };
  }
  if (typeof obj !== "object") return null;
  const zh = stripZhTagWrappers(obj.zh ?? obj.labelZh ?? "").slice(0, MAX_TAG_ZH);
  const en = s(obj.en ?? obj.labelEn ?? "").slice(0, MAX_TAG_EN);
  let slug = s(obj.slug ?? "");
  if (slug && !/^[a-z][a-z0-9_]*$/i.test(slug)) {
    slug = slugFromEn(slug) || "";
  }
  if (!slug) slug = slugFromEn(en) || slugFromZh(zh || en, index);
  if (!zh && !en) return null;
  return {
    slug: slug.slice(0, MAX_SLUG_LEN),
    zh,
    en,
  };
}

export function normalizeIdentityTagsArray(arr) {
  if (!Array.isArray(arr)) return [];
  const out = [];
  for (let i = 0; i < arr.length && out.length < MAX_TAGS; i++) {
    const one = normalizeIdentityTagEntry(arr[i], out.length);
    if (one) out.push(one);
  }
  return out;
}

/** 从旧版 identityTagsZh（含全角括号）解析为结构化标签 */
export function identityTagsFromLegacyIdentityTagsZh(str) {
  const raw = s(str);
  if (!raw) return [];
  const fromParens = [];
  const re = /（([^）]+)）/g;
  let m;
  while ((m = re.exec(raw)) !== null) {
    const zh = stripZhTagWrappers(m[1]).slice(0, MAX_TAG_ZH);
    if (zh) fromParens.push({ zh, en: "", slug: "" });
  }
  if (fromParens.length) return normalizeIdentityTagsArray(fromParens);
  const fromHalf = [];
  const reHalf = /\(([^)]+)\)/g;
  let mh;
  while ((mh = reHalf.exec(raw)) !== null) {
    const zh = stripZhTagWrappers(mh[1]).slice(0, MAX_TAG_ZH);
    if (zh) fromHalf.push({ zh, en: "", slug: "" });
  }
  if (fromHalf.length) return normalizeIdentityTagsArray(fromHalf);
  const parts = splitDisplayLabelLine(raw);
  return normalizeIdentityTagsArray(parts);
}

export function identityTagsFromParallelLines(zhLine, enLine) {
  const zhParts = splitDisplayLabelLine(zhLine);
  const enParts = splitDisplayLabelLine(enLine);
  const n = Math.max(zhParts.length, enParts.length);
  const paired = [];
  for (let i = 0; i < n && paired.length < MAX_TAGS; i++) {
    const zh = (zhParts[i] != null ? stripZhTagWrappers(zhParts[i]) : "").slice(
      0,
      MAX_TAG_ZH
    );
    const en = (enParts[i] != null ? s(enParts[i]) : "").slice(0, MAX_TAG_EN);
    if (!zh && !en) continue;
    paired.push({
      slug: slugFromEn(en) || slugFromZh(zh || en, paired.length),
      zh,
      en,
    });
  }
  return normalizeIdentityTagsArray(paired);
}

export function formatIdentityTagsLineZh(tags) {
  return normalizeIdentityTagsArray(tags)
    .map((t) => t.zh)
    .filter(Boolean)
    .join(" · ")
    .slice(0, 240);
}

export function formatIdentityTagsLineEn(tags) {
  const parts = normalizeIdentityTagsArray(tags)
    .map((t) => s(t.en))
    .filter(Boolean);
  if (!parts.length) return "";
  return parts.join(" · ").slice(0, 240);
}

/**
 * 合并 AI 或旧字段：优先 identityTags 数组，否则 identityTagsZh，否则 legacy 字符串。
 */
export function coalesceIdentityTagsFromAiPayload(parsed) {
  if (!parsed || typeof parsed !== "object") return [];
  let rawTags = parsed.identityTags;
  if (typeof rawTags === "string" && rawTags.trim()) {
    const t = rawTags.trim();
    if (t.startsWith("[")) {
      try {
        rawTags = JSON.parse(t);
      } catch (_) {
        rawTags = null;
      }
    }
  }
  if (Array.isArray(rawTags) && rawTags.length) {
    return normalizeIdentityTagsArray(rawTags);
  }
  const legacy = s(parsed.identityTagsZh || "");
  if (legacy) return identityTagsFromLegacyIdentityTagsZh(legacy);
  return [];
}

/**
 * 保存档案时：优先 identityTags 数组；否则从并行中英文行解析；两行皆空则 [ ]。
 */
export function mergeStoredCharacterDisplayLabels(incoming) {
  const inc = incoming && typeof incoming === "object" ? incoming : {};
  let tags = [];
  if (Array.isArray(inc.identityTags)) {
    tags = normalizeIdentityTagsArray(inc.identityTags);
  } else {
    const zhLine = s(inc.identityTagsZh ?? "");
    const enLine = s(inc.identityTagsEn ?? "");
    if (zhLine || enLine) {
      const legacyZhTags = identityTagsFromLegacyIdentityTagsZh(zhLine);
      if (legacyZhTags.length > 0 && /[（(]/.test(zhLine)) {
        const ens = splitDisplayLabelLine(enLine);
        tags = legacyZhTags.map((t, i) => ({
          slug:
            t.slug ||
            slugFromEn(ens[i]) ||
            slugFromZh(t.zh, i),
          zh: t.zh,
          en: s(ens[i] || "").slice(0, MAX_TAG_EN),
        }));
        tags = normalizeIdentityTagsArray(tags);
      } else {
        tags = identityTagsFromParallelLines(zhLine, enLine);
      }
    }
  }

  const identityTagsZh = formatIdentityTagsLineZh(tags);
  const identityTagsEn = formatIdentityTagsLineEn(tags);
  return {
    identityTags: tags,
    identityTagsZh,
    identityTagsEn,
  };
}

export function buildReaderRosterIdentityDisplay(entry) {
  const base =
    entry && typeof entry === "object"
      ? mergeStoredCharacterDisplayLabels({
          identityTags: entry.identityTags,
          identityTagsZh: entry.identityTagsZh,
          identityTagsEn: entry.identityTagsEn,
        })
      : { identityTags: [], identityTagsZh: "", identityTagsEn: "" };
  return {
    identityTags: base.identityTags,
    identityTagsZh: base.identityTagsZh,
    identityTagsLineEn: base.identityTagsEn,
  };
}
