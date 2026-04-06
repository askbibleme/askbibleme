import * as OpenCC from "/node_modules/opencc-js/dist/esm/cn2t.js";

const FRONT_STATE_KEY = "bible_front_state_v4";
const FONT_SCALE_KEY = "bible_font_scale_v1";
const VIEWPORT_SCROLL_KEY = "bible_viewport_scroll_v1";
const FAVORITES_KEY = "bible_verse_favorites_v1";
const CHAPTER_FAVORITES_KEY = "bible_chapter_favorites_v1";
const QUESTION_FAVORITES_KEY = "bible_question_favorites_v1";
const PENDING_FAVORITE_FOCUS_KEY = "bible_pending_favorite_focus_v1";
const PENDING_QUESTION_FOCUS_KEY = "bible_pending_question_focus_v1";
const GLOBAL_SYNC_VERSE_KEYS = "bible_global_sync_verse_keys_v1";
const GLOBAL_SYNC_QUESTION_KEYS = "bible_global_sync_question_keys_v1";
const LAST_QUESTION_SUBMIT_AT_KEY = "bible_last_question_submit_at_v1";
const USER_AUTH_TOKEN_KEY = "bible_user_auth_token_v1";
const COLOR_THEME_STORAGE_KEY = "bible_color_theme_id_v1";
const QA_INTERACTIONS_KEY = "bible_qa_interactions_v1";
const VERSE_SEARCH_PREFS_KEY = "bible_verse_search_prefs_v1";
let verseSearchDebounceTimer = null;
let verseSearchSeq = 0;
/** Safari/iOS 将「↩」绘成彩色系统符号；用 currentColor SVG 与 ♥/✎ 同色 */
const REPLY_ACTION_GLYPH_SVG = `<svg class="qa-reply-glyph" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="14" height="14" aria-hidden="true" focusable="false"><path fill="currentColor" d="M10 9V5l-7 7 7 7v-4.1c5 0 8.5 1.6 11 5.1-1-5-4-10-11-11z"/></svg>`;
const PUBLISH_MANAGER_FEATURE_VERSION = "v2.1";
const PUBLISH_HISTORY_KEY = "bible_publish_history_v1";
const PUBLISH_LAST_CHANGES_KEY = "bible_publish_last_changes_v1";
const BOOK_NAME_EN_BY_ID = {
  GEN: "Genesis",
  EXO: "Exodus",
  LEV: "Leviticus",
  NUM: "Numbers",
  DEU: "Deuteronomy",
  JOS: "Joshua",
  JDG: "Judges",
  RUT: "Ruth",
  "1SA": "1 Samuel",
  "2SA": "2 Samuel",
  "1KI": "1 Kings",
  "2KI": "2 Kings",
  "1CH": "1 Chronicles",
  "2CH": "2 Chronicles",
  EZR: "Ezra",
  NEH: "Nehemiah",
  EST: "Esther",
  JOB: "Job",
  PSA: "Psalms",
  PRO: "Proverbs",
  ECC: "Ecclesiastes",
  SNG: "Song of Solomon",
  ISA: "Isaiah",
  JER: "Jeremiah",
  LAM: "Lamentations",
  EZK: "Ezekiel",
  DAN: "Daniel",
  HOS: "Hosea",
  JOL: "Joel",
  AMO: "Amos",
  OBA: "Obadiah",
  JON: "Jonah",
  MIC: "Micah",
  NAM: "Nahum",
  HAB: "Habakkuk",
  ZEP: "Zephaniah",
  HAG: "Haggai",
  ZEC: "Zechariah",
  MAL: "Malachi",
  MAT: "Matthew",
  MRK: "Mark",
  LUK: "Luke",
  JHN: "John",
  ACT: "Acts",
  ROM: "Romans",
  "1CO": "1 Corinthians",
  "2CO": "2 Corinthians",
  GAL: "Galatians",
  EPH: "Ephesians",
  PHP: "Philippians",
  COL: "Colossians",
  "1TH": "1 Thessalonians",
  "2TH": "2 Thessalonians",
  "1TI": "1 Timothy",
  "2TI": "2 Timothy",
  TIT: "Titus",
  PHM: "Philemon",
  HEB: "Hebrews",
  JAS: "James",
  "1PE": "1 Peter",
  "2PE": "2 Peter",
  "1JN": "1 John",
  "2JN": "2 John",
  "3JN": "3 John",
  JUD: "Jude",
  REV: "Revelation",
};

const state = {
  bootstrap: null,
  frontState: loadFrontState(),
  scriptureRows: [],
  studyContent: null,
  favorites: loadFavorites(),
  favoriteKeys: new Set(),
  chapterFavorites: loadChapterFavorites(),
  chapterFavoriteKeys: new Set(),
  questionFavorites: loadQuestionFavorites(),
  questionFavoriteKeys: new Set(),
  approvedChapterQuestions: [],
  currentUser: null,
};

/** 收藏夹弹层：pages | verses | questions */
let favoritesPanelActiveTab = "pages";

/** 页面收藏：补全第一小段标题时避免同一 key 并发重复请求 */
const chapterFavoriteSegmentEnrichLocks = new Set();

const adminState = {
  bootstrap: null,
  currentRuleVersion: "default",
  currentRuleConfig: null,
  currentPointsConfig: null,
  testResult: null,
  jobsRefreshTimer: null,
  lastJobsSnapshotKey: "",
  publishedOverview: null,
  lastPublishedBulkResult: null,
  lastPublishedAutoMissingResult: null,
  lastPublishedPanelKind: "bulk",
  lastPublishedChanges: loadLastPublishedChanges(),
  publishHistory: loadPublishHistory(),
  scriptureVersions: [],
  editingScriptureVersionId: "",
};

let saveScrollTimer = null;
const zhHansToHant = OpenCC.Converter({ from: "cn", to: "tw" });

function safeJsonParse(raw, fallback) {
  try {
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

/** fetch 后解析 JSON；网关返回 HTML（502 等）时不会抛错，便于提示用户 */
async function readJsonResponse(res) {
  const text = await res.text();
  const trimmed = String(text || "").trim();
  if (!trimmed) {
    return {
      status: res.status,
      ok: res.ok,
      data: null,
      parseError: true,
      raw: "",
    };
  }
  try {
    return {
      status: res.status,
      ok: res.ok,
      data: JSON.parse(text),
      parseError: false,
      raw: text,
    };
  } catch {
    return {
      status: res.status,
      ok: res.ok,
      data: null,
      parseError: true,
      raw: text,
      bodySnippet: text.slice(0, 200),
    };
  }
}

function loadPublishHistory() {
  const parsed = safeJsonParse(localStorage.getItem(PUBLISH_HISTORY_KEY), []);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map((x) => {
      const actionType =
        x?.actionType === "auto_republish_missing"
          ? "auto_republish_missing"
          : "bulk";
      const base = {
        at: String(x?.at || ""),
        actionType,
        mode: String(x?.mode || "all"),
        dryRun: x?.dryRun === true,
        onlyChanged: x?.onlyChanged !== false,
        matchedPairs: Number(x?.matchedPairs || 0),
      };
      if (actionType === "auto_republish_missing") {
        return {
          ...base,
          totalMissingBefore: Number(x?.totalMissingBefore || 0),
          totalRepublished: Number(x?.totalRepublished || 0),
          totalSkipped: Number(x?.totalSkipped || 0),
          totalFailed: Number(x?.totalFailed || 0),
          totalNoSource: Number(x?.totalNoSource || 0),
        };
      }
      return {
        ...base,
        totalPublishedCount: Number(x?.totalPublishedCount || 0),
        totalSkippedCount: Number(x?.totalSkippedCount || 0),
        changeCount: Number(x?.changeCount || 0),
      };
    })
    .filter((x) => x.at);
}

function savePublishHistory() {
  localStorage.setItem(
    PUBLISH_HISTORY_KEY,
    JSON.stringify((adminState.publishHistory || []).slice(0, 10))
  );
}

function loadLastPublishedChanges() {
  const parsed = safeJsonParse(localStorage.getItem(PUBLISH_LAST_CHANGES_KEY), []);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map((x) => ({
      version: String(x?.version || ""),
      lang: String(x?.lang || ""),
      bookId: String(x?.bookId || ""),
      chapter: Number(x?.chapter || 0),
    }))
    .filter((x) => x.version && x.lang && x.bookId && x.chapter > 0);
}

function saveLastPublishedChanges() {
  localStorage.setItem(
    PUBLISH_LAST_CHANGES_KEY,
    JSON.stringify(adminState.lastPublishedChanges || [])
  );
}

function loadQaInteractions() {
  const parsed = safeJsonParse(localStorage.getItem(QA_INTERACTIONS_KEY), {});
  return parsed && typeof parsed === "object" ? parsed : {};
}

function saveQaInteractions(data) {
  localStorage.setItem(QA_INTERACTIONS_KEY, JSON.stringify(data || {}));
}

function getQaInteractionById(questionId) {
  const all = loadQaInteractions();
  const id = String(questionId || "").trim();
  const row = all[id];
  if (!id || !row || typeof row !== "object") {
    return { liked: false, saved: false };
  }
  return {
    liked: row.liked === true,
    saved: row.saved === true,
  };
}

function setQaInteractionById(questionId, patch) {
  const id = String(questionId || "").trim();
  if (!id) return { liked: false, saved: false };
  const all = loadQaInteractions();
  const prev = all[id] && typeof all[id] === "object" ? all[id] : {};
  const next = {
    liked: patch?.liked === true,
    saved: patch?.saved === true,
  };
  all[id] = { ...prev, ...next };
  saveQaInteractions(all);
  return next;
}

function escapeHtml(str) {
  return String(str ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

const STUDY_ASSET_DEFAULT_ORIGIN = "https://askbible.me";

function normalizeStudyApiOrigin(raw) {
  let s = String(raw || "").trim();
  if (!s) return "";
  s = s.replace(/\/+$/, "");
  if (/\/api$/i.test(s)) s = s.replace(/\/api$/i, "");
  return s.replace(/\/+$/, "");
}

function isLikelyStaticDevServerPortStudy(port) {
  const p = String(port || "").trim();
  if (!p) return false;
  if (p === "5173" || p === "4173" || p === "8081") return true;
  if (/^55\d{2}$/.test(p)) return true;
  return false;
}

function getStudyApiBase() {
  try {
    const m = document.querySelector('meta[name="askbible-api-base"]');
    const fromMeta = m && String(m.getAttribute("content") || "").trim();
    if (fromMeta) return normalizeStudyApiOrigin(fromMeta);
    if (typeof window !== "undefined" && window.__ASKBIBLE_API_BASE__) {
      return normalizeStudyApiOrigin(window.__ASKBIBLE_API_BASE__);
    }
    const proto = window.location.protocol || "";
    if (proto === "capacitor:" || proto === "file:") {
      return STUDY_ASSET_DEFAULT_ORIGIN;
    }
    const h = window.location.hostname || "";
    const port = window.location.port || "";
    if (
      (h === "localhost" || h === "127.0.0.1") &&
      proto === "http:" &&
      isLikelyStaticDevServerPortStudy(port)
    ) {
      return "http://127.0.0.1:3000";
    }
  } catch (_) {}
  return "";
}

function studyApiOriginRootFromNormalizedBase(normalized) {
  if (!normalized) return "";
  try {
    const href = normalized.includes("://")
      ? normalized
      : "http://" + normalized;
    const u = new URL(href);
    return u.origin + "/";
  } catch (_) {
    return "";
  }
}

/** 将 /api/... 转为绝对地址（配图 img、与 Capacitor / 静态页一致） */
function resolveStudyApiPath(path) {
  const p = path.startsWith("/") ? path : "/" + path;
  const base = getStudyApiBase();
  if (!base) return p;
  const normalized = normalizeStudyApiOrigin(base);
  const root = studyApiOriginRootFromNormalizedBase(normalized);
  if (root) {
    try {
      return new URL(p, root).href;
    } catch (_) {
      /* fall through */
    }
  }
  try {
    const origin = normalized.endsWith("/") ? normalized : normalized + "/";
    return new URL(p, origin).href;
  } catch (_) {
    return String(base).replace(/\/$/, "") + p;
  }
}

function renderChapterArtSlotHtml() {
  const art = state.studyContent?.chapterArt;
  const raw = art && String(art.imageUrl || "").trim();
  if (!raw) return "";
  let src = /^https?:\/\//i.test(raw) ? raw : resolveStudyApiPath(raw);
  const cv = String(art?.updatedAt || "").trim();
  if (cv) {
    src += (src.includes("?") ? "&" : "?") + "_cv=" + encodeURIComponent(cv);
  }
  const bookLabel = getBookLabelForPrimaryScripture();
  const ch = Number(state.frontState.chapter || 0);
  const alt = `${bookLabel} 第${ch}章 配图`;
  return `<figure class="chapter-art-figure"><img class="chapter-art-img" src="${escapeHtml(
    src
  )}" alt="${escapeHtml(alt)}" loading="lazy" decoding="async" /></figure>`;
}

/** 搜索结果摘要：转义后包裹匹配片段（纯英文关键词不区分大小写，与服务器一致） */
function highlightVerseSearchSnippet(snippet, query) {
  const raw = String(snippet ?? "");
  const q = String(query ?? "").trim();
  if (!q) return escapeHtml(raw);

  const asciiQuery = !/[^\x00-\x7F]/.test(q);
  const haystack = asciiQuery ? raw.toLowerCase() : raw;
  const needle = asciiQuery ? q.toLowerCase() : q;
  const needleLen = needle.length;
  if (needleLen < 1) return escapeHtml(raw);

  let out = "";
  let i = 0;
  while (i < raw.length) {
    const idx = haystack.indexOf(needle, i);
    if (idx < 0) {
      out += escapeHtml(raw.slice(i));
      break;
    }
    out += escapeHtml(raw.slice(i, idx));
    const matched = raw.slice(idx, idx + needleLen);
    out += `<mark class="verse-search-hit">${escapeHtml(matched)}</mark>`;
    i = idx + needleLen;
  }
  return out;
}

function loadFontScale() {
  const raw = Number(localStorage.getItem(FONT_SCALE_KEY));
  if (Number.isFinite(raw) && raw >= 0.85 && raw <= 1.3) return raw;
  return 1;
}

function saveFontScale() {
  localStorage.setItem(FONT_SCALE_KEY, String(state.frontState.fontScale));
}

function loadViewportScrollY() {
  const raw = Number(localStorage.getItem(VIEWPORT_SCROLL_KEY));
  return Number.isFinite(raw) && raw >= 0 ? raw : 0;
}

function saveViewportScrollY(y) {
  const safeY = Math.max(0, Number(y) || 0);
  localStorage.setItem(VIEWPORT_SCROLL_KEY, String(safeY));
}

function loadFavorites() {
  const parsed = safeJsonParse(localStorage.getItem(FAVORITES_KEY), []);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map((item) => ({
      key: String(item?.key || ""),
      versionId: String(item?.versionId || ""),
      verse: Number(item?.verse || 0),
      text: String(item?.text || ""),
      bookId: String(item?.bookId || ""),
      chapter: Number(item?.chapter || 0),
      createdAt: Number(item?.createdAt || Date.now()),
    }))
    .filter((item) => item.key && item.versionId && item.bookId && item.chapter > 0 && item.verse > 0);
}

function syncFavoriteKeySet() {
  state.favoriteKeys = new Set((state.favorites || []).map((x) => x.key));
}

function saveFavorites() {
  localStorage.setItem(FAVORITES_KEY, JSON.stringify(state.favorites || []));
  syncFavoriteKeySet();
}

function loadChapterFavorites() {
  const parsed = safeJsonParse(localStorage.getItem(CHAPTER_FAVORITES_KEY), []);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map((item) => ({
      key: String(item?.key || ""),
      versionId: String(item?.versionId || ""),
      bookId: String(item?.bookId || ""),
      chapter: Number(item?.chapter || 0),
      contentVersion: String(item?.contentVersion || ""),
      contentLang: String(item?.contentLang || ""),
      firstSegmentTitle: String(item?.firstSegmentTitle || ""),
      createdAt: Number(item?.createdAt || Date.now()),
    }))
    .filter((item) => item.key && item.versionId && item.bookId && item.chapter > 0);
}

function syncChapterFavoriteKeySet() {
  state.chapterFavoriteKeys = new Set((state.chapterFavorites || []).map((x) => x.key));
}

function saveChapterFavorites() {
  localStorage.setItem(
    CHAPTER_FAVORITES_KEY,
    JSON.stringify(state.chapterFavorites || [])
  );
  syncChapterFavoriteKeySet();
}

function makeChapterFavoriteKey(bookId, chapter, versionId) {
  return `chapter|${versionId}|${bookId}|${chapter}`;
}

function getCurrentChapterFavoriteKey() {
  const versionId = String(state.frontState.primaryScriptureVersionId || "");
  const bookId = String(state.frontState.bookId || "");
  const chapter = Number(state.frontState.chapter || 0);
  if (!versionId || !bookId || !chapter) return "";
  return makeChapterFavoriteKey(bookId, chapter, versionId);
}

function loadQuestionFavorites() {
  const parsed = safeJsonParse(localStorage.getItem(QUESTION_FAVORITES_KEY), []);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map((item) => ({
      key: String(item?.key || ""),
      question: String(item?.question || ""),
      title: String(item?.title || ""),
      bookId: String(item?.bookId || ""),
      chapter: Number(item?.chapter || 0),
      contentVersion: String(item?.contentVersion || ""),
      contentLang: String(item?.contentLang || ""),
      createdAt: Number(item?.createdAt || Date.now()),
    }))
    .filter((item) => item.key && item.question && item.bookId && item.chapter > 0);
}

function syncQuestionFavoriteKeySet() {
  state.questionFavoriteKeys = new Set((state.questionFavorites || []).map((x) => x.key));
}

function saveQuestionFavorites() {
  localStorage.setItem(
    QUESTION_FAVORITES_KEY,
    JSON.stringify(state.questionFavorites || [])
  );
  syncQuestionFavoriteKeySet();
}

async function reportGlobalFavoriteToggle(payload) {
  try {
    await fetch("/api/global-favorites/toggle", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (error) {
    console.warn("global favorite sync failed:", error);
  }
}

function loadSyncedKeySet(storageKey) {
  const parsed = safeJsonParse(localStorage.getItem(storageKey), []);
  if (!Array.isArray(parsed)) return new Set();
  return new Set(parsed.map((x) => String(x || "")).filter(Boolean));
}

function saveSyncedKeySet(storageKey, keySet) {
  localStorage.setItem(storageKey, JSON.stringify(Array.from(keySet)));
}

function backfillGlobalFavoritesFromLocal() {
  const syncedVerseKeys = loadSyncedKeySet(GLOBAL_SYNC_VERSE_KEYS);
  const syncedQuestionKeys = loadSyncedKeySet(GLOBAL_SYNC_QUESTION_KEYS);
  let verseChanged = false;
  let questionChanged = false;

  for (const item of state.favorites || []) {
    const key = String(item?.key || "");
    if (!key || syncedVerseKeys.has(key)) continue;
    syncedVerseKeys.add(key);
    verseChanged = true;
    reportGlobalFavoriteToggle({
      action: "add",
      type: "verse",
      key,
      bookId: item.bookId,
      chapter: Number(item.chapter || 0),
      verse: Number(item.verse || 0),
      title:
        formatBookChapterLabel(item.bookId, Number(item.chapter || 0)) +
        ":" +
        Number(item.verse || 0),
      content: String(item.text || ""),
    });
  }

  for (const item of state.questionFavorites || []) {
    const key = String(item?.key || "");
    if (!key || syncedQuestionKeys.has(key)) continue;
    syncedQuestionKeys.add(key);
    questionChanged = true;
    reportGlobalFavoriteToggle({
      action: "add",
      type: "question",
      key,
      bookId: item.bookId,
      chapter: Number(item.chapter || 0),
      title: String(item.title || ""),
      content: String(item.question || ""),
      contentVersion: String(item.contentVersion || ""),
      contentLang: String(item.contentLang || ""),
    });
  }

  if (verseChanged) {
    saveSyncedKeySet(GLOBAL_SYNC_VERSE_KEYS, syncedVerseKeys);
  }
  if (questionChanged) {
    saveSyncedKeySet(GLOBAL_SYNC_QUESTION_KEYS, syncedQuestionKeys);
  }
}

function loadFrontState() {
  const parsed = safeJsonParse(localStorage.getItem(FRONT_STATE_KEY), null);

  const legacyScriptureIds =
    Array.isArray(parsed?.scriptureVersionIds) &&
    parsed.scriptureVersionIds.length
      ? parsed.scriptureVersionIds
      : [];

  return {
    uiLang: parsed?.uiLang || "zh",
    contentVersion: parsed?.contentVersion || "default",
    contentLang: parsed?.contentLang || "zh",
    primaryScriptureVersionId:
      parsed?.primaryScriptureVersionId || legacyScriptureIds[0] || "cuvs_zh",
    secondaryScriptureVersionIds: Array.isArray(
      parsed?.secondaryScriptureVersionIds
    )
      ? parsed.secondaryScriptureVersionIds
      : legacyScriptureIds.slice(1).length
      ? legacyScriptureIds.slice(1)
      : [],
    testament: parsed?.testament || "旧约",
    bookId: parsed?.bookId || "GEN",
    chapter: Number(parsed?.chapter || 1),
    hideScripture: parsed?.hideScripture === true,
    showQuestions:
      typeof parsed?.showQuestions === "boolean"
        ? parsed.showQuestions
        : true,
    showScripture:
      typeof parsed?.showScripture === "boolean"
        ? parsed.showScripture
        : true,
    fontScale: loadFontScale(),
  };
}

function saveFrontState() {
  localStorage.setItem(
    FRONT_STATE_KEY,
    JSON.stringify({
      uiLang: state.frontState.uiLang,
      contentVersion: state.frontState.contentVersion,
      contentLang: state.frontState.contentLang,
      primaryScriptureVersionId: state.frontState.primaryScriptureVersionId,
      secondaryScriptureVersionIds:
        state.frontState.secondaryScriptureVersionIds,
      testament: state.frontState.testament,
      bookId: state.frontState.bookId,
      chapter: state.frontState.chapter,
      hideScripture: state.frontState.hideScripture,
      showQuestions: state.frontState.showQuestions,
      showScripture: state.frontState.showScripture,
    })
  );
}

function applyFontScale() {
  document.documentElement.style.setProperty(
    "--font-scale",
    String(state.frontState.fontScale)
  );
}

function getBooksForCurrentTestament() {
  const allBooks = state.bootstrap?.testamentOptions || [];
  return allBooks.filter((b) => b.testamentName === state.frontState.testament);
}

function getBooksForTestament(testamentName) {
  const allBooks = state.bootstrap?.testamentOptions || [];
  return allBooks.filter((b) => b.testamentName === testamentName);
}

/** 旧约书卷分组（与 src/books.js 顺序一致，小标题随界面语言 uiLang） */
const OLD_TESTAMENT_SECTIONS = [
  {
    labels: {
      zh: "摩西五经",
      en: "The Pentateuch",
      es: "El Pentateuco",
      he: "תורה",
    },
    bookIds: ["GEN", "EXO", "LEV", "NUM", "DEU"],
  },
  {
    labels: {
      zh: "历史书",
      en: "Historical Books",
      es: "Libros históricos",
      he: "ספרי ההיסטוריה",
    },
    bookIds: [
      "JOS",
      "JDG",
      "RUT",
      "1SA",
      "2SA",
      "1KI",
      "2KI",
      "1CH",
      "2CH",
      "EZR",
      "NEH",
      "EST",
    ],
  },
  {
    labels: {
      zh: "诗歌智慧书",
      en: "Poetry & Wisdom",
      es: "Poesía y sabiduría",
      he: "כתובים",
    },
    bookIds: ["JOB", "PSA", "PRO", "ECC", "SNG"],
  },
  {
    labels: {
      zh: "大先知书",
      en: "Major Prophets",
      es: "Profetas mayores",
      he: "נביאים גדולים",
    },
    bookIds: ["ISA", "JER", "LAM", "EZK", "DAN"],
  },
  {
    labels: {
      zh: "小先知书",
      en: "Minor Prophets",
      es: "Profetas menores",
      he: "נביאים קטנים",
    },
    bookIds: [
      "HOS",
      "JOL",
      "AMO",
      "OBA",
      "JON",
      "MIC",
      "NAM",
      "HAB",
      "ZEP",
      "HAG",
      "ZEC",
      "MAL",
    ],
  },
];

/** 新约书卷分组（与 src/books.js 顺序一致） */
const NEW_TESTAMENT_SECTIONS = [
  {
    labels: {
      zh: "四福音书",
      en: "The Four Gospels",
      es: "Los cuatro evangelios",
      he: "ארבע הבשורות",
    },
    bookIds: ["MAT", "MRK", "LUK", "JHN"],
  },
  {
    labels: {
      zh: "使徒行传",
      en: "Acts of the Apostles",
      es: "Hechos de los apóstoles",
      he: "מעשי השליחים",
    },
    bookIds: ["ACT"],
  },
  {
    labels: {
      zh: "保罗书信",
      en: "Paul's Letters",
      es: "Epístolas de Pablo",
      he: "אגרות פאולוס",
    },
    bookIds: [
      "ROM",
      "1CO",
      "2CO",
      "GAL",
      "EPH",
      "PHP",
      "COL",
      "1TH",
      "2TH",
      "1TI",
      "2TI",
      "TIT",
      "PHM",
    ],
  },
  {
    labels: {
      zh: "普通书信",
      en: "General Epistles",
      es: "Epístolas generales",
      he: "אגרות כלליות",
    },
    bookIds: ["HEB", "JAS", "1PE", "2PE", "1JN", "2JN", "3JN", "JUD"],
  },
  {
    labels: {
      zh: "启示录",
      en: "The Revelation",
      es: "El Apocalipsis",
      he: "חזון יוחנן",
    },
    bookIds: ["REV"],
  },
];

/** 新教传统书卷顺序 1–66（与上列分组顺序一致） */
const BOOK_CANONICAL_ORDER_MAP = (() => {
  const m = new Map();
  let n = 1;
  for (const s of OLD_TESTAMENT_SECTIONS) {
    for (const id of s.bookIds) {
      if (!m.has(id)) m.set(id, n++);
    }
  }
  for (const s of NEW_TESTAMENT_SECTIONS) {
    for (const id of s.bookIds) {
      if (!m.has(id)) m.set(id, n++);
    }
  }
  return m;
})();

function formatBookGridButtonLabel(book, bookLabelFn) {
  const ord = BOOK_CANONICAL_ORDER_MAP.get(book.bookId);
  const name = bookLabelFn(book);
  return ord != null ? `${ord}. ${name}` : name;
}

/** 与 getLocalizedCopy、书卷名一致：按主经文语言，不用 uiLang（避免英文经卷 + 中文界面时分组仍中文） */
function pickBibleGridSectionLabel(labels) {
  let lang = getPrimaryScriptureLang() || "zh";
  if (typeof lang === "string") {
    if (lang.startsWith("en")) lang = "en";
    else if (lang.startsWith("es")) lang = "es";
    else if (lang.startsWith("he")) lang = "he";
  }
  return (
    labels[lang] || labels.zh || labels.en || Object.values(labels)[0] || ""
  );
}

function buildGroupedBookGridHtml(books, bookLabel, sections) {
  const booksById = new Map(books.map((b) => [b.bookId, b]));
  const mappedIds = new Set(sections.flatMap((s) => s.bookIds));

  const buttonHtml = (book) => {
    const active = book.bookId === state.frontState.bookId ? "active" : "";
    return `<button type="button" class="book-item ${active}" data-book-grid-id="${escapeHtml(
      book.bookId
    )}">${escapeHtml(formatBookGridButtonLabel(book, bookLabel))}</button>`;
  };

  const parts = [];
  for (const section of sections) {
    const sectionBooks = section.bookIds
      .map((id) => booksById.get(id))
      .filter(Boolean);
    if (!sectionBooks.length) continue;
    parts.push(
      `<div class="book-bible-section-title" role="heading" aria-level="3">${escapeHtml(
        pickBibleGridSectionLabel(section.labels)
      )}</div>`
    );
    for (const book of sectionBooks) {
      parts.push(buttonHtml(book));
    }
  }

  const extra = books.filter((b) => !mappedIds.has(b.bookId));
  if (extra.length) {
    parts.push(
      `<div class="book-bible-section-title" role="heading" aria-level="3">${escapeHtml(
        pickBibleGridSectionLabel({
          zh: "其他",
          en: "Other books",
          es: "Otros libros",
          he: "אחר",
        })
      )}</div>`
    );
    for (const book of extra) {
      parts.push(buttonHtml(book));
    }
  }

  return parts.join("");
}

function buildOldTestamentBookGridHtml(books, bookLabel) {
  return buildGroupedBookGridHtml(books, bookLabel, OLD_TESTAMENT_SECTIONS);
}

function buildNewTestamentBookGridHtml(books, bookLabel) {
  return buildGroupedBookGridHtml(books, bookLabel, NEW_TESTAMENT_SECTIONS);
}

function getCurrentBookMeta() {
  return (
    (state.bootstrap?.testamentOptions || []).find(
      (b) => b.bookId === state.frontState.bookId
    ) || null
  );
}

function getCurrentBookLabel() {
  const book = getCurrentBookMeta();
  if (!book) return state.frontState.bookId;

  if (state.frontState.uiLang === "en")
    return book.bookEn || book.bookCn || book.bookId;
  if (state.frontState.uiLang === "es")
    return book.bookEn || book.bookCn || book.bookId;
  return book.bookCn || book.bookEn || book.bookId;
}

function getBookLabelForPrimaryScripture() {
  const book = getCurrentBookMeta();
  if (!book) return state.frontState.bookId;

  const primaryLang = getPrimaryScriptureVersion()?.lang || "";
  if (primaryLang === "zh") {
    return book.bookCn || book.bookEn || book.bookId;
  }
  if (primaryLang === "en" || primaryLang === "es" || primaryLang === "he") {
    return BOOK_NAME_EN_BY_ID[book.bookId] || book.bookEn || book.bookId;
  }
  return book.bookEn || BOOK_NAME_EN_BY_ID[book.bookId] || book.bookId;
}

function getPrimaryScriptureLang() {
  return getPrimaryScriptureVersion()?.lang || "zh";
}

function formatBookChapterLabel(bookLabel, chapter) {
  const lang = getPrimaryScriptureLang();
  if (lang === "zh") return `${bookLabel} ${chapter}章`;
  if (lang === "en") return `${bookLabel} ${chapter}`;
  if (lang === "es") return `${bookLabel} ${chapter}`;
  if (lang === "he") return `${bookLabel} ${chapter}`;
  return `${bookLabel} ${chapter}`;
}

function getLocalizedCopy() {
  const lang = getPrimaryScriptureLang();
  if (lang === "en") {
    return {
      triggerBook: "Select book",
      triggerTranslation: "Select version",
      triggerVersion: "Question type",
      bookChapter: "Select book",
      searchScripture: "Search",
      display: "Display",
      type: "Type",
      bibleVersion: "Select version",
      close: "Close",
      oldTestament: "Old Testament",
      newTestament: "New Testament",
      chapters: "Chapters",
      displayContent: "Display Content",
      quickActions: "Quick Actions",
      all: "All",
      scripture: "Scripture",
      questions: "Questions",
      export: "Export",
      print: "Print",
      primaryVersionSingle: "Primary (single)",
      compareVersionMulti: "Compare (multi)",
      noCompareVersion: "No compare versions available",
      allDisplay: "Show All",
      onlyScripture: "Scripture Only",
      onlyQuestion: "Questions Only",
      favorites: "Favorites",
      favoritesTitle: "Favorites",
      removeFavoriteAria: "Remove from favorites",
      emptyFavorites: "No favorites yet. Double-click a verse or use the left ribbon to bookmark a chapter.",
      favoritesSectionPageTitle: "Page bookmarks",
      favoritesSectionVerseTitle: "Verse bookmarks",
      favoritesSectionQuestionTitle: "Question bookmarks",
      favoritesSectionEmptyPage: "None yet — use the left ribbon to save this page (chapter).",
      favoritesSectionEmptyVerse: "None yet — double-click a verse to save.",
      favoritesSectionEmptyQuestion: "None yet — double-click a question to save.",
      favoritesListRibbon: "Saved list",
      favoritesListOpenAria: "Open saved list to jump to a bookmark",
      ribbonChapterSave: "Bookmark chapter",
      ribbonChapterSaved: "Chapter saved",
      ribbonChapterSaveAria: "Bookmark this chapter",
      ribbonChapterSavedAria: "This chapter is bookmarked; click to remove",
      favoriteChapterHint: "Whole chapter",
      verseFavoriteAddedToast: "Verse saved to bookmarks",
      verseFavoriteRemovedToast: "Removed from verse bookmarks",
      questionFavoriteAddedToast: "Question saved to bookmarks",
      questionFavoriteRemovedToast: "Removed from question bookmarks",
      prevChapter: "Previous",
      nextChapter: "Next",
      noContent: "No content yet for this chapter in the selected version/language.",
      promoHelpAria:
        "About Berean-style reading and AskBible.me (opens in new tab)",
    };
  }
  if (lang === "es") {
    return {
      triggerBook: "Elegir libro",
      triggerTranslation: "Elegir version",
      triggerVersion: "Tipo de preguntas",
      bookChapter: "Elegir libro",
      searchScripture: "Buscar",
      display: "Mostrar",
      type: "Tipo",
      bibleVersion: "Elegir version",
      close: "Cerrar",
      oldTestament: "Antiguo Testamento",
      newTestament: "Nuevo Testamento",
      chapters: "Capitulos",
      displayContent: "Contenido",
      quickActions: "Acciones rapidas",
      all: "Todo",
      scripture: "Escritura",
      questions: "Preguntas",
      export: "Exportar",
      print: "Imprimir",
      primaryVersionSingle: "Principal (una)",
      compareVersionMulti: "Comparar (multi)",
      noCompareVersion: "No hay versiones de comparacion disponibles",
      allDisplay: "Mostrar todo",
      onlyScripture: "Solo escritura",
      onlyQuestion: "Solo preguntas",
      favorites: "Favoritos",
      favoritesTitle: "Favoritos",
      removeFavoriteAria: "Quitar de favoritos",
      emptyFavorites: "Aun no hay favoritos. Doble clic en un versiculo o la cinta izquierda para guardar un capitulo.",
      favoritesSectionPageTitle: "Marcadores de pagina",
      favoritesSectionVerseTitle: "Versiculos",
      favoritesSectionQuestionTitle: "Preguntas",
      favoritesSectionEmptyPage: "Ninguno — usa la cinta izquierda para esta pagina (capitulo).",
      favoritesSectionEmptyVerse: "Ninguno — doble clic en un versiculo.",
      favoritesSectionEmptyQuestion: "Ninguno — doble clic en una pregunta.",
      favoritesListRibbon: "Lista guardados",
      favoritesListOpenAria: "Abrir lista para ir a un favorito",
      ribbonChapterSave: "Guardar capitulo",
      ribbonChapterSaved: "Capitulo guardado",
      ribbonChapterSaveAria: "Guardar este capitulo",
      ribbonChapterSavedAria: "Capitulo guardado; clic para quitar",
      favoriteChapterHint: "Capitulo entero",
      verseFavoriteAddedToast: "Versiculo guardado en marcadores",
      verseFavoriteRemovedToast: "Quitado de marcadores de versiculos",
      questionFavoriteAddedToast: "Pregunta guardada en marcadores",
      questionFavoriteRemovedToast: "Quitada de marcadores de preguntas",
      prevChapter: "Anterior",
      nextChapter: "Siguiente",
      noContent: "Aun no hay contenido para este capitulo en la version/idioma seleccionados.",
      promoHelpAria: "Sobre el estilo Berea y AskBible.me (nueva pestana)",
    };
  }
  if (lang === "he") {
    return {
      triggerBook: "בחירת ספר",
      triggerTranslation: "בחירת גרסה",
      triggerVersion: "סוג שאלות",
      bookChapter: "בחירת ספר",
      searchScripture: "חיפוש",
      display: "תצוגה",
      type: "סוג",
      bibleVersion: "בחירת גרסה",
      close: "סגור",
      oldTestament: "הברית הישנה",
      newTestament: "הברית החדשה",
      chapters: "פרקים",
      displayContent: "תוכן תצוגה",
      quickActions: "פעולות מהירות",
      all: "הכול",
      scripture: "כתובים",
      questions: "שאלות",
      export: "ייצוא",
      print: "הדפסה",
      primaryVersionSingle: "ראשית (בחירה אחת)",
      compareVersionMulti: "השוואה (רב-בחירה)",
      noCompareVersion: "אין גרסאות להשוואה",
      allDisplay: "הצג הכול",
      onlyScripture: "כתובים בלבד",
      onlyQuestion: "שאלות בלבד",
      favorites: "מועדפים",
      favoritesTitle: "מועדפים",
      removeFavoriteAria: "הסר מהמועדפים",
      emptyFavorites: "אין עדיין מועדפים. לחיצה כפולה על פסוק או הסרט השמאלי לשמירת פרק.",
      favoritesSectionPageTitle: "סימניות עמוד",
      favoritesSectionVerseTitle: "פסוקים שמורים",
      favoritesSectionQuestionTitle: "שאלות שמורות",
      favoritesSectionEmptyPage: "אין — השתמש בסרט השמאלי לשמירת העמוד (פרק).",
      favoritesSectionEmptyVerse: "אין — לחיצה כפולה על פסוק.",
      favoritesSectionEmptyQuestion: "אין — לחיצה כפולה על שאלה.",
      favoritesListRibbon: "רשימת שמורים",
      favoritesListOpenAria: "פתח רשימה לקפיצה לסימנייה",
      ribbonChapterSave: "שמור פרק",
      ribbonChapterSaved: "הפרק נשמר",
      ribbonChapterSaveAria: "שמור את הפרק הנוכחי",
      ribbonChapterSavedAria: "הפרק נשמר; לחץ להסרה",
      favoriteChapterHint: "פרק שלם",
      verseFavoriteAddedToast: "הפסוק נשמר בסימניות",
      verseFavoriteRemovedToast: "הוסר מסימניות הפסוקים",
      questionFavoriteAddedToast: "השאלה נשמרה בסימניות",
      questionFavoriteRemovedToast: "הוסר מסימניות השאלות",
      prevChapter: "הקודם",
      nextChapter: "הבא",
      noContent: "עדיין אין תוכן לפרק זה בגרסה או בשפה שנבחרו.",
      promoHelpAria: "אודות לימוד בסגנון בראיים ו-AskBible.me (בלשונית חדשה)",
    };
  }
  return {
    triggerBook: "书卷",
    triggerTranslation: "版本+",
    triggerVersion: "问题类型",
    bookChapter: "书卷",
    searchScripture: "搜索",
    display: "显示",
    type: "类型",
    bibleVersion: "版本+",
    close: "关闭",
    oldTestament: "旧约",
    newTestament: "新约",
    chapters: "章节",
    displayContent: "显示内容",
    quickActions: "快捷操作",
    all: "全部",
    scripture: "经文",
    questions: "问题",
    export: "导出",
    print: "打印",
    primaryVersionSingle: "主版本（单选）",
    compareVersionMulti: "对照版本（可多选）",
    noCompareVersion: "没有可选对照版本",
    allDisplay: "全部显示",
    onlyScripture: "只限经文",
    onlyQuestion: "只限问题",
    favorites: "收藏",
    favoritesTitle: "收藏夹",
    removeFavoriteAria: "从收藏中移除",
    emptyFavorites: "还没有收藏：可双击经文，或点左侧丝带收藏整章。",
    favoritesSectionPageTitle: "页面收藏",
    favoritesSectionVerseTitle: "经文收藏",
    favoritesSectionQuestionTitle: "问题收藏",
    favoritesSectionEmptyPage: "暂无：点左侧丝带可收藏当前页（整章）。",
    favoritesSectionEmptyVerse: "暂无：双击经文可收藏。",
    favoritesSectionEmptyQuestion: "暂无：双击问题可收藏。",
    favoritesListRibbon: "收藏列表",
    favoritesListOpenAria: "打开收藏列表，跳转到已收藏的经文或章节",
    ribbonChapterSave: "收藏本章",
    ribbonChapterSaved: "本页已收藏",
    ribbonChapterSaveAria: "将当前章加入收藏",
    ribbonChapterSavedAria: "本页已收藏，点击取消收藏",
    favoriteChapterHint: "整章书签",
    verseFavoriteAddedToast: "已加入经文收藏",
    verseFavoriteRemovedToast: "已从经文收藏移除",
    questionFavoriteAddedToast: "已加入问题收藏",
    questionFavoriteRemovedToast: "已从问题收藏移除",
    prevChapter: "上一章",
    nextChapter: "下一章",
    noContent: "这一章还没有该版本 / 该语言的内容。",
    promoHelpAria: "了解庇哩亚式读经与 AskBible.me（新窗口打开）",
  };
}

function applyReaderI18n() {
  const copy = getLocalizedCopy();
  const setText = (selector, value) => {
    const el = document.querySelector(selector);
    if (el) el.textContent = value;
  };

  setText("#bookChapterPanel .toolbar-panel-title", copy.bookChapter);
  setText("#qaViewPanel .toolbar-panel-title", copy.displayContent);
  setText("#primaryVersionPanel .toolbar-panel-title", copy.bibleVersion);
  setText("#verseSearchTitle", copy.searchScripture);
  document
    .querySelectorAll(
      '#bookChapterPanel [data-book-chapter-section="ot"]'
    )
    .forEach((el) => {
      el.textContent = copy.oldTestament;
    });
  document
    .querySelectorAll(
      '#bookChapterPanel [data-book-chapter-section="nt"]'
    )
    .forEach((el) => {
      el.textContent = copy.newTestament;
    });
  document
    .querySelectorAll(
      '#bookChapterPanel [data-book-chapter-section="chapters"]'
    )
    .forEach((el) => {
      el.textContent = copy.chapters;
    });
  setText("#qaViewPanel .chapter-grid-title", copy.quickActions);
  setText("#contentVersionSectionTitle", copy.triggerVersion || "问题类型");
  setText("#primaryVersionSectionTitle", copy.primaryVersionSingle);
  setText("#compareVersionSectionTitle", copy.compareVersionMulti);
  setText("#exportPrettyPdfBtn", copy.export);
  setText("#favoritesPanelTitle", copy.favoritesTitle);
  setText("#favoritesTabPages", copy.favoritesSectionPageTitle || "");
  setText("#favoritesTabVerses", copy.favoritesSectionVerseTitle || "");
  setText("#favoritesTabQuestions", copy.favoritesSectionQuestionTitle || "");

  document
    .querySelectorAll(
      "#bookChapterPanel .toolbar-panel-close, #qaViewPanel .toolbar-panel-close, #primaryVersionPanel .toolbar-panel-close, #favoritesPanel .toolbar-panel-close"
    )
    .forEach((btn) => {
      btn.textContent = copy.close;
    });
}

function getLocalizedContentVersionLabel(itemOrId) {
  const id = typeof itemOrId === "string" ? itemOrId : itemOrId?.id;
  const fallback =
    (typeof itemOrId === "string" ? "" : itemOrId?.label) || id || "";
  const lang = getPrimaryScriptureLang();
  const labels = {
    default: { zh: "默认版", en: "Default", es: "Predeterminado", he: "ברירת מחדל" },
    gospel: { zh: "福音版", en: "Gospel", es: "Evangelio", he: "בשורה" },
    children: { zh: "儿童版", en: "Children", es: "Ninos", he: "ילדים" },
    youth: { zh: "青少年版", en: "Youth", es: "Jovenes", he: "נוער" },
    couple: { zh: "夫妇版", en: "Couples", es: "Parejas", he: "זוגות" },
    workplace: { zh: "职场版", en: "Workplace", es: "Trabajo", he: "עבודה" },
  };
  const entry = labels[id] || null;
  if (!entry) return fallback;
  return entry[lang] || entry.zh || fallback;
}

function getEnabledScriptureVersions() {
  return (state.bootstrap?.scriptureVersions || []).slice().sort((a, b) => {
    return Number(a.sortOrder || 999) - Number(b.sortOrder || 999);
  });
}

function getPrimaryVersionCandidates() {
  const preferredIds = new Set([
    "cuvs_zh",
    "cuv_zh_tw",
    "bbe_en",
    "web_en",
    "rv1909_es",
  ]);
  const options = getEnabledScriptureVersions().filter((x) =>
    preferredIds.has(x.id)
  );
  return options.length ? options : getEnabledScriptureVersions();
}

function getScriptureVersionById(id) {
  return getEnabledScriptureVersions().find((x) => x.id === id) || null;
}

function getAllSelectedScriptureVersionIds() {
  const ids = [
    state.frontState.primaryScriptureVersionId,
    ...(state.frontState.secondaryScriptureVersionIds || []),
  ].filter(Boolean);

  return Array.from(new Set(ids));
}

function getSecondaryScriptureVersions() {
  return (state.frontState.secondaryScriptureVersionIds || [])
    .map((id) => getScriptureVersionById(id))
    .filter(Boolean);
}

function getPrimaryScriptureVersion() {
  return getScriptureVersionById(state.frontState.primaryScriptureVersionId);
}

function getCurrentContentVersionLabel() {
  const found = (state.bootstrap?.contentVersions || []).find(
    (x) => x.id === state.frontState.contentVersion
  );
  return (
    getLocalizedContentVersionLabel(found || state.frontState.contentVersion) ||
    getLocalizedCopy().type
  );
}

function makeFavoriteKey(versionId, verse) {
  return `${versionId}|${state.frontState.bookId}|${state.frontState.chapter}|${verse}`;
}

function getBookMetaById(bookId) {
  return (state.bootstrap?.testamentOptions || []).find((b) => b.bookId === bookId) || null;
}

function getLocalizedBookLabelById(bookId) {
  const book = getBookMetaById(bookId);
  if (!book) return bookId;
  const lang = getPrimaryScriptureLang();
  if (lang === "zh") return book.bookCn || BOOK_NAME_EN_BY_ID[bookId] || bookId;
  return BOOK_NAME_EN_BY_ID[bookId] || book.bookEn || book.bookCn || bookId;
}

function isDivineSpeechVerseText(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return false;

  const patterns = [
    /耶和华说/u,
    /主说/u,
    /神说/u,
    /耶和华\s*说/u,
    /主\s*说/u,
    /神\s*说/u,
    /耶稣说/u,
    /圣灵说/u,
    /耶和华如此说/u,
    /主如此说/u,
    /神如此说/u,
    /耶和华晓谕/u,
    /主晓谕/u,
    /神晓谕/u,
    /耶和华吩咐/u,
    /主吩咐/u,
    /神吩咐/u,
    /耶和华向.*显现.*说/u,
    /主向.*显现.*说/u,
    /神向.*显现.*说/u,
    /耶和华向.*显现/u,
    /主向.*显现/u,
    /神向.*显现/u,
    /耶和华临到.*说/u,
    /主临到.*说/u,
    /神临到.*说/u,
    /耶和华应许.*说/u,
    /主应许.*说/u,
    /神应许.*说/u,
    /耶和华对.*说/u,
    /主对.*说/u,
    /神对.*说/u,
    /耶稣对.*说/u,
    /耶和华曰/u,
    /主曰/u,
    /神曰/u,
    /耶和华云/u,
    /主云/u,
    /神云/u,
    /\bthe\s+lord\s+said\b/i,
    /\bgod\s+said\b/i,
    /\bjesus\s+said\b/i,
    /\bthus\s+says?\s+the\s+lord\b/i,
    /\bthe\s+lord\s+spoke\b/i,
    /\bgod\s+spoke\b/i,
    /\bthe\s+lord\s+spoke\s+to\b/i,
    /\bgod\s+spoke\s+to\b/i,
    /\bjesus\s+spoke\s+to\b/i,
    /\bel\s+senor\s+dijo\b/i,
    /\bdios\s+dijo\b/i,
    /\bjesus\s+dijo\b/i,
    /\bas[ií]\s+dice\s+el\s+se[nñ]or\b/i,
    /\bel\s+se[nñ]or\s+habl[oó]\b/i,
    /\bdios\s+habl[oó]\b/i,
    /\bel\s+se[nñ]or\s+habl[oó]\s+a\b/i,
    /\bdios\s+habl[oó]\s+a\b/i,
  ];
  return patterns.some((re) => re.test(text));
}

function getQuoteDelta(rawText) {
  const text = String(rawText || "");
  const openCount = (text.match(/[「『“‘«﹁〝]/g) || []).length;
  const closeCount = (text.match(/[」』”’»﹂〞]/g) || []).length;
  return openCount - closeCount;
}

function hasClosingQuote(rawText) {
  return /[」』”’»﹂〞]/.test(String(rawText || ""));
}

function hasOpeningQuote(rawText) {
  return /[「『“‘«﹁〝]/.test(String(rawText || ""));
}

function buildDivineSpeechVerseSet(rows, versionId) {
  const sorted = [...(rows || [])].sort(
    (a, b) => Number(a?.verse || 0) - Number(b?.verse || 0)
  );
  const result = new Set();
  let inDivineQuote = false;
  let quoteBalance = 0;
  let pendingTailCarry = 0;

  for (const row of sorted) {
    const verseNo = Number(row?.verse || 0);
    const text = String(row?.texts?.[versionId] || "").trim();
    if (!verseNo || !text) continue;

    const trigger = isDivineSpeechVerseText(text);
    if (trigger || inDivineQuote || pendingTailCarry > 0) {
      result.add(verseNo);
    }

    const delta = getQuoteDelta(text);
    if (trigger && delta > 0) {
      inDivineQuote = true;
      quoteBalance = delta;
      pendingTailCarry = 0;
      continue;
    }
    if (trigger && !hasClosingQuote(text)) {
      inDivineQuote = true;
      quoteBalance = hasOpeningQuote(text) ? Math.max(1, delta) : 1;
      pendingTailCarry = 0;
      continue;
    }
    if (inDivineQuote) {
      quoteBalance += delta;
      if (quoteBalance <= 0 && hasClosingQuote(text)) {
        inDivineQuote = false;
        quoteBalance = 0;
        pendingTailCarry = 0;
      }
      continue;
    }
    if (trigger && hasClosingQuote(text)) {
      pendingTailCarry = 0;
      continue;
    }
    if (pendingTailCarry > 0) {
      pendingTailCarry -= 1;
      continue;
    }
    if (trigger) {
      pendingTailCarry = 1;
    }
  }

  return result;
}

function buildFavoriteVerseUnit(
  row,
  versionId,
  text,
  highlightWords,
  isDivineSpeechOverride = null
) {
  const verseNo = Number(row?.verse || 0);
  const rawText = String(text || "").trim();
  if (!verseNo || !rawText) return "";
  const key = makeFavoriteKey(versionId, verseNo);
  const active = state.favoriteKeys.has(key) ? " is-favorited" : "";
  const isDivineSpeech =
    typeof isDivineSpeechOverride === "boolean"
      ? isDivineSpeechOverride
      : isDivineSpeechVerseText(rawText);
  const divineClass = isDivineSpeech
    ? " verse-unit-divine-speech"
    : "";
  const renderedText = highlightText(rawText, highlightWords);
  return `<span class="verse-unit verse-unit-favorite${active}${divineClass}" data-favorite-key="${escapeHtml(
    key
  )}" data-favorite-version-id="${escapeHtml(versionId)}" data-favorite-verse="${escapeHtml(
    String(verseNo)
  )}" data-favorite-text="${escapeHtml(rawText)}"><span class="verse-no">${verseNo}</span>${renderedText}</span>`;
}

function focusQuestionFavoriteInDom(key) {
  const safeSelectorKey =
    typeof CSS !== "undefined" && typeof CSS.escape === "function"
      ? CSS.escape(key)
      : String(key).replace(/"/g, '\\"');
  const tryFocus = (retry = 0) => {
    const el = document.querySelector(`[data-question-fav-key="${safeSelectorKey}"]`);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "center" });
      el.classList.add("favorite-flash");
      window.setTimeout(() => el.classList.remove("favorite-flash"), 900);
      return;
    }
    if (retry < 14) {
      window.setTimeout(() => tryFocus(retry + 1), 120);
    }
  };
  tryFocus();
}

function setFavoritesPanelTab(tab) {
  const allowed = new Set(["pages", "verses", "questions"]);
  if (!allowed.has(tab)) tab = "pages";
  favoritesPanelActiveTab = tab;

  document.querySelectorAll("#favoritesPanel [data-favorites-tab]").forEach((btn) => {
    const t = btn.getAttribute("data-favorites-tab");
    const on = t === tab;
    btn.classList.toggle("active", on);
    btn.setAttribute("aria-selected", on ? "true" : "false");
    btn.tabIndex = on ? 0 : -1;
  });

  const pages = document.getElementById("favoritesPagesList");
  const verses = document.getElementById("favoritesVersesList");
  const questions = document.getElementById("favoritesQuestionsList");
  if (pages) pages.hidden = tab !== "pages";
  if (verses) verses.hidden = tab !== "verses";
  if (questions) questions.hidden = tab !== "questions";
}

function initFavoritesPanelTabs() {
  document.querySelectorAll("#favoritesPanel [data-favorites-tab]").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const t = btn.getAttribute("data-favorites-tab");
      if (t) setFavoritesPanelTab(t);
    });
  });
}

function truncateChapterFavoriteSegmentTitle(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  return s.length > 100 ? `${s.slice(0, 97)}…` : s;
}

async function enrichChapterFavoriteSegmentTitles() {
  const list = state.chapterFavorites || [];
  const targets = [];
  for (const item of list) {
    if (String(item.firstSegmentTitle || "").trim()) continue;
    if (chapterFavoriteSegmentEnrichLocks.has(item.key)) continue;
    chapterFavoriteSegmentEnrichLocks.add(item.key);
    targets.push(item);
  }
  if (!targets.length) return;

  let changed = false;
  await Promise.all(
    targets.map(async (item) => {
      try {
        const version = String(
          item.contentVersion || state.frontState.contentVersion || "default"
        );
        const lang = String(
          item.contentLang || state.frontState.contentLang || "zh"
        );
        const params = new URLSearchParams({
          version,
          lang,
          bookId: String(item.bookId),
          chapter: String(item.chapter),
        });
        const res = await fetch(`/api/study-content?${params.toString()}`, {
          cache: "no-store",
        });
        const data = await res.json();
        if (!res.ok || !data || data.missing === true) return;
        const firstSegmentTitle = truncateChapterFavoriteSegmentTitle(
          data.segments?.[0]?.title
        );
        if (!firstSegmentTitle) return;
        const idx = (state.chapterFavorites || []).findIndex(
          (x) => x.key === item.key
        );
        if (idx < 0) return;
        const cur = state.chapterFavorites[idx];
        if (String(cur.firstSegmentTitle || "").trim()) return;
        state.chapterFavorites[idx] = { ...cur, firstSegmentTitle };
        changed = true;
      } catch {
        /* 忽略网络错误，下次打开收藏可再试 */
      } finally {
        chapterFavoriteSegmentEnrichLocks.delete(item.key);
      }
    })
  );

  if (changed) {
    saveChapterFavorites();
    renderFavoritesPanel();
  }
}

function renderFavoritesPanel() {
  const pagesList = document.getElementById("favoritesPagesList");
  const versesList = document.getElementById("favoritesVersesList");
  const questionsList = document.getElementById("favoritesQuestionsList");
  const panel = document.getElementById("favoritesPanel");
  if (!pagesList || !versesList || !questionsList || !panel) return;
  const copy = getLocalizedCopy();

  const chapterSorted = [...(state.chapterFavorites || [])].sort(
    (a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0)
  );
  const verseSorted = [...(state.favorites || [])].sort(
    (a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0)
  );
  const questionSorted = [...(state.questionFavorites || [])].sort(
    (a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0)
  );

  const pageBody =
    chapterSorted.length === 0
      ? `<div class="favorites-section-empty">${escapeHtml(
          copy.favoritesSectionEmptyPage || ""
        )}</div>`
      : chapterSorted
          .map((item) => {
            const titleLine = formatBookChapterLabel(
              getLocalizedBookLabelById(item.bookId),
              item.chapter
            );
            const segPart = String(item.firstSegmentTitle || "").trim();
            const refLine = segPart ? `${titleLine} · ${segPart}` : titleLine;
            return `<div class="favorite-item favorite-item--chapter favorite-item--page-row">
          <button type="button" class="favorite-jump-btn" data-favorite-jump="${escapeHtml(
            item.key
          )}">
            <span class="favorite-ref">${escapeHtml(refLine)}</span>
          </button>
          <button type="button" class="favorite-remove-btn" data-favorite-remove="${escapeHtml(
            item.key
          )}" aria-label="${escapeHtml(copy.removeFavoriteAria || "移除")}">×</button>
        </div>`;
          })
          .join("");

  const verseBody =
    verseSorted.length === 0
      ? `<div class="favorites-section-empty">${escapeHtml(
          copy.favoritesSectionEmptyVerse || ""
        )}</div>`
      : verseSorted
          .map((item) => {
            const title = `${getLocalizedBookLabelById(item.bookId)} ${item.chapter}:${item.verse}`;
            return `<div class="favorite-item favorite-item--verse">
          <button type="button" class="favorite-jump-btn" data-favorite-jump="${escapeHtml(
            item.key
          )}">
            <div class="favorite-text">${escapeHtml(item.text)}</div>
            <div class="favorite-ref">${escapeHtml(title)}</div>
          </button>
          <button type="button" class="favorite-remove-btn" data-favorite-remove="${escapeHtml(
            item.key
          )}" aria-label="${escapeHtml(copy.removeFavoriteAria || "移除")}">×</button>
        </div>`;
          })
          .join("");

  const questionBody =
    questionSorted.length === 0
      ? `<div class="favorites-section-empty">${escapeHtml(
          copy.favoritesSectionEmptyQuestion || ""
        )}</div>`
      : questionSorted
          .map((item) => {
            const bookLine = formatBookChapterLabel(
              getLocalizedBookLabelById(item.bookId),
              item.chapter
            );
            const segTitle = String(item.title || "").trim();
            const refLine = segTitle ? `${bookLine} · ${segTitle}` : bookLine;
            const cvLabel =
              getLocalizedContentVersionLabel(item.contentVersion) ||
              item.contentVersion ||
              "";
            const refLineWithVersion = cvLabel
              ? `${refLine}（${cvLabel}）`
              : refLine;
            return `<div class="favorite-item favorite-item--question">
          <button type="button" class="favorite-jump-btn" data-question-fav-jump="${escapeHtml(
            item.key
          )}">
            <div class="favorite-text">${escapeHtml(item.question)}</div>
            <div class="favorite-ref">${escapeHtml(refLineWithVersion)}</div>
          </button>
          <button type="button" class="favorite-remove-btn" data-question-favorite-remove="${escapeHtml(
            item.key
          )}" aria-label="${escapeHtml(copy.removeFavoriteAria || "移除")}">×</button>
        </div>`;
          })
          .join("");

  pagesList.innerHTML = pageBody;
  versesList.innerHTML = verseBody;
  questionsList.innerHTML = questionBody;

  setFavoritesPanelTab(favoritesPanelActiveTab);

  panel.querySelectorAll("[data-favorite-jump]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.getAttribute("data-favorite-jump");
      const chItem = (state.chapterFavorites || []).find((x) => x.key === key);
      if (chItem) {
        const book = getBookMetaById(chItem.bookId);
        if (!book) return;
        state.frontState.testament = book.testamentName;
        state.frontState.bookId = chItem.bookId;
        state.frontState.chapter = chItem.chapter;
        state.frontState.primaryScriptureVersionId = chItem.versionId;
        if (chItem.contentVersion) {
          state.frontState.contentVersion = chItem.contentVersion;
        }
        if (chItem.contentLang) {
          state.frontState.contentLang = chItem.contentLang;
        }
        state.frontState.secondaryScriptureVersionIds = (
          state.frontState.secondaryScriptureVersionIds || []
        ).filter((id) => id !== chItem.versionId);
        syncContentLangWithPrimaryVersion();
        saveFrontState();
        renderAllSelectors();
        closeToolbarPanel("favoritesPanel");
        await refreshCurrentPage();
        window.scrollTo({ top: 0, behavior: "smooth" });
        return;
      }
      const item = (state.favorites || []).find((x) => x.key === key);
      if (!item) return;
      const book = getBookMetaById(item.bookId);
      if (!book) return;
      state.frontState.testament = book.testamentName;
      state.frontState.bookId = item.bookId;
      state.frontState.chapter = item.chapter;
      state.frontState.primaryScriptureVersionId = item.versionId;
      state.frontState.secondaryScriptureVersionIds = (
        state.frontState.secondaryScriptureVersionIds || []
      ).filter((id) => id !== item.versionId);
      syncContentLangWithPrimaryVersion();
      saveFrontState();
      renderAllSelectors();
      closeToolbarPanel("favoritesPanel");
      await refreshCurrentPage();
      const target = document.querySelector(`[data-favorite-key="${CSS.escape(item.key)}"]`);
      if (target) {
        target.scrollIntoView({ behavior: "smooth", block: "center" });
        target.classList.add("favorite-flash");
        window.setTimeout(() => target.classList.remove("favorite-flash"), 900);
      }
    });
  });

  panel.querySelectorAll("[data-question-fav-jump]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.getAttribute("data-question-fav-jump");
      const qItem = (state.questionFavorites || []).find((x) => x.key === key);
      if (!qItem) return;
      const book = getBookMetaById(qItem.bookId);
      if (!book) return;
      state.frontState.testament = book.testamentName;
      state.frontState.bookId = qItem.bookId;
      state.frontState.chapter = qItem.chapter;
      if (qItem.contentVersion) {
        state.frontState.contentVersion = qItem.contentVersion;
      }
      if (qItem.contentLang) {
        state.frontState.contentLang = qItem.contentLang;
      }
      state.frontState.showScripture = true;
      state.frontState.showQuestions = true;
      saveFrontState();
      renderAllSelectors();
      closeToolbarPanel("favoritesPanel");
      await refreshCurrentPage();
      focusQuestionFavoriteInDom(key);
    });
  });

  panel.querySelectorAll("[data-favorite-remove]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const key = btn.getAttribute("data-favorite-remove");
      if (!key) return;
      if ((state.chapterFavorites || []).some((x) => x.key === key)) {
        state.chapterFavorites = (state.chapterFavorites || []).filter((x) => x.key !== key);
        saveChapterFavorites();
      } else {
        state.favorites = (state.favorites || []).filter((x) => x.key !== key);
        saveFavorites();
      }
      renderToolbarTriggers();
      renderFavoritesPanel();
      renderStudyContent();
    });
  });

  panel.querySelectorAll("[data-question-favorite-remove]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const key = btn.getAttribute("data-question-favorite-remove");
      if (!key) return;
      state.questionFavorites = (state.questionFavorites || []).filter(
        (x) => x.key !== key
      );
      saveQuestionFavorites();
      renderToolbarTriggers();
      renderFavoritesPanel();
      renderStudyContent();
    });
  });

  void enrichChapterFavoriteSegmentTitles();
}

function shouldConvertQuestionsToTraditional() {
  return state.frontState.primaryScriptureVersionId === "cuv_zh_tw";
}

function transformQuestionDisplayText(text) {
  const raw = String(text || "");
  if (!raw) return "";
  if (!shouldConvertQuestionsToTraditional()) return raw;
  return zhHansToHant(raw);
}

function syncContentLangWithPrimaryVersion() {
  const primary = getPrimaryScriptureVersion();
  const nextLang = primary?.lang;
  if (!nextLang) return;

  const exists = (state.bootstrap?.uiLanguages || []).find(
    (x) => x.id === nextLang
  );
  if (exists) {
    state.frontState.contentLang = nextLang;
  }
}

async function init() {
  try {
    syncFavoriteKeySet();
    syncChapterFavoriteKeySet();
    syncQuestionFavoriteKeySet();
    backfillGlobalFavoritesFromLocal();
    applyFontScale();
    initFontTools();
    initExportButtons();
    initFavorites();
    await loadBootstrap();
    ensureScriptureCompareUI();
    initSelectors();
    initToolbarPanels();
    initFavoritesPanelTabs();
    initVerseSearchOverlay();
    initChapterNav();
    initInlineDisplayToggles();
    initChapterQuestionCollector();
    initApprovedQuestionReply();
    initPresetQuestionActions();
    initAuthModal();
    initChapterRibbonFavorite();
    initMemberHub();
    initOnlinePulseVisibility();
    initAdminModal();
    tryConsumeAdminDeepLink();
    bindViewportScrollPersistence();
    renderAllSelectors();
    await refreshCurrentPage();
    focusPendingFavoriteIfAny();
    focusPendingQuestionIfAny();
    restoreViewportScroll();
  } catch (error) {
    console.error("初始化失败:", error);
  }
}

function initPresetQuestionActions() {
  if (document.body.dataset.presetQaBound === "1") return;
  document.body.dataset.presetQaBound = "1";
  document.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const correctBtn = target.closest(
      '.preset-qa-action-btn[data-action="correct"]'
    );
    if (correctBtn) {
      if (!state.currentUser) {
        if (typeof window.openAuthModal === "function") window.openAuthModal("login");
        return;
      }
      openQuestionCorrectionDialog({
        targetType: "preset",
        bookId: String(correctBtn.dataset.bookId || ""),
        chapter: Number(correctBtn.dataset.chapter || 0),
        contentVersion: String(correctBtn.dataset.contentVersion || ""),
        contentLang: String(correctBtn.dataset.contentLang || ""),
        rangeStart: Number(correctBtn.dataset.rangeStart || 0),
        rangeEnd: Number(correctBtn.dataset.rangeEnd || 0),
        segmentTitle: String(correctBtn.dataset.segmentTitle || ""),
        questionIndex: Number(correctBtn.dataset.questionIndex || 0),
        originalText: String(correctBtn.dataset.originalText || ""),
      });
      return;
    }
    const btn = target.closest(".preset-qa-action-btn");
    if (!btn) return;
    const action = String(btn.dataset.action || "");
    const questionId = String(btn.dataset.questionId || "").trim();
    if (!questionId || (action !== "like" && action !== "reply")) return;
    const iconEl = btn.querySelector(".preset-qa-action-icon");
    if (iconEl instanceof HTMLElement) {
      iconEl.classList.remove("is-pop");
      void iconEl.offsetWidth;
      iconEl.classList.add("is-pop");
    }
    if (action === "reply") {
      const input = document.getElementById("chapterQuestionInput");
      if (input instanceof HTMLTextAreaElement) {
        input.focus();
        if (!String(input.value || "").trim()) {
          const qText = String(btn.dataset.questionText || "").trim();
          input.value = qText ? `回复：${qText} ` : "";
        }
      }
      return;
    }
    const interaction = getQaInteractionById(questionId);
    const nextActive = !interaction.liked;
    setQaInteractionById(questionId, {
      liked: nextActive,
      saved: interaction.saved,
    });
    btn.classList.toggle("is-active", nextActive);
    const countEl = btn.querySelector(".preset-qa-action-count");
    if (countEl instanceof HTMLElement) {
      const base = Math.max(0, Number(btn.dataset.baseCount || 0));
      countEl.textContent = String(base + (nextActive ? 1 : 0));
    }
  });
}

/** 会员菜单「站点管理」与千夫长专属能力：仅 adminRole 为千夫长 */
function userHasQianfuzhangMemberHubAccess(u) {
  if (!u || typeof u !== "object") return false;
  return String(u.adminRole || "").toLowerCase() === "qianfuzhang";
}

function isQianfuzhangAdmin() {
  return userHasQianfuzhangMemberHubAccess(state.currentUser);
}

function openQianfuzhangQuestionInlineEditor(itemEl, qtextEl) {
  const questionId = String(itemEl.dataset.questionId || "").trim();
  if (!questionId) return;
  const originalText = qtextEl.textContent || "";
  const wrap = document.createElement("span");
  wrap.className = "chapter-approved-qtext-edit-wrap";
  const ta = document.createElement("textarea");
  ta.className = "custom-textarea chapter-approved-qtext-edit";
  ta.rows = Math.min(12, Math.max(3, originalText.split("\n").length + 2));
  ta.value = originalText;
  const actions = document.createElement("div");
  actions.className = "chapter-approved-qtext-edit-actions";
  const saveBtn = document.createElement("button");
  saveBtn.type = "button";
  saveBtn.className = "primary-btn chapter-qtext-save";
  saveBtn.textContent = "保存";
  const cancelBtn = document.createElement("button");
  cancelBtn.type = "button";
  cancelBtn.className = "secondary-btn chapter-qtext-cancel";
  cancelBtn.textContent = "取消";
  const statusEl = document.createElement("span");
  statusEl.className = "chapter-qtext-status error-text";
  actions.append(saveBtn, cancelBtn, statusEl);
  wrap.append(ta, actions);
  qtextEl.replaceWith(wrap);
  ta.focus();
  const len = ta.value.length;
  ta.setSelectionRange(len, len);

  function restoreSpan(text) {
    const span = document.createElement("span");
    span.className = "chapter-approved-qtext";
    span.title = "点击修改问题正文（千夫长）";
    span.textContent = text;
    wrap.replaceWith(span);
  }

  ta.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape") {
      ev.preventDefault();
      restoreSpan(originalText);
    }
  });

  cancelBtn.addEventListener("click", () => {
    restoreSpan(originalText);
  });

  saveBtn.addEventListener("click", async () => {
    const next = String(ta.value || "").trim();
    if (next.length < 4) {
      statusEl.textContent = "至少 4 个字";
      return;
    }
    saveBtn.setAttribute("disabled", "disabled");
    cancelBtn.setAttribute("disabled", "disabled");
    statusEl.textContent = "保存中...";
    try {
      const res = await fetch("/api/admin/questions/update-text", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getAuthToken()}`,
        },
        body: JSON.stringify({ questionId, questionText: next }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "保存失败");
      await loadApprovedChapterQuestions();
      renderStudyContent();
    } catch (err) {
      statusEl.textContent = err?.message || "保存失败";
      saveBtn.removeAttribute("disabled");
      cancelBtn.removeAttribute("disabled");
    }
  });
}

function initApprovedQuestionReply() {
  const approvedEl = document.getElementById("chapterApprovedQuestions");
  if (!approvedEl || approvedEl.dataset.replyBound === "1") return;
  approvedEl.dataset.replyBound = "1";
  approvedEl.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;

    const qtextEl = target.closest(".chapter-approved-qtext");
    if (qtextEl && isQianfuzhangAdmin()) {
      const itemRoot = qtextEl.closest(".chapter-approved-item");
      if (
        itemRoot &&
        itemRoot.getAttribute("data-qian-edit") === "1" &&
        !target.closest(".chapter-approved-qtext-edit-wrap")
      ) {
        openQianfuzhangQuestionInlineEditor(itemRoot, qtextEl);
        return;
      }
    }

    const actionBtn = target.closest(".qa-action-btn");
    if (actionBtn) {
      const questionId = String(actionBtn.dataset.questionId || "").trim();
      const action = String(actionBtn.dataset.action || "");
      if (action === "correct") {
        if (!questionId) return;
        if (!state.currentUser) {
          if (typeof window.openAuthModal === "function") window.openAuthModal("login");
          return;
        }
        openQuestionCorrectionDialog({
          targetType: "approved",
          questionId,
          originalText: String(actionBtn.dataset.questionText || ""),
        });
        return;
      }
      if (!questionId || (action !== "like" && action !== "reply")) return;
      const safeQuestionIdSelector =
        typeof CSS !== "undefined" && typeof CSS.escape === "function"
          ? CSS.escape(questionId)
          : questionId.replace(/"/g, '\\"');
      const wrap = approvedEl.querySelector(
        `.chapter-approved-item[data-question-id="${safeQuestionIdSelector}"]`
      );
      if (!(wrap instanceof HTMLElement)) return;
      const iconEl = actionBtn.querySelector(".qa-action-icon");
      if (iconEl instanceof HTMLElement) {
        iconEl.classList.remove("is-pop");
        void iconEl.offsetWidth;
        iconEl.classList.add("is-pop");
      }
      if (action === "reply") {
        const editor = wrap.querySelector(".chapter-reply-editor");
        if (editor instanceof HTMLElement) editor.classList.add("is-open");
        const input = wrap.querySelector(".chapter-reply-input");
        if (input instanceof HTMLTextAreaElement) input.focus();
        return;
      }
      const interaction = getQaInteractionById(questionId);
      const key = "liked";
      const nextActive = !interaction[key];
      const next = setQaInteractionById(questionId, {
        liked: nextActive,
        saved: interaction.saved,
      });
      actionBtn.classList.toggle("is-active", nextActive);
      const countEl = actionBtn.querySelector(".qa-action-count");
      if (countEl instanceof HTMLElement) {
        const base = Number(actionBtn.dataset.baseCount || 0);
        countEl.textContent = String(base + (nextActive ? 1 : 0));
      }
      wrap.setAttribute("data-liked", next.liked ? "1" : "0");
      wrap.setAttribute("data-saved", next.saved ? "1" : "0");
      return;
    }

    const btn = target.closest(".chapter-reply-submit-btn");
    if (!btn) return;
    const questionId = String(btn.dataset.questionId || "").trim();
    if (!questionId) return;
    const safeQuestionIdSelector =
      typeof CSS !== "undefined" && typeof CSS.escape === "function"
        ? CSS.escape(questionId)
        : questionId.replace(/"/g, '\\"');
    const wrap = approvedEl.querySelector(
      `.chapter-approved-item[data-question-id="${safeQuestionIdSelector}"]`
    );
    if (!wrap) return;
    const input = wrap.querySelector(".chapter-reply-input");
    const status = wrap.querySelector(".chapter-reply-status");
    if (!(input instanceof HTMLTextAreaElement) || !(status instanceof HTMLElement)) return;
    const replyText = String(input.value || "").trim();
    if (replyText.length < 2) {
      status.textContent = "回复至少 2 个字";
      return;
    }
    if (!state.currentUser) {
      status.textContent = "请先登录后回复";
      return;
    }
    btn.setAttribute("disabled", "disabled");
    status.textContent = "回复中...";
    try {
      const res = await fetch("/api/questions/reply", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getAuthToken()}`,
        },
        body: JSON.stringify({ questionId, replyText }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "回复失败");
      input.value = "";
      status.textContent = "回复成功";
      await loadApprovedChapterQuestions();
      renderStudyContent();
    } catch (error) {
      status.textContent = error?.message || "回复失败";
    } finally {
      btn.removeAttribute("disabled");
    }
  });
}

function getAuthToken() {
  return String(localStorage.getItem(USER_AUTH_TOKEN_KEY) || "");
}

function setAuthToken(token) {
  if (token) localStorage.setItem(USER_AUTH_TOKEN_KEY, token);
  else localStorage.removeItem(USER_AUTH_TOKEN_KEY);
}

let onlinePulseTimerId = null;

let memberHubCloseFn = () => {};

async function performLogout() {
  stopOnlinePulseTimer();
  const token = getAuthToken();
  try {
    await fetch("/api/auth/logout", {
      method: "POST",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  } catch {}
  setAuthToken("");
  state.currentUser = null;
  memberHubCloseFn();
  renderAuthStatus();
  void refreshAppliedColorTheme();
}

function formatTotalOnlineSeconds(sec) {
  const n = Math.max(0, Math.floor(Number(sec) || 0));
  if (n < 60) return `${n} 秒`;
  const m = Math.floor(n / 60);
  if (m < 60) return `${m} 分钟`;
  const h = Math.floor(m / 60);
  const m2 = m % 60;
  return m2 > 0 ? `${h} 小时 ${m2} 分` : `${h} 小时`;
}

/** 仅数字与冒号，用于丝带旁展示（无「小时」等文案） */
function formatTotalOnlineSecondsDigits(sec) {
  const n = Math.max(0, Math.floor(Number(sec) || 0));
  const s = n % 60;
  const mTotal = Math.floor(n / 60);
  const m = mTotal % 60;
  const h = Math.floor(mTotal / 60);
  const p2 = (x) => String(x).padStart(2, "0");
  if (h > 0) return `${h}:${p2(m)}:${p2(s)}`;
  if (mTotal > 0) return `${m}:${p2(s)}`;
  return `${s}`;
}

function stopOnlinePulseTimer() {
  if (onlinePulseTimerId != null) {
    window.clearInterval(onlinePulseTimerId);
    onlinePulseTimerId = null;
  }
}

async function sendOnlinePulseOnce() {
  if (!state.currentUser || !getAuthToken()) return;
  if (typeof document !== "undefined" && document.visibilityState !== "visible") return;
  try {
    const res = await fetch("/api/user/online/pulse", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${getAuthToken()}`,
      },
      body: JSON.stringify({ seconds: 45 }),
    });
    const data = await res.json().catch(() => ({}));
    if (res.ok && state.currentUser) {
      const next = Number(data.totalOnlineSeconds);
      if (Number.isFinite(next) && next >= 0) {
        state.currentUser.totalOnlineSeconds = Math.floor(next);
        renderMemberHub();
        renderChapterRibbonTag();
      }
    }
  } catch {
    /* 忽略网络错误 */
  }
}

function startOnlinePulseTimer() {
  stopOnlinePulseTimer();
  if (!state.currentUser || !getAuthToken()) return;
  void sendOnlinePulseOnce();
  onlinePulseTimerId = window.setInterval(() => void sendOnlinePulseOnce(), 45000);
}

function initOnlinePulseVisibility() {
  if (typeof document === "undefined") return;
  if (document.body?.dataset.onlinePulseVisBound === "1") return;
  if (document.body) document.body.dataset.onlinePulseVisBound = "1";
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") void sendOnlinePulseOnce();
  });
}

function normalizeUserTotalOnlineSeconds(user) {
  if (!user || typeof user !== "object") return;
  const raw = user.totalOnlineSeconds;
  const n =
    raw === undefined || raw === null || raw === "" ? NaN : Number(raw);
  user.totalOnlineSeconds =
    Number.isFinite(n) && n >= 0 ? Math.min(Math.floor(n), 86400 * 365 * 80) : 0;
}

function getColorThemesMetaFromBootstrap() {
  return (
    state.bootstrap?.colorThemes || {
      defaultThemeId: "classic",
      themes: [],
    }
  );
}

function getStoredColorThemeId() {
  try {
    return String(localStorage.getItem(COLOR_THEME_STORAGE_KEY) || "").trim();
  } catch {
    return "";
  }
}

function applyColorVariablesToRoot(variables) {
  if (!variables || typeof variables !== "object") return;
  const root = document.documentElement;
  for (const [k, v] of Object.entries(variables)) {
    if (!k.startsWith("--")) continue;
    if (v == null) continue;
    root.style.setProperty(k, String(v));
  }
}

async function applyColorThemeById(themeId) {
  const id = String(themeId || "").trim();
  if (!id) return;
  try {
    const res = await fetch(
      `/api/color-themes/variables?themeId=${encodeURIComponent(id)}`,
      {
        cache: "no-store",
        headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
      }
    );
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "theme");
    applyColorVariablesToRoot(data.variables);
  } catch {
    /* 保留 styles.css 默认 */
  }
}

async function refreshAppliedColorTheme() {
  const meta = getColorThemesMetaFromBootstrap();
  const def =
    String(meta.defaultThemeId || "classic").trim() || "classic";
  const u = state.currentUser;
  const accountName = String(u?.name || "").trim();
  let themeId = "";
  if (accountName) {
    themeId = String(u?.colorThemeId || "").trim();
  } else {
    themeId = getStoredColorThemeId();
  }
  if (!themeId || !(meta.themes || []).some((t) => t.id === themeId)) {
    themeId = def;
  }
  await applyColorThemeById(themeId);
}

async function fetchCurrentUser() {
  const token = getAuthToken();
  if (!token) {
    state.currentUser = null;
    renderAuthStatus();
    void refreshAppliedColorTheme();
    return;
  }
  try {
    const res = await fetch("/api/auth/me", {
      headers: { Authorization: `Bearer ${token}` },
      cache: "no-store",
    });
    const data = await res.json();
    if (!res.ok) {
      state.currentUser = null;
      setAuthToken("");
    } else {
      const user = data.user || null;
      normalizeUserTotalOnlineSeconds(user);
      state.currentUser = user;
      const nm = String(user?.name || "").trim();
      if (nm) {
        const tid = String(user?.colorThemeId || "").trim();
        try {
          if (tid) localStorage.setItem(COLOR_THEME_STORAGE_KEY, tid);
          else localStorage.removeItem(COLOR_THEME_STORAGE_KEY);
        } catch (_) {
          /* ignore */
        }
      }
    }
  } catch {
    state.currentUser = null;
  }
  renderAuthStatus();
  if (state.currentUser) startOnlinePulseTimer();
  else stopOnlinePulseTimer();
  void refreshAppliedColorTheme();
}

function displayNameFromEmail(email) {
  const e = String(email || "").trim();
  const at = e.indexOf("@");
  if (at <= 0) return "";
  let local = e.slice(0, at).trim();
  local = local.replace(/^[\s._-]+|[\s._-]+$/g, "");
  if (local.length < 2) return "";
  if (local.length > 14) local = `${local.slice(0, 13)}…`;
  return local;
}

function isGenericUserDisplayName(name) {
  const n = String(name || "").trim();
  if (!n) return true;
  const lower = n.toLowerCase();
  if (n === "昵称的圣经" || (n.includes("昵称") && n.includes("圣经"))) {
    return true;
  }
  const generics = new Set([
    "昵称",
    "用户",
    "名字",
    "name",
    "user",
    "test",
    "admin",
    "新用户",
    "用户昵称",
    "请输入用户昵称",
    "your name",
    "username",
  ]);
  if (generics.has(n) || generics.has(lower)) return true;
  if (/^用户\d+$/.test(n)) return true;
  return false;
}

/** 会员区、问候语：占位称呼时用邮箱前缀或兜底文案 */
function getSensibleDisplayName(user) {
  const raw = String(user?.name || "").trim();
  if (raw && !isGenericUserDisplayName(raw)) return raw;
  const fromEmail = displayNameFromEmail(user?.email || "");
  if (fromEmail) return fromEmail;
  return "同路人";
}

function renderAuthStatus() {
  const userNameEl = document.getElementById("authUserName");
  const openBtn = document.getElementById("openAuthBtn");
  const logoutBtn = document.getElementById("logoutAuthBtn");
  const u = state.currentUser;
  const name = String(u?.name || "");
  const authed = Boolean(name);
  const showName = authed ? getSensibleDisplayName(u) : "";
  if (userNameEl) {
    userNameEl.style.display = authed ? "" : "none";
    userNameEl.textContent = authed ? `你好，${showName}` : "";
  }
  if (openBtn) openBtn.style.display = authed ? "none" : "";
  if (logoutBtn) logoutBtn.style.display = authed ? "" : "none";
  updateChapterQuestionSubmitButtonLabel();
  renderMemberHub();
  renderChapterRibbonTag();
  renderStudyContent();
}

function userHasSiteAdminAccess(u) {
  if (!u || typeof u !== "object") return false;
  if (u.isAdmin === true) return true;
  return ["shifuzhang", "baifuzhang", "qianfuzhang"].includes(
    String(u.adminRole || "").toLowerCase()
  );
}

function renderMemberHub() {
  const guest = document.getElementById("memberHubGuest");
  const member = document.getElementById("memberHubMember");
  const av = document.getElementById("memberHubAvatar");
  const dn = document.getElementById("memberHubDisplayName");
  const em = document.getElementById("memberHubEmail");
  const onlineEl = document.getElementById("memberHubOnlineTotal");
  const hubLabel = document.getElementById("memberHubLabel");
  const hubTrigger = document.getElementById("memberHubTrigger");
  const triggerGlyph = document.getElementById("memberHubTriggerGlyph");
  const triggerStarsWrap = document.getElementById("memberHubTriggerStarsWrap");
  if (!guest || !member) return;
  const u = state.currentUser;
  const name = String(u?.name || "").trim();
  const authed = Boolean(name);
  const sensible = authed ? getSensibleDisplayName(u) : "";
  guest.hidden = authed;
  member.hidden = !authed;
  if (hubLabel) {
    if (authed) {
      /* 书签上显示「昵称的圣经」+ 等级星（与账户资料中的称呼一致） */
      hubLabel.textContent = `${name}的圣经`;
      hubLabel.classList.remove("member-hub-label--sr");
    } else {
      hubLabel.textContent = "免费注册";
      hubLabel.classList.remove("member-hub-label--sr");
    }
  }
  if (hubTrigger) {
    hubTrigger.classList.toggle("member-hub-trigger--guest-label", !authed);
    hubTrigger.classList.toggle("member-hub-trigger--member-label", authed);
    const lvNum = Number(u?.userLevel) || 0;
    const levelAria = lvNum > 0 ? `，等级 L${Math.min(12, lvNum)}` : "";
    hubTrigger.setAttribute(
      "aria-label",
      authed
        ? `${name}的圣经${levelAria}，打开账户菜单`
        : "免费注册，打开登录"
    );
    const tipLevel = lvNum > 0 ? ` · 等级 L${Math.min(12, lvNum)}` : "";
    hubTrigger.title =
      authed && name
        ? (isGenericUserDisplayName(name)
            ? `当前登录（称呼未完善，显示为 ${sensible}）`
            : `当前登录：${name}`) + tipLevel
        : "点击打开登录";
  }
  if (authed) {
    const email = String(u?.email || "").trim();
    const initial = (sensible.charAt(0) || "?").toUpperCase();
    if (av) av.textContent = initial;
    if (dn) dn.textContent = sensible;
    if (em) em.textContent = email || "—";
    if (onlineEl) {
      /* 勿用 foo != null 判断：undefined == null，缺字段时 former 会误判 */
      const ts = Math.max(0, Math.floor(Number(u?.totalOnlineSeconds) || 0));
      onlineEl.textContent = `累计在线 ${formatTotalOnlineSeconds(ts)}`;
    }
    const qianSection = document.getElementById("memberHubQianfuzhangSection");
    const showQianMenu = userHasQianfuzhangMemberHubAccess(u);
    if (qianSection) {
      qianSection.hidden = !showQianMenu;
    }
    if (triggerGlyph) triggerGlyph.hidden = true;
    if (triggerStarsWrap) {
      triggerStarsWrap.hidden = false;
      triggerStarsWrap.innerHTML = renderMemberHubBookmarkStars(u?.userLevel);
    }
  } else {
    const qianSectionGuest = document.getElementById("memberHubQianfuzhangSection");
    if (qianSectionGuest) qianSectionGuest.hidden = true;
    if (triggerGlyph) triggerGlyph.hidden = false;
    if (triggerStarsWrap) {
      triggerStarsWrap.hidden = true;
      triggerStarsWrap.innerHTML = "";
    }
  }
}

function updateChapterQuestionSubmitButtonLabel() {
  const btn = document.getElementById("submitChapterQuestionBtn");
  if (!btn) return;
  btn.textContent = state.currentUser ? "提交问题" : "登录";
}

function initAuthModal() {
  const modal = document.getElementById("authModal");
  const openBtn = document.getElementById("openAuthBtn");
  const closeBtn = document.getElementById("closeAuthBtn");
  const logoutBtn = document.getElementById("logoutAuthBtn");
  const modeLoginBtn = document.getElementById("authModeLoginBtn");
  const modeRegisterBtn = document.getElementById("authModeRegisterBtn");
  const submitBtn = document.getElementById("authSubmitBtn");
  const titleEl = document.getElementById("authModalTitle");
  const nameFieldEl = document.getElementById("authNameField");
  const nameInput = document.getElementById("authNameInput");
  const emailInput = document.getElementById("authEmailInput");
  const passwordInput = document.getElementById("authPasswordInput");
  const errEl = document.getElementById("authErrorText");
  const authForm = document.getElementById("authForm");
  let authMode = "login";

  function applyAuthMode(mode) {
    authMode = mode === "register" ? "register" : "login";
    if (titleEl) titleEl.textContent = authMode === "register" ? "用户注册" : "用户登录";
    if (submitBtn) submitBtn.textContent = authMode === "register" ? "免费注册" : "登录";
    if (nameFieldEl) nameFieldEl.style.display = authMode === "register" ? "" : "none";
    if (modeLoginBtn) modeLoginBtn.classList.toggle("active", authMode === "login");
    if (modeRegisterBtn) modeRegisterBtn.classList.toggle("active", authMode === "register");
    if (passwordInput) {
      passwordInput.setAttribute(
        "autocomplete",
        authMode === "register" ? "new-password" : "current-password"
      );
    }
    if (errEl) errEl.textContent = "";
  }

  const open = (mode = "login") => {
    applyAuthMode(mode);
    if (modal) modal.style.display = "block";
  };
  window.openAuthModal = open;
  const close = () => {
    if (modal) modal.style.display = "none";
  };

  async function submit() {
    const email = String(emailInput?.value || "").trim();
    const password = String(passwordInput?.value || "").trim();
    const name = String(nameInput?.value || "").trim();
    if (!email || !password) {
      if (errEl) errEl.textContent = "请输入邮箱和密码";
      return;
    }
    if (authMode === "register" && !name) {
      if (errEl) errEl.textContent = "注册时请输入用户昵称";
      return;
    }
    if (errEl) errEl.textContent = "提交中...";
    const endpoint =
      authMode === "register" ? "/api/auth/register" : "/api/auth/login";
    const body =
      authMode === "register" ? { name, email, password } : { email, password };
    try {
      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "操作失败");
      if (authMode === "register") {
        if (errEl) errEl.textContent = "注册成功，请登录";
        applyAuthMode("login");
        return;
      }
      setAuthToken(String(data.token || ""));
      await fetchCurrentUser();
      close();
    } catch (error) {
      if (errEl) errEl.textContent = error?.message || "操作失败";
    }
  }

  openBtn?.addEventListener("click", () => open("login"));
  closeBtn?.addEventListener("click", close);
  modeLoginBtn?.addEventListener("click", () => applyAuthMode("login"));
  modeRegisterBtn?.addEventListener("click", () => applyAuthMode("register"));
  authForm?.addEventListener("submit", (e) => {
    e.preventDefault();
    void submit();
  });
  logoutBtn?.addEventListener("click", async () => {
    await performLogout();
  });

  fetchCurrentUser();
  applyAuthMode("login");
}

function initMemberHub() {
  const root = document.getElementById("memberHub");
  const trigger = document.getElementById("memberHubTrigger");
  const panel = document.getElementById("memberHubPanel");
  if (!root || !trigger || !panel) return;
  if (root.dataset.memberHubBound === "1") return;
  root.dataset.memberHubBound = "1";

  function close() {
    panel.setAttribute("hidden", "");
    document.body.classList.remove("member-hub-open");
    trigger.setAttribute("aria-expanded", "false");
  }

  function open() {
    if (getAuthToken() && state.currentUser) {
      void fetchCurrentUser();
    }
    closeAllToolbarPanels();
    renderMemberHub();
    panel.removeAttribute("hidden");
    document.body.classList.add("member-hub-open");
    trigger.setAttribute("aria-expanded", "true");
  }

  function toggle() {
    if (panel.hidden) open();
    else close();
  }

  memberHubCloseFn = close;

  window.openMemberHub = () => {
    const name = String(state.currentUser?.name || "").trim();
    if (!name) {
      close();
      window.openAuthModal?.("login");
      return;
    }
    open();
  };

  trigger.addEventListener("click", (event) => {
    event.stopPropagation();
    const name = String(state.currentUser?.name || "").trim();
    if (!name) {
      close();
      window.openAuthModal?.("login");
      return;
    }
    toggle();
  });

  /* 捕获阶段处理 <a>，避免被其它逻辑或层叠挡住导致无法跳转 */
  panel.addEventListener(
    "click",
    (event) => {
      const t = event.target;
      if (!(t instanceof Element)) return;
      const link = t.closest("a.member-hub-link");
      if (!(link instanceof HTMLAnchorElement)) return;
      const href = link.getAttribute("href");
      if (!href || href === "#") return;
      event.preventDefault();
      event.stopPropagation();
      close();
      window.location.assign(link.href);
    },
    true
  );

  panel.addEventListener("click", (event) => {
    event.stopPropagation();
    const el = event.target;
    if (!(el instanceof HTMLElement)) return;
    const btn = el.closest("[data-member-action]");
    if (!(btn instanceof HTMLElement)) return;
    const action = String(btn.dataset.memberAction || "");
    if (action === "login") {
      close();
      window.openAuthModal?.("login");
      return;
    }
    if (action === "register") {
      close();
      window.openAuthModal?.("register");
      return;
    }
    if (action === "logout") {
      close();
      void performLogout();
      return;
    }
    if (action === "admin") {
      close();
      document.getElementById("openAdminBtn")?.dispatchEvent(
        new MouseEvent("click", { bubbles: true, cancelable: true })
      );
    }
  });

  document.addEventListener(
    "mousedown",
    (event) => {
      if (panel.hasAttribute("hidden")) return;
      if (!(event.target instanceof Node)) return;
      if (root.contains(event.target) || panel.contains(event.target)) return;
      close();
    },
    true
  );

  /** 顶栏导航等可用 `/#openMemberHub` 或 `?memberHub=1` 打开会员书签（与点「昵称的圣经」面板一致） */
  function tryConsumeMemberHubDeepLink() {
    try {
      const u = new URL(window.location.href);
      const fromHash = u.hash === "#openMemberHub";
      const fromQuery = u.searchParams.get("memberHub") === "1";
      if (!fromHash && !fromQuery) return;
      if (fromHash) {
        u.hash = "";
        history.replaceState({}, "", u.pathname + (u.search || ""));
      } else {
        u.searchParams.delete("memberHub");
        const q = u.searchParams.toString();
        history.replaceState({}, "", u.pathname + (q ? `?${q}` : ""));
      }
      queueMicrotask(() => {
        if (typeof window.openMemberHub === "function") {
          window.openMemberHub();
        }
      });
    } catch {
      /* ignore */
    }
  }
  tryConsumeMemberHubDeepLink();
  window.addEventListener("hashchange", () => {
    if (window.location.hash !== "#openMemberHub") return;
    try {
      history.replaceState(
        {},
        "",
        window.location.pathname + (window.location.search || "")
      );
    } catch {
      /* ignore */
    }
    queueMicrotask(() => {
      if (typeof window.openMemberHub === "function") {
        window.openMemberHub();
      }
    });
  });
}

function initChapterQuestionCollector() {
  const inputEl = document.getElementById("chapterQuestionInput");
  const submitBtn = document.getElementById("submitChapterQuestionBtn");
  const statusEl = document.getElementById("chapterQuestionStatus");
  if (!inputEl || !submitBtn || !statusEl) return;

  submitBtn.addEventListener("click", async () => {
    if (!state.currentUser) {
      if (typeof window.openAuthModal === "function") {
        window.openAuthModal("login");
      } else {
        document.getElementById("openAuthBtn")?.dispatchEvent(
          new MouseEvent("click", { bubbles: true, cancelable: true })
        );
      }
      return;
    }

    const questionText = String(inputEl.value || "").trim();
    if (questionText.length < 4) {
      statusEl.textContent = "请至少输入 4 个字";
      return;
    }

    const now = Date.now();
    const lastAt = Number(localStorage.getItem(LAST_QUESTION_SUBMIT_AT_KEY) || 0);
    if (now - lastAt < 10000) {
      statusEl.textContent = "提交太快，请稍后再试";
      return;
    }

    submitBtn.disabled = true;
    statusEl.textContent = "提交中...";
    try {
      const body = {
        questionText,
        note: "",
        bookId: state.frontState.bookId,
        chapter: Number(state.frontState.chapter || 0),
        rangeStart: 0,
        rangeEnd: 0,
        contentVersion: state.frontState.contentVersion,
        contentLang: state.frontState.contentLang,
      };
      const res = await fetch("/api/questions/submit", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getAuthToken()}`,
        },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "提交失败");
      localStorage.setItem(LAST_QUESTION_SUBMIT_AT_KEY, String(now));
      statusEl.textContent = "已提交，感谢你的好问题";
      inputEl.value = "";
      await loadApprovedChapterQuestions();
      renderStudyContent();
    } catch (error) {
      statusEl.textContent = error?.message || "提交失败";
    } finally {
      submitBtn.disabled = false;
    }
  });
}

function initInlineDisplayToggles() {
  const allDisplayToggleBtn = document.getElementById("allDisplayToggleBtn");
  const scriptureToggleBtn = document.getElementById("scriptureToggleBtn");
  const questionToggleBtn = document.getElementById("questionToggleBtn");
  if (!allDisplayToggleBtn || !scriptureToggleBtn || !questionToggleBtn) return;

  allDisplayToggleBtn.addEventListener("click", () => {
    state.frontState.showScripture = true;
    state.frontState.showQuestions = true;
    saveFrontState();
    renderToolbarTriggers();
    renderStudyContent();
  });

  scriptureToggleBtn.addEventListener("click", () => {
    state.frontState.showScripture = true;
    state.frontState.showQuestions = false;
    saveFrontState();
    renderToolbarTriggers();
    renderStudyContent();
  });

  questionToggleBtn.addEventListener("click", () => {
    state.frontState.showScripture = false;
    state.frontState.showQuestions = true;
    saveFrontState();
    renderToolbarTriggers();
    renderStudyContent();
  });
}

function focusPendingFavoriteIfAny() {
  const key = localStorage.getItem(PENDING_FAVORITE_FOCUS_KEY);
  if (!key) return;
  localStorage.removeItem(PENDING_FAVORITE_FOCUS_KEY);

  const safeSelectorKey =
    typeof CSS !== "undefined" && typeof CSS.escape === "function"
      ? CSS.escape(key)
      : key.replace(/"/g, '\\"');

  const tryFocus = (retry = 0) => {
    const el = document.querySelector(`[data-favorite-key="${safeSelectorKey}"]`);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "center" });
      el.classList.add("favorite-flash");
      window.setTimeout(() => el.classList.remove("favorite-flash"), 900);
      return;
    }
    if (retry < 8) {
      window.setTimeout(() => tryFocus(retry + 1), 120);
    }
  };

  tryFocus();
}

function focusPendingQuestionIfAny() {
  const key = localStorage.getItem(PENDING_QUESTION_FOCUS_KEY);
  if (!key) return;
  localStorage.removeItem(PENDING_QUESTION_FOCUS_KEY);

  const safeSelectorKey =
    typeof CSS !== "undefined" && typeof CSS.escape === "function"
      ? CSS.escape(key)
      : key.replace(/"/g, '\\"');

  const tryFocus = (retry = 0) => {
    const el = document.querySelector(`[data-question-fav-key="${safeSelectorKey}"]`);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "center" });
      el.classList.add("favorite-flash");
      window.setTimeout(() => el.classList.remove("favorite-flash"), 900);
      return;
    }
    if (retry < 8) {
      window.setTimeout(() => tryFocus(retry + 1), 120);
    }
  };

  tryFocus();
}

function bindViewportScrollPersistence() {
  window.addEventListener("scroll", () => {
    if (saveScrollTimer) window.clearTimeout(saveScrollTimer);
    saveScrollTimer = window.setTimeout(() => {
      saveViewportScrollY(window.scrollY || window.pageYOffset || 0);
    }, 150);
  });

  window.addEventListener("beforeunload", () => {
    saveViewportScrollY(window.scrollY || window.pageYOffset || 0);
  });
}

function restoreViewportScroll() {
  const y = loadViewportScrollY();
  if (y <= 0) return;
  window.scrollTo(0, y);
}

async function loadBootstrap() {
  const res = await fetch("/api/front/bootstrap", { cache: "no-store" });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || "无法读取前台配置");
  state.bootstrap = data;
  if (data.siteChrome && typeof window.__applyAskBibleSiteChrome === "function") {
    window.__applyAskBibleSiteChrome(data.siteChrome);
  }
  normalizeFrontStateByBootstrap();
  void refreshAppliedColorTheme();
}

function normalizeFrontStateByBootstrap() {
  const uiLanguages = state.bootstrap?.uiLanguages || [];
  const scriptureVersions = getEnabledScriptureVersions();
  const primaryCandidates = getPrimaryVersionCandidates();
  const contentVersions = state.bootstrap?.contentVersions || [];
  const books = state.bootstrap?.testamentOptions || [];

  if (!uiLanguages.find((x) => x.id === state.frontState.uiLang)) {
    state.frontState.uiLang = state.bootstrap?.defaultState?.uiLang || "zh";
  }

  if (!contentVersions.find((x) => x.id === state.frontState.contentVersion)) {
    state.frontState.contentVersion =
      state.bootstrap?.defaultState?.contentVersionId || "default";
  }

  if (!uiLanguages.find((x) => x.id === state.frontState.contentLang)) {
    state.frontState.contentLang =
      state.bootstrap?.defaultState?.contentLang || "zh";
  }

  const primaryExists = primaryCandidates.find(
    (x) => x.id === state.frontState.primaryScriptureVersionId
  );

  if (!primaryExists) {
    state.frontState.primaryScriptureVersionId =
      state.bootstrap?.defaultState?.primaryScriptureVersionId ||
      primaryCandidates[0]?.id ||
      "";
  }

  const validSecondary = (
    state.frontState.secondaryScriptureVersionIds || []
  ).filter(
    (id) =>
      id !== state.frontState.primaryScriptureVersionId &&
      scriptureVersions.find((x) => x.id === id)
  );

  state.frontState.secondaryScriptureVersionIds = Array.from(
    new Set(validSecondary)
  );

  const validBook = books.find((b) => b.bookId === state.frontState.bookId);
  if (!validBook) {
    state.frontState.bookId = "GEN";
  }

  const currentBook = books.find((b) => b.bookId === state.frontState.bookId);
  const maxChapters = Number(currentBook?.chapters || 1);
  if (
    !Number.isInteger(state.frontState.chapter) ||
    state.frontState.chapter < 1 ||
    state.frontState.chapter > maxChapters
  ) {
    state.frontState.chapter = 1;
  }

  syncContentLangWithPrimaryVersion();
  saveFrontState();
}

function ensureScriptureCompareUI() {
  return;
}

function bumpFontScale(delta) {
  const step = 0.05;
  const cur = Number(state.frontState.fontScale) || 1;
  const next =
    delta < 0
      ? Math.max(0.85, Number((cur - step).toFixed(2)))
      : Math.min(1.3, Number((cur + step).toFixed(2)));
  if (next === cur) return;
  state.frontState.fontScale = next;
  applyFontScale();
  saveFontScale();
}

function initFontTools() {
  document.getElementById("fontDecreaseBtn")?.addEventListener("click", () => {
    bumpFontScale(-1);
  });

  document.getElementById("fontIncreaseBtn")?.addEventListener("click", () => {
    bumpFontScale(1);
  });

  /** 顶栏导航：`/#fontSmaller` / `/#fontLarger` 与书页 − / + 一致 */
  function tryConsumeFontScaleDeepLink() {
    try {
      const u = new URL(window.location.href);
      let d = 0;
      if (u.hash === "#fontSmaller") d = -1;
      else if (u.hash === "#fontLarger") d = 1;
      else return;
      u.hash = "";
      history.replaceState({}, "", u.pathname + (u.search || ""));
      queueMicrotask(() => bumpFontScale(d));
    } catch {
      /* ignore */
    }
  }
  tryConsumeFontScaleDeepLink();
  window.addEventListener("hashchange", () => {
    const h = window.location.hash;
    let d = 0;
    if (h === "#fontSmaller") d = -1;
    else if (h === "#fontLarger") d = 1;
    else return;
    try {
      history.replaceState(
        {},
        "",
        window.location.pathname + (window.location.search || "")
      );
    } catch {
      /* ignore */
    }
    queueMicrotask(() => bumpFontScale(d));
  });
}

function initExportButtons() {
  document
    .getElementById("exportPrettyPdfBtn")
    ?.addEventListener("click", () => {
      alert("这一步先保留按钮，后面再接导出新版内容。");
    });

}

function initSelectors() {
  document.getElementById("uiLang")?.addEventListener("change", async (e) => {
    state.frontState.uiLang = e.target.value;
    saveFrontState();
    renderAllSelectors();
    renderStudyContent();
    updatePageTitle();
  });

  document
    .getElementById("contentVersion")
    ?.addEventListener("change", async (e) => {
      state.frontState.contentVersion = e.target.value;
      saveFrontState();
      renderAllSelectors();
      await loadStudyContent();
      renderStudyContent();
    });

  document
    .getElementById("contentLang")
    ?.addEventListener("change", async (e) => {
      state.frontState.contentLang = e.target.value;
      saveFrontState();
      renderAllSelectors();
      await refreshCurrentPage();
    });

  document
    .getElementById("scriptureVersion")
    ?.addEventListener("change", async (e) => {
      const nextPrimary = e.target.value;
      const secondary = (
        state.frontState.secondaryScriptureVersionIds || []
      ).filter((id) => id !== nextPrimary);

      state.frontState.primaryScriptureVersionId = nextPrimary;
      state.frontState.secondaryScriptureVersionIds = secondary;
      syncContentLangWithPrimaryVersion();
      saveFrontState();
      renderAllSelectors();
      await refreshCurrentPage();
    });

  document.getElementById("bookId")?.addEventListener("change", async (e) => {
    state.frontState.bookId = e.target.value;
    state.frontState.chapter = 1;
    saveFrontState();
    renderAllSelectors();
    await refreshCurrentPage();
  });

  document.getElementById("chapter")?.addEventListener("change", async (e) => {
    state.frontState.chapter = Number(e.target.value);
    saveFrontState();
    renderAllSelectors();
    await refreshCurrentPage();
  });
}

function initToolbarPanels() {
  const triggerMap = [
    { triggerId: "bookChapterTrigger", panelId: "bookChapterPanel" },
    { triggerId: "primaryVersionTrigger", panelId: "primaryVersionPanel" },
    { triggerId: "favoritesTrigger", panelId: "favoritesPanel" },
    { triggerId: "sideUserTag", panelId: "favoritesPanel" },
  ];

  triggerMap.forEach(({ triggerId, panelId }) => {
    const trigger = document.getElementById(triggerId);
    const panel = document.getElementById(panelId);
    if (!trigger || !panel) return;

    trigger.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      toggleToolbarPanel(panelId);
    });

    panel.addEventListener("click", (event) => {
      event.stopPropagation();
    });
  });

  document.querySelectorAll("[data-close-panel]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const panelId = btn.getAttribute("data-close-panel");
      if (!panelId) return;
      closeToolbarPanel(panelId);
    });
  });

  document.addEventListener("click", () => {
    closeAllToolbarPanels();
  });

  window.addEventListener("resize", syncVisibleToolbarSheetTops);
  document.addEventListener("keydown", toolbarSheetsEscHandler);
}

/** 书卷 / 选版本 全屏卡片：Esc 关闭（搜经文打开时不抢） */
function toolbarSheetsEscHandler(e) {
  if (e.key !== "Escape") return;
  const verse = document.getElementById("verseSearchOverlay");
  if (verse && !verse.hasAttribute("hidden")) return;
  const mh = document.getElementById("memberHubPanel");
  if (mh && !mh.hasAttribute("hidden")) {
    e.preventDefault();
    memberHubCloseFn();
    return;
  }
  const pv = document.getElementById("primaryVersionPanel");
  if (pv && !pv.hasAttribute("hidden")) {
    e.preventDefault();
    closeToolbarPanel("primaryVersionPanel");
    return;
  }
  const bc = document.getElementById("bookChapterPanel");
  if (bc && !bc.hasAttribute("hidden")) {
    e.preventDefault();
    closeToolbarPanel("bookChapterPanel");
    return;
  }
  const fav = document.getElementById("favoritesPanel");
  if (fav && !fav.hasAttribute("hidden")) {
    e.preventDefault();
    closeToolbarPanel("favoritesPanel");
  }
}

function toggleToolbarPanel(panelId) {
  const panel = document.getElementById(panelId);
  if (!panel) return;

  const willOpen = panel.hasAttribute("hidden");
  closeAllToolbarPanels();

  if (willOpen) {
    closeVerseSearchOverlay();
    panel.removeAttribute("hidden");
    markToolbarTriggerActive(panelId, true);
    if (panelId === "primaryVersionPanel") {
      document.body.classList.add("primary-version-open");
      syncToolbarSheetCardTop(panel);
      window.requestAnimationFrame(() => {
        syncToolbarSheetCardTop(panel);
      });
    }
    if (panelId === "bookChapterPanel") {
      document.body.classList.add("book-chapter-open");
      syncToolbarSheetCardTop(panel);
      window.requestAnimationFrame(() => {
        syncToolbarSheetCardTop(panel);
      });
    }
    if (panelId === "favoritesPanel") {
      document.body.classList.add("favorites-open");
      favoritesPanelActiveTab = "pages";
      renderFavoritesPanel();
      syncToolbarSheetCardTop(panel);
      window.requestAnimationFrame(() => {
        syncToolbarSheetCardTop(panel);
      });
    }
  }
}

function closeToolbarPanel(panelId) {
  if (panelId === "memberHubPanel") {
    memberHubCloseFn();
    return;
  }
  const panel = document.getElementById(panelId);
  if (!panel) return;
  if (
    panelId === "primaryVersionPanel" ||
    panelId === "bookChapterPanel" ||
    panelId === "favoritesPanel"
  ) {
    const card = panel.querySelector(".verse-search-card");
    if (card) card.style.marginTop = "";
    if (panelId === "primaryVersionPanel") {
      document.body.classList.remove("primary-version-open");
    }
    if (panelId === "bookChapterPanel") {
      document.body.classList.remove("book-chapter-open");
    }
    if (panelId === "favoritesPanel") {
      document.body.classList.remove("favorites-open");
      favoritesPanelActiveTab = "pages";
    }
  }
  panel.setAttribute("hidden", "");
  markToolbarTriggerActive(panelId, false);
}

function closeAllToolbarPanels() {
  ["bookChapterPanel", "primaryVersionPanel", "favoritesPanel"].forEach(
    (panelId) => {
      closeToolbarPanel(panelId);
    }
  );
  memberHubCloseFn();
}

function verseSearchOverlayEscHandler(e) {
  if (e.key !== "Escape") return;
  const overlay = document.getElementById("verseSearchOverlay");
  if (!overlay || overlay.hasAttribute("hidden")) return;
  e.preventDefault();
  closeVerseSearchOverlay();
}

function loadVerseSearchPrefs() {
  try {
    const raw = localStorage.getItem(VERSE_SEARCH_PREFS_KEY);
    if (!raw) return { scope: "all" };
    const p = JSON.parse(raw);
    const scope = p?.scope === "ot" || p?.scope === "nt" ? p.scope : "all";
    return { scope };
  } catch {
    return { scope: "all" };
  }
}

function saveVerseSearchPrefs(prefs) {
  try {
    localStorage.setItem(VERSE_SEARCH_PREFS_KEY, JSON.stringify(prefs));
  } catch {
    /* ignore */
  }
}

function syncVerseSearchScopeButtons() {
  const { scope } = loadVerseSearchPrefs();
  document.querySelectorAll("[data-verse-search-scope]").forEach((btn) => {
    const v = btn.getAttribute("data-verse-search-scope");
    btn.classList.toggle("active", v === scope);
  });
}

function getVerseSearchScopeFromUI() {
  const active = document.querySelector("#verseSearchOverlay [data-verse-search-scope].active");
  const v = active?.getAttribute("data-verse-search-scope") || "all";
  return v === "ot" || v === "nt" ? v : "all";
}

function scheduleVerseSearch() {
  if (verseSearchDebounceTimer) window.clearTimeout(verseSearchDebounceTimer);
  verseSearchDebounceTimer = window.setTimeout(() => {
    verseSearchDebounceTimer = null;
    void runVerseSearchQuery();
  }, 320);
}

async function runVerseSearchQuery() {
  const input = document.getElementById("verseSearchInput");
  const resultsEl = document.getElementById("verseSearchResults");
  if (!resultsEl) return;
  const q = String(input?.value || "").trim();
  const versionId = state.frontState.primaryScriptureVersionId;
  if (!versionId) {
    resultsEl.innerHTML = `<p class="verse-search-status">请先在顶栏选择版本。</p>`;
    return;
  }
  if (!q) {
    resultsEl.innerHTML = `<p class="verse-search-status">输入关键字开始搜索。</p>`;
    return;
  }
  const scope = getVerseSearchScopeFromUI();
  const seq = ++verseSearchSeq;
  resultsEl.innerHTML = `<p class="verse-search-status">搜索中…</p>`;
  try {
    const params = new URLSearchParams({ q, versionId, scope, limit: "40" });
    const res = await fetch(`/api/scripture/search?${params.toString()}`);
    const data = await parseFetchJsonResponse(res);
    if (seq !== verseSearchSeq) return;
    if (!res.ok) throw new Error(data.error || "搜索失败");
    const matches = data.matches || [];
    if (!matches.length) {
      resultsEl.innerHTML = `<p class="verse-search-status">没有匹配的经文。</p>`;
      return;
    }
    const lang = state.frontState.uiLang || "zh";
    resultsEl.innerHTML = matches
      .map((m) => {
        const ref =
          lang === "en"
            ? `${escapeHtml(m.bookLabel)} ${m.chapter}:${m.verse}`
            : `${escapeHtml(m.bookLabel)} ${m.chapter}章${m.verse}节`;
        return `<button type="button" class="verse-search-result-btn" data-verse-search-jump="1"
          data-book-id="${escapeHtml(m.bookId)}"
          data-chapter="${Number(m.chapter)}"
          data-verse="${Number(m.verse)}"
          data-version-id="${escapeHtml(versionId)}">
          <div class="verse-search-result-ref">${ref}</div>
          <div class="verse-search-result-snippet">${highlightVerseSearchSnippet(
            m.snippet || "",
            q
          )}</div>
        </button>`;
      })
      .join("");
    resultsEl.querySelectorAll("[data-verse-search-jump]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const bookId = btn.getAttribute("data-book-id");
        const chapter = Number(btn.getAttribute("data-chapter"));
        const verse = Number(btn.getAttribute("data-verse"));
        const vid = btn.getAttribute("data-version-id");
        void jumpToVerseFromSearch(vid, bookId, chapter, verse);
      });
    });
  } catch (e) {
    if (seq !== verseSearchSeq) return;
    resultsEl.innerHTML = `<p class="verse-search-status">${escapeHtml(
      e?.message || String(e)
    )}</p>`;
  }
}

async function jumpToVerseFromSearch(versionId, bookId, chapter, verse) {
  const book = getBookMetaById(bookId);
  if (!book || !versionId) return;
  closeVerseSearchOverlay();
  state.frontState.testament = book.testamentName;
  state.frontState.bookId = bookId;
  state.frontState.chapter = chapter;
  state.frontState.primaryScriptureVersionId = versionId;
  state.frontState.secondaryScriptureVersionIds = (
    state.frontState.secondaryScriptureVersionIds || []
  ).filter((id) => id !== versionId);
  syncContentLangWithPrimaryVersion();
  saveFrontState();
  renderAllSelectors();
  await refreshCurrentPage();
  const key = `${versionId}|${bookId}|${chapter}|${verse}`;
  const tryFlash = (retry = 0) => {
    const safe =
      typeof CSS !== "undefined" && typeof CSS.escape === "function"
        ? CSS.escape(key)
        : key.replace(/"/g, '\\"');
    const el = document.querySelector(`[data-favorite-key="${safe}"]`);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "center" });
      el.classList.add("favorite-flash", "verse-search-jump-highlight");
      window.setTimeout(() => {
        el.classList.remove("favorite-flash", "verse-search-jump-highlight");
      }, 1400);
      return;
    }
    if (retry < 12) {
      window.setTimeout(() => tryFlash(retry + 1), 100);
    }
  };
  tryFlash();
}

function closeVerseSearchOverlay() {
  const overlay = document.getElementById("verseSearchOverlay");
  const openBtn = document.getElementById("verseSearchOpenBtn");
  const card = overlay?.querySelector(".verse-search-card");
  if (card) card.style.marginTop = "";
  if (overlay) overlay.setAttribute("hidden", "");
  document.body.classList.remove("verse-search-open");
  if (openBtn) openBtn.setAttribute("aria-expanded", "false");
  document.removeEventListener("keydown", verseSearchOverlayEscHandler);
}

/** 选书卷 / 选版本 / 搜经文 / 收藏等全屏卡片均由 CSS 在视口中垂直居中；清除遗留的 marginTop。 */
function syncToolbarSheetCardTop(overlay) {
  if (!overlay || overlay.hasAttribute("hidden")) return;
  const card = overlay.querySelector(".verse-search-card");
  if (!card) return;
  card.style.marginTop = "";
}

function syncVerseSearchCardTop() {
  syncToolbarSheetCardTop(document.getElementById("verseSearchOverlay"));
}

function syncVisibleToolbarSheetTops() {
  const verse = document.getElementById("verseSearchOverlay");
  if (verse && !verse.hasAttribute("hidden")) {
    syncToolbarSheetCardTop(verse);
  }
  const pv = document.getElementById("primaryVersionPanel");
  if (pv && !pv.hasAttribute("hidden")) {
    syncToolbarSheetCardTop(pv);
  }
  const bc = document.getElementById("bookChapterPanel");
  if (bc && !bc.hasAttribute("hidden")) {
    syncToolbarSheetCardTop(bc);
  }
  const fav = document.getElementById("favoritesPanel");
  if (fav && !fav.hasAttribute("hidden")) {
    syncToolbarSheetCardTop(fav);
  }
}

function openVerseSearchOverlay() {
  const overlay = document.getElementById("verseSearchOverlay");
  const openBtn = document.getElementById("verseSearchOpenBtn");
  const input = document.getElementById("verseSearchInput");
  const resultsEl = document.getElementById("verseSearchResults");
  if (!overlay) return;
  closeAllToolbarPanels();
  syncVerseSearchScopeButtons();
  if (resultsEl) {
    const q = String(input?.value || "").trim();
    resultsEl.innerHTML = q
      ? `<p class="verse-search-status">正在更新结果…</p>`
      : `<p class="verse-search-status">输入关键字开始搜索。</p>`;
  }
  overlay.removeAttribute("hidden");
  syncVerseSearchCardTop();
  window.requestAnimationFrame(() => {
    syncVerseSearchCardTop();
  });
  document.body.classList.add("verse-search-open");
  if (openBtn) openBtn.setAttribute("aria-expanded", "true");
  document.removeEventListener("keydown", verseSearchOverlayEscHandler);
  document.addEventListener("keydown", verseSearchOverlayEscHandler);
  window.requestAnimationFrame(() => {
    input?.focus();
    if (input && typeof input.select === "function") input.select();
    scheduleVerseSearch();
  });
}

function initVerseSearchOverlay() {
  const overlay = document.getElementById("verseSearchOverlay");
  const openBtn = document.getElementById("verseSearchOpenBtn");
  if (!overlay || !openBtn) return;

  window.openVerseSearch = () => {
    openVerseSearchOverlay();
  };

  /** 顶栏导航等可用 `/#openVerseSearch` 打开搜经文面板（与工具栏「搜经文」一致） */
  function tryConsumeVerseSearchDeepLink() {
    try {
      const u = new URL(window.location.href);
      if (u.hash !== "#openVerseSearch") return;
      u.hash = "";
      history.replaceState({}, "", u.pathname + (u.search || ""));
      queueMicrotask(() => {
        if (typeof window.openVerseSearch === "function") {
          window.openVerseSearch();
        }
      });
    } catch {
      /* ignore */
    }
  }
  tryConsumeVerseSearchDeepLink();
  window.addEventListener("hashchange", () => {
    if (window.location.hash !== "#openVerseSearch") return;
    try {
      history.replaceState(
        {},
        "",
        window.location.pathname + (window.location.search || "")
      );
    } catch {
      /* ignore */
    }
    queueMicrotask(() => {
      if (typeof window.openVerseSearch === "function") {
        window.openVerseSearch();
      }
    });
  });

  openBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    openVerseSearchOverlay();
  });

  overlay.querySelectorAll("[data-verse-search-dismiss]").forEach((el) => {
    el.addEventListener("click", () => closeVerseSearchOverlay());
  });

  overlay
    .querySelector(".verse-search-card")
    ?.addEventListener("click", (e) => e.stopPropagation());

  overlay.querySelectorAll("[data-verse-search-scope]").forEach((btn) => {
    btn.addEventListener("click", () => {
      overlay.querySelectorAll("[data-verse-search-scope]").forEach((b) => {
        b.classList.remove("active");
      });
      btn.classList.add("active");
      saveVerseSearchPrefs({ scope: getVerseSearchScopeFromUI() });
      scheduleVerseSearch();
    });
  });

  document.getElementById("verseSearchInput")?.addEventListener("input", () => {
    scheduleVerseSearch();
  });
}

function markToolbarTriggerActive(panelId, active) {
  const mapping = {
    bookChapterPanel: ["bookChapterTrigger"],
    primaryVersionPanel: ["primaryVersionTrigger"],
    favoritesPanel: ["favoritesTrigger", "sideUserTag"],
  };

  const ids = mapping[panelId] || [];
  for (const id of ids) {
    const trigger = document.getElementById(id);
    if (!trigger) continue;
    trigger.classList.toggle("active", !!active);
    if (
      (panelId === "primaryVersionPanel" ||
        panelId === "bookChapterPanel" ||
        panelId === "favoritesPanel") &&
      trigger.hasAttribute("aria-expanded")
    ) {
      trigger.setAttribute("aria-expanded", active ? "true" : "false");
    }
  }
}

function renderAllSelectors() {
  renderTestamentButtons();
  renderUiLangOptions();
  renderContentVersionOptions();
  renderContentLangOptions();
  renderPrimaryScriptureVersionOptions();
  renderSecondaryScriptureVersionChecks();
  renderBookOptions();
  renderChapterOptions();
  renderToolbarTriggers();
  renderToolbarPanels();
  applyReaderI18n();
  updatePageTitle();
}

function renderTestamentButtons() {
  return;
}

function renderUiLangOptions() {
  const el = document.getElementById("uiLang");
  if (!el) return;

  el.innerHTML = (state.bootstrap?.uiLanguages || [])
    .map(
      (item) =>
        `<option value="${escapeHtml(item.id)}">${escapeHtml(
          item.label
        )}</option>`
    )
    .join("");

  el.value = state.frontState.uiLang;
}

function renderContentVersionOptions() {
  const el = document.getElementById("contentVersion");
  if (!el) return;

  el.innerHTML = (state.bootstrap?.contentVersions || [])
    .map(
      (item) =>
        `<option value="${escapeHtml(item.id)}">${escapeHtml(
          getLocalizedContentVersionLabel(item)
        )}</option>`
    )
    .join("");

  el.value = state.frontState.contentVersion;
}

function renderContentLangOptions() {
  const el = document.getElementById("contentLang");
  if (!el) return;

  el.innerHTML = (state.bootstrap?.uiLanguages || [])
    .map(
      (item) =>
        `<option value="${escapeHtml(item.id)}">${escapeHtml(
          item.label
        )}</option>`
    )
    .join("");

  el.value = state.frontState.contentLang;
}

function renderPrimaryScriptureVersionOptions() {
  const el = document.getElementById("scriptureVersion");
  if (!el) return;

  const options = getPrimaryVersionCandidates();

  el.innerHTML = options
    .map((item) => {
      const langTag = item.lang ? ` [${item.lang}]` : "";
      return `<option value="${escapeHtml(item.id)}">${escapeHtml(
        item.label + langTag
      )}</option>`;
    })
    .join("");

  const current = state.frontState.primaryScriptureVersionId;
  if (!options.find((x) => x.id === current) && options[0]) {
    state.frontState.primaryScriptureVersionId = options[0].id;
    syncContentLangWithPrimaryVersion();
    saveFrontState();
  }

  el.value = state.frontState.primaryScriptureVersionId || options[0]?.id || "";
}

function renderSecondaryScriptureVersionChecks() {
  const container = document.getElementById("secondaryScriptureVersions");
  if (!container) return;

  const primaryId = state.frontState.primaryScriptureVersionId;
  const options = getEnabledScriptureVersions().filter(
    (x) => x.id !== primaryId
  );
  const selected = new Set(state.frontState.secondaryScriptureVersionIds || []);

  container.innerHTML = options.length
    ? options
        .map((item) => {
          const checked = selected.has(item.id) ? "checked" : "";
          return `
            <label>
              <input type="checkbox" value="${escapeHtml(
                item.id
              )}" ${checked} data-secondary-scripture />
              <span>${escapeHtml(
                item.label
              )} <span style="opacity:.65;">[${escapeHtml(
            item.lang || ""
          )}]</span></span>
            </label>
          `;
        })
        .join("")
    : `<div class="empty-state">${escapeHtml(getLocalizedCopy().noCompareVersion)}</div>`;

  container.querySelectorAll("[data-secondary-scripture]").forEach((input) => {
    input.addEventListener("change", async () => {
      const allChecked = Array.from(
        container.querySelectorAll("[data-secondary-scripture]:checked")
      ).map((x) => x.value);

      state.frontState.secondaryScriptureVersionIds = allChecked.filter(
        (id) => id !== state.frontState.primaryScriptureVersionId
      );
      saveFrontState();
      renderAllSelectors();
      await refreshCurrentPage();
    });
  });
}

function renderBookOptions() {
  const el = document.getElementById("bookId");
  if (!el) return;

  const books = getBooksForCurrentTestament();

  el.innerHTML = books
    .map((book) => {
      const label =
        state.frontState.uiLang === "en"
          ? book.bookEn || book.bookCn
          : book.bookCn || book.bookEn;
      return `<option value="${escapeHtml(book.bookId)}">${escapeHtml(
        label
      )}</option>`;
    })
    .join("");

  const stillValid = books.find((b) => b.bookId === state.frontState.bookId);
  if (!stillValid && books[0]) {
    state.frontState.bookId = books[0].bookId;
  }

  el.value = state.frontState.bookId;
}

function renderChapterOptions() {
  const el = document.getElementById("chapter");
  if (!el) return;

  const book = getCurrentBookMeta();
  const chapterCount = Number(book?.chapters || 1);

  el.innerHTML = Array.from({ length: chapterCount }, (_, i) => {
    const chapterNo = i + 1;
    return `<option value="${chapterNo}">${chapterNo}</option>`;
  }).join("");

  if (state.frontState.chapter > chapterCount) {
    state.frontState.chapter = 1;
    saveFrontState();
  }

  el.value = String(state.frontState.chapter);
}

/** 丝带已登录文案：与界面语言一致的「昵称的圣经」 */
function formatUserRibbonBibleTitle(rawName) {
  const name = String(rawName || "").trim();
  if (!name) return "";
  const esc = escapeHtml(name);
  const lang = String(state.frontState?.uiLang || "zh").toLowerCase();
  if (lang.startsWith("zh")) return `${esc}的圣经`;
  if (lang === "es") return `${esc} — Biblia`;
  return `${esc}'s Bible`;
}

function renderChapterRibbonTag() {
  const el = document.getElementById("sideUserTag");
  if (!el) return;
  const copy = getLocalizedCopy();
  const key = getCurrentChapterFavoriteKey();
  const saved = Boolean(key && state.chapterFavoriteKeys.has(key));
  el.classList.toggle("book-side-extend-tag--chapter-saved", saved);

  const u = state.currentUser;
  const accountName = String(u?.name || "").trim();
  const authed = Boolean(accountName);

  const listAria =
    copy.favoritesListOpenAria || copy.favoritesTitle || "打开收藏列表";

  el.classList.toggle("book-side-extend-tag--ribbon-user", authed);
  el.classList.toggle("book-side-extend-tag--ribbon-guest", !authed);
  el.classList.remove("book-side-extend-tag--ribbon-no-label");

  if (authed) {
    const sensible = getSensibleDisplayName(u);
    const lvNum = Number(u?.userLevel) || 0;
    const capped = lvNum > 0 ? Math.min(12, lvNum) : 0;
    const levelAria = capped > 0 ? `，等级 L${capped}` : "，尚无等级";
    el.innerHTML = `<span class="ribbon-user-stack ribbon-user-stack--bible-line">
      <span class="ribbon-bible-line-text">${formatUserRibbonBibleTitle(
        accountName
      )}</span>
      <span class="ribbon-user-stars-wrap">${renderRibbonLevelStars(
        lvNum
      )}</span>
    </span>`;
    el.setAttribute(
      "aria-label",
      `${listAria}（${sensible}${levelAria}）`
    );
  } else {
    const guestLabel = escapeHtml(copy.favoritesTitle || "收藏夹");
    el.innerHTML = `<span class="ribbon-guest-label">${guestLabel}</span>`;
    el.setAttribute("aria-label", listAria);
  }
  el.setAttribute("href", "#");

  const onlineLine = document.getElementById("sideRibbonOnlineLine");
  if (onlineLine) {
    if (authed) {
      const ts = Math.max(0, Math.floor(Number(u?.totalOnlineSeconds) || 0));
      onlineLine.textContent = formatTotalOnlineSecondsDigits(ts);
      onlineLine.setAttribute(
        "aria-label",
        `累计在线 ${formatTotalOnlineSeconds(ts)}`
      );
    } else {
      onlineLine.textContent = "0";
      onlineLine.setAttribute("aria-label", "累计在线，未登录");
    }
  }
}

function toggleCurrentChapterFavorite() {
  const key = getCurrentChapterFavoriteKey();
  if (!key) return;
  const versionId = state.frontState.primaryScriptureVersionId;
  const bookId = state.frontState.bookId;
  const chapter = Number(state.frontState.chapter || 0);
  if (!versionId || !bookId || !chapter) return;

  if (state.chapterFavoriteKeys.has(key)) {
    state.chapterFavorites = (state.chapterFavorites || []).filter(
      (x) => x.key !== key
    );
  } else {
    const seg0Title = String(
      state.studyContent?.segments?.[0]?.title || ""
    ).trim();
    const firstSegmentTitle = truncateChapterFavoriteSegmentTitle(seg0Title);
    const contentVersion = String(state.frontState.contentVersion || "");
    const contentLang = String(state.frontState.contentLang || "");
    state.chapterFavorites = [
      {
        key,
        versionId,
        bookId,
        chapter,
        contentVersion,
        contentLang,
        firstSegmentTitle,
        createdAt: Date.now(),
      },
      ...(state.chapterFavorites || []),
    ];
  }
  saveChapterFavorites();
  renderToolbarTriggers();
  renderFavoritesPanel();
}

function initChapterRibbonFavorite() {
  const sideStar = document.getElementById("sideFavoritesListBtn");
  if (sideStar) {
    sideStar.addEventListener("click", (event) => {
      event.preventDefault();
      toggleCurrentChapterFavorite();
    });
  }
}

function renderToolbarTriggers() {
  const bookChapterTriggerText = document.getElementById(
    "bookChapterTriggerText"
  );
  const qaViewTriggerText = document.getElementById("qaViewTriggerText");
  const primaryVersionTriggerText = document.getElementById(
    "primaryVersionTriggerText"
  );
  const favoritesTriggerText = document.getElementById("favoritesTriggerText");
  const allDisplayToggleBtn = document.getElementById("allDisplayToggleBtn");
  const scriptureToggleBtn = document.getElementById("scriptureToggleBtn");
  const questionToggleBtn = document.getElementById("questionToggleBtn");

  const bookLabel = getBookLabelForPrimaryScripture();
  const copy = getLocalizedCopy();

  if (bookChapterTriggerText) {
    bookChapterTriggerText.textContent = copy.triggerBook || "书卷";
  }
  const verseSearchTriggerText = document.getElementById("verseSearchTriggerText");
  if (verseSearchTriggerText) {
    verseSearchTriggerText.textContent = copy.searchScripture || "搜索";
  }

  if (qaViewTriggerText) {
    qaViewTriggerText.textContent = copy.display;
  }

  if (primaryVersionTriggerText) {
    primaryVersionTriggerText.textContent = copy.triggerTranslation || "版本+";
  }
  if (favoritesTriggerText) {
    favoritesTriggerText.textContent = copy.favorites;
  }

  const sideFavListBtn = document.getElementById("sideFavoritesListBtn");
  if (sideFavListBtn) {
    const key = getCurrentChapterFavoriteKey();
    const chSaved = Boolean(key && state.chapterFavoriteKeys.has(key));
    sideFavListBtn.classList.toggle(
      "book-side-extend-list-btn--chapter-saved",
      chSaved
    );
    sideFavListBtn.setAttribute(
      "aria-label",
      chSaved
        ? copy.ribbonChapterSavedAria || "本页已收藏，点击取消收藏"
        : copy.ribbonChapterSaveAria || "收藏当前章"
    );
    sideFavListBtn.setAttribute("aria-pressed", chSaved ? "true" : "false");
  }

  renderChapterRibbonTag();

  const showQuestions = state.frontState.showQuestions !== false;
  const showScripture = state.frontState.showScripture !== false;
  const currentValue = showQuestions && showScripture
    ? "all"
    : showScripture
    ? "scripture"
    : "question";
  if (allDisplayToggleBtn) {
    allDisplayToggleBtn.textContent = copy.all || "全部";
    allDisplayToggleBtn.classList.toggle("active", currentValue === "all");
  }
  if (scriptureToggleBtn) {
    scriptureToggleBtn.textContent = copy.scripture || "经文";
    scriptureToggleBtn.classList.toggle("active", currentValue === "scripture");
  }
  if (questionToggleBtn) {
    questionToggleBtn.textContent = copy.questions || "问题";
    questionToggleBtn.classList.toggle("active", currentValue === "question");
  }
  document.body.classList.toggle("question-only-mode", currentValue === "question");
}

function renderToolbarPanels() {
  renderBookChapterPanel();
  renderPrimaryVersionPanel();
  renderCompareVersionPanel(false);
  renderFavoritesPanel();
}

function renderQaViewPanel() {
  const list = document.getElementById("qaViewList");
  if (!list) return;

  const showQuestions = state.frontState.showQuestions !== false;
  const showScripture = state.frontState.showScripture !== false;
  const currentValue = showQuestions && showScripture
    ? "all"
    : showScripture
    ? "scripture"
    : "question";

  const copy = getLocalizedCopy();
  const options = [
    { id: "all", label: copy.all },
    { id: "scripture", label: copy.scripture },
    { id: "question", label: copy.questions },
  ];

  list.innerHTML = options
    .map((item) => {
      const active = item.id === currentValue ? "active" : "";
      return `<button type="button" class="option-item ${active}" data-qa-view="${item.id}">${item.label}</button>`;
    })
    .join("");

  list.querySelectorAll("[data-qa-view]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const value = btn.getAttribute("data-qa-view");
      if (!value) return;

      state.frontState.showScripture = value === "scripture" || value === "all";
      state.frontState.showQuestions = value === "question" || value === "all";
      saveFrontState();
      renderAllSelectors();
      renderStudyContent();
      closeToolbarPanel("qaViewPanel");
    });
  });
}

function renderBookChapterPanel() {
  renderBookChapterGrids({
    otGridId: "bookGridOt",
    ntGridId: "bookGridNt",
    chapterGridId: "chapterGrid",
    closePanelId: "bookChapterPanel",
  });
}

function renderBookChapterGrids({
  otGridId,
  ntGridId,
  chapterGridId,
  closePanelId,
}) {
  const otGrid = document.getElementById(otGridId);
  const ntGrid = document.getElementById(ntGridId);
  const chapterGrid = document.getElementById(chapterGridId);
  if (!otGrid || !ntGrid || !chapterGrid) return;

  const scriptureLang = getPrimaryScriptureLang();

  const bookLabel = (book) =>
    scriptureLang === "en" || scriptureLang === "es" || scriptureLang === "he"
      ? BOOK_NAME_EN_BY_ID[book.bookId] || book.bookEn || book.bookCn || book.bookId
      : book.bookCn || book.bookEn || book.bookId;

  const fillBookGrid = (container, testamentName) => {
    const books = getBooksForTestament(testamentName);
    container.innerHTML =
      testamentName === "旧约"
        ? buildOldTestamentBookGridHtml(books, bookLabel)
        : testamentName === "新约"
          ? buildNewTestamentBookGridHtml(books, bookLabel)
          : [...books]
              .sort((a, b) => {
                const oa = BOOK_CANONICAL_ORDER_MAP.get(a.bookId) ?? 999;
                const ob = BOOK_CANONICAL_ORDER_MAP.get(b.bookId) ?? 999;
                return oa - ob;
              })
              .map((book) => {
                const active =
                  book.bookId === state.frontState.bookId ? "active" : "";
                return `<button type="button" class="book-item ${active}" data-book-grid-id="${escapeHtml(
                  book.bookId
                )}">${escapeHtml(formatBookGridButtonLabel(book, bookLabel))}</button>`;
              })
              .join("");

    container.querySelectorAll("[data-book-grid-id]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const nextBookId = btn.getAttribute("data-book-grid-id");
        if (!nextBookId) return;

        state.frontState.testament = testamentName;
        state.frontState.bookId = nextBookId;
        state.frontState.chapter = 1;
        saveFrontState();
        renderAllSelectors();
      });
    });
  };

  fillBookGrid(otGrid, "旧约");
  fillBookGrid(ntGrid, "新约");

  const currentBook = getCurrentBookMeta();
  const chapterCount = Number(currentBook?.chapters || 1);

  chapterGrid.innerHTML = Array.from({ length: chapterCount }, (_, i) => {
    const chapterNo = i + 1;
    const active =
      chapterNo === Number(state.frontState.chapter) ? "active" : "";
    return `<button type="button" class="chapter-item ${active}" data-chapter-grid-no="${chapterNo}">${chapterNo}</button>`;
  }).join("");

  chapterGrid.querySelectorAll("[data-chapter-grid-no]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const nextChapter = Number(btn.getAttribute("data-chapter-grid-no") || 1);
      state.frontState.chapter = nextChapter;
      saveFrontState();
      renderAllSelectors();
      closeToolbarPanel(closePanelId);
      await refreshCurrentPage();
    });
  });
}

function renderPrimaryVersionPanel() {
  const cvSelect = document.getElementById("toolbarContentVersionSelect");
  if (cvSelect) {
    const cvOptions = state.bootstrap?.contentVersions || [];
    cvSelect.innerHTML = cvOptions
      .map((item) => {
        return `<option value="${escapeHtml(item.id)}">${escapeHtml(
          getLocalizedContentVersionLabel(item)
        )}</option>`;
      })
      .join("");

    const curCv = state.frontState.contentVersion;
    if (cvOptions.some((x) => x.id === curCv)) {
      cvSelect.value = curCv;
    } else if (cvOptions[0]) {
      cvSelect.value = cvOptions[0].id;
    }

    if (!cvSelect.dataset.toolbarContentVersionBound) {
      cvSelect.dataset.toolbarContentVersionBound = "1";
      cvSelect.addEventListener("change", async (e) => {
        const nextId = e.target.value;
        if (!nextId) return;

        state.frontState.contentVersion = nextId;
        saveFrontState();
        renderAllSelectors();
        closeToolbarPanel("primaryVersionPanel");
        await loadStudyContent();
        renderStudyContent();
      });
    }
  }

  const copy = getLocalizedCopy();
  const primaryTitleEl = document.getElementById("primaryVersionSectionTitle");
  const compareTitleEl = document.getElementById("compareVersionSectionTitle");
  if (primaryTitleEl) primaryTitleEl.textContent = copy.primaryVersionSingle;
  if (compareTitleEl) compareTitleEl.textContent = copy.compareVersionMulti;

  const primarySelect = document.getElementById("toolbarPrimaryVersionSelect");
  if (primarySelect) {
    const options = getPrimaryVersionCandidates();
    primarySelect.innerHTML = options
      .map((item) => {
        const langTag = item.lang ? ` [${item.lang}]` : "";
        return `<option value="${escapeHtml(item.id)}">${escapeHtml(
          item.label + langTag
        )}</option>`;
      })
      .join("");

    const cur = state.frontState.primaryScriptureVersionId;
    if (options.some((x) => x.id === cur)) {
      primarySelect.value = cur;
    } else if (options[0]) {
      primarySelect.value = options[0].id;
    }

    if (!primarySelect.dataset.toolbarPrimaryBound) {
      primarySelect.dataset.toolbarPrimaryBound = "1";
      primarySelect.addEventListener("change", async (e) => {
        const nextId = e.target.value;
        if (!nextId) return;

        state.frontState.primaryScriptureVersionId = nextId;
        state.frontState.secondaryScriptureVersionIds = (
          state.frontState.secondaryScriptureVersionIds || []
        ).filter((id) => id !== nextId);

        syncContentLangWithPrimaryVersion();
        saveFrontState();
        renderAllSelectors();
        await refreshCurrentPage();
      });
    }
  }
}

function renderCompareVersionPanel(closePanelOnToggle = true) {
  const list = document.getElementById("compareVersionList");
  if (!list) return;

  const primaryId = state.frontState.primaryScriptureVersionId;
  const selected = new Set(state.frontState.secondaryScriptureVersionIds || []);
  const options = getEnabledScriptureVersions().filter(
    (item) => item.id !== primaryId
  );

  list.innerHTML = options.length
    ? options
        .map((item) => {
          const checked = selected.has(item.id);
          const langTag = item.lang ? ` [${item.lang}]` : "";
          const active = checked ? "active" : "";
          const checkText = checked ? "✓" : "";
          return `<button type="button" class="version-item version-item-checkable ${active}" data-compare-version-id="${escapeHtml(
            item.id
          )}">
              <span>${escapeHtml(item.label + langTag)}</span>
              <span class="version-item-check">${escapeHtml(checkText)}</span>
            </button>`;
        })
        .join("")
    : `<div class="empty-state">${escapeHtml(getLocalizedCopy().noCompareVersion)}</div>`;

  list.querySelectorAll("[data-compare-version-id]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const versionId = btn.getAttribute("data-compare-version-id");
      if (!versionId) return;

      const next = new Set(state.frontState.secondaryScriptureVersionIds || []);
      if (next.has(versionId)) {
        next.delete(versionId);
      } else {
        next.add(versionId);
      }

      state.frontState.secondaryScriptureVersionIds = Array.from(next).filter(
        (id) => id !== state.frontState.primaryScriptureVersionId
      );

      saveFrontState();
      renderAllSelectors();
      if (closePanelOnToggle) {
        closeToolbarPanel("primaryVersionPanel");
      }
      await refreshCurrentPage();
    });
  });
}

async function refreshCurrentPage() {
  await loadScripture();
  await loadStudyContent();
  await loadApprovedChapterQuestions();
  renderStudyContent();
  updateChapterNavUI();
  updatePageTitle();
  renderToolbarTriggers();
}

async function loadApprovedChapterQuestions() {
  const params = new URLSearchParams({
    bookId: state.frontState.bookId,
    chapter: String(state.frontState.chapter),
  });
  const res = await fetch(`/api/questions/approved?${params.toString()}`, {
    cache: "no-store",
  });
  const data = await res.json();
  if (!res.ok) {
    state.approvedChapterQuestions = [];
    return;
  }
  state.approvedChapterQuestions = Array.isArray(data.items) ? data.items : [];
}

async function loadScripture() {
  const versionIds = getAllSelectedScriptureVersionIds();

  const params = new URLSearchParams({
    bookId: state.frontState.bookId,
    chapter: String(state.frontState.chapter),
    versions: versionIds.join(","),
  });

  const res = await fetch(`/api/scripture?${params.toString()}`, {
    cache: "no-store",
  });

  const data = await res.json();
  if (!res.ok) {
    state.scriptureRows = [];
    throw new Error(data.error || "读取经文失败");
  }

  state.scriptureRows = data.rows || [];
}

async function loadStudyContent() {
  const params = new URLSearchParams({
    version: state.frontState.contentVersion,
    lang: state.frontState.contentLang,
    bookId: state.frontState.bookId,
    chapter: String(state.frontState.chapter),
  });

  const res = await fetch(`/api/study-content?${params.toString()}`, {
    cache: "no-store",
  });

  if (res.status === 404) {
    state.studyContent = null;
    return;
  }

  const data = await res.json();
  if (data && data.missing === true) {
    state.studyContent = null;
    return;
  }
  if (!res.ok) {
    state.studyContent = null;
    throw new Error(data.error || "读取查经内容失败");
  }

  state.studyContent = data;
}

function updatePageTitle() {
  const chapterNumberEl = document.getElementById("pageChapterNumber");
  const bookTitleEl = document.getElementById("pageBookTitle");
  const bottomTitleEl = document.getElementById("chapterNavTitleBottom");
  const bookChapterTriggerText = document.getElementById(
    "bookChapterTriggerText"
  );
  const scriptureBookLabel = getBookLabelForPrimaryScripture();
  const chapterLabel = `${state.frontState.chapter}`;
  const copy = getLocalizedCopy();
  const prevBottomBtn = document.getElementById("prevChapterBtnBottom");
  const nextBottomBtn = document.getElementById("nextChapterBtnBottom");
  const prevTopBtn = document.getElementById("prevChapterBtnTop");
  const nextTopBtn = document.getElementById("nextChapterBtnTop");

  if (chapterNumberEl) chapterNumberEl.textContent = chapterLabel;
  if (bookTitleEl) bookTitleEl.textContent = scriptureBookLabel;
  if (bottomTitleEl) {
    bottomTitleEl.textContent = formatBookChapterLabel(
      scriptureBookLabel,
      state.frontState.chapter
    );
  }
  if (bookChapterTriggerText) {
    bookChapterTriggerText.textContent = getLocalizedCopy().triggerBook;
  }
  if (prevBottomBtn) prevBottomBtn.textContent = copy.prevChapter;
  if (nextBottomBtn) nextBottomBtn.textContent = copy.nextChapter;
  if (prevTopBtn) prevTopBtn.setAttribute("aria-label", copy.prevChapter);
  if (nextTopBtn) nextTopBtn.setAttribute("aria-label", copy.nextChapter);
}

function renderStudyContent() {
  const leftBlocksEl = document.getElementById("leftBlocks");
  const rightBlocksEl = document.getElementById("rightBlocks");
  const repeatedWordsEl = document.getElementById("repeatedWordsLine");
  const chapterArtSlotEl = document.getElementById("chapterArtSlot");
  const approvedTitleEl = document.getElementById("chapterApprovedTitle");
  const approvedEl = document.getElementById("chapterApprovedQuestions");

  if (!state.studyContent) {
    const showQuestions = state.frontState.showQuestions !== false;
    const showScripture = state.frontState.showScripture !== false;
    const verses = (state.scriptureRows || [])
      .map((row) => Number(row.verse))
      .filter((n) => Number.isFinite(n))
      .sort((a, b) => a - b);
    const fallbackStart = verses[0] || 1;
    const fallbackEnd = verses[verses.length - 1] || 1;
    const splitAt = verses.length
      ? verses[Math.ceil(verses.length / 2) - 1]
      : fallbackEnd;

    if (repeatedWordsEl) repeatedWordsEl.textContent = "—";
    if (chapterArtSlotEl) chapterArtSlotEl.innerHTML = "";
    if (leftBlocksEl) {
      const scriptureLeftHtml = showScripture
        ? `<article class="flow-card flow-card-scripture-fallback">${renderVerseRangeBlock(
            fallbackStart,
            splitAt
          )}</article>`
        : "";
      const questionEmptyHtml = showQuestions
        ? `<div class="result-box result-box-plain"><div class="empty-state">${escapeHtml(
            getLocalizedCopy().noContent
          )}</div></div>`
        : "";
      leftBlocksEl.innerHTML = `${scriptureLeftHtml}${questionEmptyHtml}`;
    }
    if (rightBlocksEl) {
      const hasRight = showScripture && splitAt < fallbackEnd;
      rightBlocksEl.innerHTML = hasRight
        ? `<article class="flow-card flow-card-scripture-fallback">${renderVerseRangeBlock(
            splitAt + 1,
            fallbackEnd
          )}</article>`
        : "";
    }
    if (approvedEl) {
      const items = state.approvedChapterQuestions || [];
      if (approvedTitleEl) approvedTitleEl.style.display = items.length ? "" : "none";
      approvedEl.innerHTML = items.length
        ? items
            .map((item, idx) => renderApprovedQuestionItem(item, idx))
            .join("")
        : "";
    }
    return;
  }

  if (repeatedWordsEl) {
    repeatedWordsEl.innerHTML = renderRepeatedWords(
      state.studyContent.repeatedWords || []
    );
  }

  if (chapterArtSlotEl) chapterArtSlotEl.innerHTML = renderChapterArtSlotHtml();

  const rendered = (state.studyContent.segments || []).map(renderSegmentCard);
  const splitIndex = Math.ceil(rendered.length / 2);

  if (leftBlocksEl)
    leftBlocksEl.innerHTML = rendered.slice(0, splitIndex).join("");
  if (rightBlocksEl)
    rightBlocksEl.innerHTML = rendered.slice(splitIndex).join("");

  if (approvedEl) {
    const items = state.approvedChapterQuestions || [];
    if (approvedTitleEl) approvedTitleEl.style.display = items.length ? "" : "none";
    approvedEl.innerHTML = items.length
      ? items
          .map((item, idx) => renderApprovedQuestionItem(item, idx))
          .join("")
      : "";
  }
}

function renderApprovedQuestionItem(item, idx) {
  const questionId = String(item?.id || "").trim();
  const rawQ = String(item?.questionText || "");
  const replies = Array.isArray(item?.replies) ? item.replies : [];
  const interaction = getQaInteractionById(questionId);
  const likeBaseCount = Math.max(0, Number(item?.likeCount || 0));
  const likeCount = likeBaseCount + (interaction.liked ? 1 : 0);
  const actionsHtml = questionId
    ? `<div class="qa-actions">
        <button type="button" class="qa-action-btn ${
          interaction.liked ? "is-active" : ""
        }" data-question-id="${escapeHtml(questionId)}" data-action="like" data-base-count="${likeBaseCount}">
          <span class="qa-action-icon">♥</span>
          <span class="qa-action-label">点赞</span>
          <span class="qa-action-count">${likeCount}</span>
        </button>
        <button type="button" class="qa-action-btn" data-question-id="${escapeHtml(
          questionId
        )}" data-action="reply" data-base-count="0">
          <span class="qa-action-icon">↩</span>
          <span class="qa-action-label">回复</span>
        </button>
        <button type="button" class="qa-action-btn" data-question-id="${escapeHtml(
          questionId
        )}" data-action="correct" data-question-text="${escapeHtml(rawQ)}">
          <span class="qa-action-icon">✎</span>
          <span class="qa-action-label">纠错</span>
        </button>
      </div>`
    : "";
  const replyListHtml = replies.length
    ? `<div class="chapter-approved-replies">${replies
        .map(
          (reply) =>
            `<div class="chapter-approved-reply"><span class="chapter-approved-reply-author">${escapeHtml(
              String(reply?.userName || "用户")
            )}</span>：${escapeHtml(String(reply?.replyText || ""))}</div>`
        )
        .join("")}</div>`
    : "";
  const replyFormHtml = questionId
    ? `<div class="chapter-reply-editor">
        <textarea class="chapter-reply-input" placeholder="写下你的回复..."></textarea>
        <div class="chapter-reply-actions">
          <span class="chapter-reply-status"></span>
          <button type="button" class="chapter-reply-submit-btn" data-question-id="${escapeHtml(
            questionId
          )}">回复</button>
        </div>
      </div>`
    : "";
  const qianEdit =
    isQianfuzhangAdmin() && questionId
      ? ` data-qian-edit="1"`
      : "";
  const questionLeadHtml =
    isQianfuzhangAdmin() && questionId
      ? `<span class="chapter-approved-qtext" title="点击修改问题正文（千夫长）">${escapeHtml(
          rawQ
        )}</span>`
      : escapeHtml(rawQ);
  return `<div class="chapter-approved-item"${qianEdit} data-question-id="${escapeHtml(
    questionId
  )}">${idx + 1}. ${questionLeadHtml}${renderApprovedContributorMeta(
    item
  )}${actionsHtml}${replyListHtml}${replyFormHtml}</div>`;
}

function renderApprovedContributorMeta(item) {
  const nickname = String(item?.userNickname || item?.userName || "").trim();
  const level = Number(item?.userLevel || 0);
  if (!nickname && !level) return "";
  const bits = [];
  if (nickname) bits.push(nickname);
  if (level > 0) bits.push(`L${level}`);
  const stars = level > 0 ? renderLevelStars(level) : "";
  const starHtml = stars ? ` <span class="chapter-approved-stars">${escapeHtml(stars)}</span>` : "";
  return ` <span class="chapter-approved-meta">（${escapeHtml(bits.join(" "))}${starHtml}）</span>`;
}

function renderLevelStars(level) {
  const lv = Math.max(1, Math.min(12, Number(level) || 1));
  const starCount = Math.max(1, Math.min(5, Math.ceil(lv / 3)));
  return "★".repeat(starCount);
}

/** 书签入口右侧：等级标星（与后台积分预览 renderStars 同色阶） */
function renderMemberHubBookmarkStars(level) {
  const raw = Number(level) || 0;
  if (raw <= 0) {
    return `<span class="member-hub-star-strip member-hub-star-strip--empty" title="尚未形成等级（尚无已通过审核的贡献）">☆</span>`;
  }
  const capped = Math.max(1, Math.min(12, raw));
  const starsHtml = renderStars(capped, `等级 L${capped}`).replace(
    /\s+title="[^"]*"/,
    ""
  );
  return `<span class="member-hub-star-strip" title="等级 L${capped}">${starsHtml}</span>`;
}

/** 左侧丝带：等级星用 g1–g4 分段金色（与 renderStars / 积分配置一致） */
function renderRibbonLevelStars(level) {
  const raw = Number(level) || 0;
  if (raw <= 0) {
    return `<span class="member-hub-star-strip member-hub-star-strip--empty" title="尚未形成等级（尚无已通过审核的贡献）">☆</span>`;
  }
  const capped = Math.max(1, Math.min(12, raw));
  return `<span class="member-hub-star-strip">${renderStars(
    capped,
    `等级 L${capped}`
  )}</span>`;
}

/** 主题行纯文本（与页面 repeatedWords 一致，用于收藏列表展示） */
function formatRepeatedWordsThemePlain(items) {
  if (!items || !items.length) return "";
  return [...items]
    .sort((a, b) => Number(b.count || 0) - Number(a.count || 0))
    .map((item) => {
      const word = String(item.word || "").trim();
      if (!word) return "";
      const count = Number(item.count || 0);
      return count > 0 ? `${word} ×${count}` : word;
    })
    .filter(Boolean)
    .join(" · ");
}

function renderRepeatedWords(items) {
  if (!items || !items.length) return "—";

  return [...items]
    .sort((a, b) => Number(b.count || 0) - Number(a.count || 0))
    .map((item) => {
      const word = escapeHtml(item.word || "");
      const count = Number(item.count || 0);
      if (count > 0) {
        return `<span class="repeated-word-item">${word} <span class="repeated-word-count">× ${count}</span></span>`;
      }
      return `<span class="repeated-word-item">${word}</span>`;
    })
    .join("");
}

function cleanSegmentTitle(title) {
  return String(title || "")
    .replace(/\s*[\(（]\s*\d+\s*[-—–~～]\s*\d+\s*节?\s*[\)）]\s*$/g, "")
    .replace(/\s*[\(（]\s*\d+\s*节?\s*[\)）]\s*$/g, "")
    .trim();
}

function getHighlightWords() {
  return (state.studyContent?.repeatedWords || [])
    .map((x) => String(x.word || "").trim())
    .filter(Boolean)
    .sort((a, b) => b.length - a.length)
    .slice(0, 3);
}

function highlightText(rawText, words) {
  if (!rawText) return "";
  if (!words?.length) return escapeHtml(rawText);

  const cleanWords = words.map((w) => String(w).trim()).filter(Boolean);
  const escapedWords = cleanWords.map((w) =>
    w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
  );

  const regex = new RegExp(escapedWords.join("|"), "g");
  let result = "";
  let lastIndex = 0;

  for (const match of rawText.matchAll(regex)) {
    const found = match[0];
    const offset = match.index ?? 0;

    result += escapeHtml(rawText.slice(lastIndex, offset));

    const wordIndex = cleanWords.findIndex((w) => w === found);
    const clsIndex = wordIndex >= 0 ? Math.min(wordIndex + 1, 3) : 1;

    result += `<span class="hl-word hl-${clsIndex}">${escapeHtml(
      found
    )}</span>`;
    lastIndex = offset + found.length;
  }

  result += escapeHtml(rawText.slice(lastIndex));
  return result;
}

function getVersesByRange(start, end) {
  return (state.scriptureRows || []).filter(
    (row) => row.verse >= start && row.verse <= end
  );
}

function renderVerseRangeBlock(start, end) {
  const rows = getVersesByRange(start, end);
  const primaryId = state.frontState.primaryScriptureVersionId;
  const primaryVersion = getPrimaryScriptureVersion();
  const secondaryVersions = getSecondaryScriptureVersions();
  const highlightWords = getHighlightWords();

  if (!rows.length) {
    return `<div class="flow-scripture empty-state">暂无这段经文</div>`;
  }

  const renderRtlContinuous = (versionId) => {
    const mergedText = rows
      .map((row) => String(row.texts?.[versionId] || "").trim())
      .filter(Boolean)
      .join(" ");
    if (!mergedText) return "";
    return escapeHtml(mergedText);
  };

  const primaryIsHebrew = primaryVersion?.lang === "he";
  const renderVersionIds = [
    primaryId,
    ...secondaryVersions.map((v) => v.id),
  ].filter(Boolean);
  const chapterRows = state.scriptureRows || rows;
  const divineSetByVersion = new Map(
    renderVersionIds.map((versionId) => [
      versionId,
      buildDivineSpeechVerseSet(chapterRows, versionId),
    ])
  );

  const primaryHtml = primaryIsHebrew
    ? `<div class="flow-verse flow-verse-rtl flow-verse-continuous">${renderRtlContinuous(
        primaryId
      )}</div>`
    : `<div class="flow-verse flow-verse-continuous">${rows
        .map((row) => {
          return buildFavoriteVerseUnit(
            row,
            primaryId,
            row.texts?.[primaryId] || "",
            highlightWords,
            divineSetByVersion.get(primaryId)?.has(Number(row?.verse || 0)) || false
          );
        })
        .filter(Boolean)
        .join(" ")}</div>`;

  const secondaryContinuous = secondaryVersions
            .map((version) => {
      if (version.lang === "he") {
        const rtlText = renderRtlContinuous(version.id);
        if (!rtlText) return "";
        return `<div class="flow-verse-sub flow-verse-rtl flow-verse-continuous">${rtlText}</div>`;
      }

      const verseUnits = rows
        .map((row) => {
          return buildFavoriteVerseUnit(
            row,
            version.id,
            row.texts?.[version.id] || "",
            highlightWords,
            divineSetByVersion.get(version.id)?.has(Number(row?.verse || 0)) || false
          );
        })
        .filter(Boolean)
        .join(" ");
      if (!verseUnits) return "";
      return `<div class="flow-verse-sub flow-verse-continuous">${verseUnits}</div>`;
            })
            .join("");

          return `
    <div class="flow-scripture">
      ${primaryHtml}
      ${secondaryContinuous}
    </div>
  `;
}

function buildQuestionFavoriteKey(seg, questionText, index) {
  const seed = [
    state.frontState.bookId,
    state.frontState.chapter,
    String(seg?.rangeStart || ""),
    String(seg?.rangeEnd || ""),
    String(seg?.title || ""),
    String(index),
    String(questionText || ""),
  ].join("|");
  let hash = 0;
  for (let i = 0; i < seed.length; i += 1) {
    hash = (hash * 31 + seed.charCodeAt(i)) >>> 0;
  }
  return `qfav_${hash.toString(16)}`;
}

/** 与 server.js stablePresetCorrectionKey 一致（不含题干文本，便于纠错后仍命中同一条） */
function stablePresetCorrectionKeyClient(
  bookId,
  chapter,
  contentVersion,
  contentLang,
  rangeStart,
  rangeEnd,
  segmentTitle,
  questionIndex
) {
  const seed = [
    String(bookId || ""),
    String(Number(chapter) || 0),
    String(contentVersion || ""),
    String(contentLang || ""),
    String(Number(rangeStart) || 0),
    String(Number(rangeEnd) || 0),
    String(segmentTitle || ""),
    String(Number(questionIndex) || 0),
  ].join("|");
  let hash = 0;
  for (let i = 0; i < seed.length; i += 1) {
    hash = (hash * 31 + seed.charCodeAt(i)) >>> 0;
  }
  return `qcorr_${hash.toString(16)}`;
}

function openQuestionCorrectionDialog(payload) {
  const mask = document.createElement("div");
  mask.className = "modal-mask question-correction-mask";
  const card = document.createElement("div");
  card.className = "modal-card modal-card-sm question-correction-card";
  const title = document.createElement("h2");
  title.className = "question-correction-title";
  title.textContent = "纠错";
  const hint = document.createElement("p");
  hint.className = "question-correction-hint";
  hint.textContent = isQianfuzhangAdmin()
    ? "千夫长提交后立即生效。"
    : "提交后由管理员审核，通过后将替换展示文案。";
  const ta = document.createElement("textarea");
  ta.className = "custom-textarea question-correction-textarea";
  ta.value = String(payload.originalText || "");
  ta.rows = 5;
  const statusEl = document.createElement("div");
  statusEl.className = "error-text question-correction-status";
  const actions = document.createElement("div");
  actions.className = "question-correction-actions";
  const submitBtn = document.createElement("button");
  submitBtn.type = "button";
  submitBtn.className = "primary-btn";
  submitBtn.textContent = "提交";
  const cancelBtn = document.createElement("button");
  cancelBtn.type = "button";
  cancelBtn.className = "secondary-btn";
  cancelBtn.textContent = "取消";
  actions.append(submitBtn, cancelBtn, statusEl);
  card.append(title, hint, ta, actions);
  mask.append(card);
  document.body.append(mask);

  function close() {
    mask.remove();
  }

  cancelBtn.addEventListener("click", close);
  mask.addEventListener("click", (ev) => {
    if (ev.target === mask) close();
  });

  submitBtn.addEventListener("click", async () => {
    const proposedText = String(ta.value || "").trim();
    if (proposedText.length < 2) {
      statusEl.textContent = "至少 2 个字";
      return;
    }
    submitBtn.setAttribute("disabled", "disabled");
    cancelBtn.setAttribute("disabled", "disabled");
    statusEl.textContent = "提交中...";
    try {
      const body =
        payload.targetType === "preset"
          ? {
              targetType: "preset",
              bookId: payload.bookId,
              chapter: payload.chapter,
              contentVersion: payload.contentVersion,
              contentLang: payload.contentLang,
              rangeStart: payload.rangeStart,
              rangeEnd: payload.rangeEnd,
              segmentTitle: payload.segmentTitle,
              questionIndex: payload.questionIndex,
              originalText: payload.originalText,
              proposedText,
            }
          : {
              targetType: "approved",
              questionId: payload.questionId,
              originalText: payload.originalText,
              proposedText,
            };
      const res = await fetch("/api/question-corrections/submit", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${getAuthToken()}`,
        },
        body: JSON.stringify(body),
      });
      const raw = await res.text();
      let data = {};
      if (raw) {
        try {
          data = JSON.parse(raw);
        } catch {
          const looksHtml = raw.trimStart().toLowerCase().startsWith("<!doctype") || raw.trimStart().startsWith("<html");
          throw new Error(
            looksHtml
              ? "服务器返回了网页而不是接口数据。请把含「纠错」接口的最新后端部署到 askbible.me 并重启 Node，或本地用 node server.js 访问（勿只用静态托管）。"
              : "服务器返回异常，无法解析为 JSON"
          );
        }
      }
      if (!res.ok) throw new Error(data.error || "提交失败");
      if (data.status === "approved") {
        statusEl.textContent = "已生效";
      } else {
        statusEl.textContent = "已提交审核";
      }
      setTimeout(async () => {
        close();
        await refreshCurrentPage();
      }, 500);
    } catch (err) {
      statusEl.textContent = err?.message || "提交失败";
      submitBtn.removeAttribute("disabled");
      cancelBtn.removeAttribute("disabled");
    }
  });
}

const READER_TOAST_MS = 2200;
const VERSE_FAVORITE_FLASH_MS = 900;

let readerToastHideTimer = 0;

function showReaderToast(message) {
  const text = String(message || "").trim();
  if (!text) return;
  let el = document.getElementById("readerAppToast");
  if (!el) {
    el = document.createElement("div");
    el.id = "readerAppToast";
    el.className = "reader-app-toast";
    el.setAttribute("role", "status");
    el.setAttribute("aria-live", "polite");
    document.body.appendChild(el);
  }
  el.textContent = text;
  el.classList.add("reader-app-toast--visible");
  window.clearTimeout(readerToastHideTimer);
  readerToastHideTimer = window.setTimeout(() => {
    el.classList.remove("reader-app-toast--visible");
  }, READER_TOAST_MS);
}

const FAVORITES_LIST_PULSE_MS = 620;
/** 与 CSS sideFavoritesPlusOneInner 时长一致，结束前不移除节点 */
const FAVORITES_PLUS_ONE_MS = 1500;

/** 双点加入经文/问题收藏后：丝带 + 顶栏星 pulse；+1 在双击处飘出（外层只负责定位，内层动画单独用 transform） */
function cueFavoritesListItemSaved(atPoint) {
  const pulseClass = "favorites-list-saved-pulse";
  const ribbon = document.getElementById("sideUserTag");
  if (ribbon) {
    ribbon.classList.remove(pulseClass);
    void ribbon.offsetWidth;
    ribbon.classList.add(pulseClass);
    window.setTimeout(() => ribbon.classList.remove(pulseClass), FAVORITES_LIST_PULSE_MS);
  }
  const starBtn = document.getElementById("sideFavoritesListBtn");
  if (starBtn) {
    starBtn.classList.remove(pulseClass);
    void starBtn.offsetWidth;
    starBtn.classList.add(pulseClass);
    window.setTimeout(() => starBtn.classList.remove(pulseClass), FAVORITES_LIST_PULSE_MS);
  }

  const x = Number(atPoint?.clientX);
  const y = Number(atPoint?.clientY);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return;
  const anchor = document.createElement("span");
  anchor.className = "side-favorites-plus-one-anchor";
  anchor.style.left = `${Math.round(x)}px`;
  /* 起点在点击点略下，便于看清再向上飘 */
  anchor.style.top = `${Math.round(y + 4)}px`;
  const pop = document.createElement("span");
  pop.className = "side-favorites-plus-one-float";
  pop.textContent = "+1";
  pop.setAttribute("aria-hidden", "true");
  anchor.appendChild(pop);
  document.body.appendChild(anchor);
  window.setTimeout(() => anchor.remove(), FAVORITES_PLUS_ONE_MS);
}

function initFavorites() {
  document.addEventListener("dblclick", (event) => {
    const unit = event.target?.closest?.("[data-favorite-key]");
    if (!unit) return;
    const key = unit.getAttribute("data-favorite-key");
    const versionId = unit.getAttribute("data-favorite-version-id");
    const verse = Number(unit.getAttribute("data-favorite-verse") || 0);
    const text = unit.getAttribute("data-favorite-text") || "";
    if (!key || !versionId || !verse || !text) return;

    const copy = getLocalizedCopy();
    let didAdd = false;

    if (state.favoriteKeys.has(key)) {
      state.favorites = (state.favorites || []).filter((x) => x.key !== key);
      unit.classList.remove("is-favorited");
      reportGlobalFavoriteToggle({
        action: "remove",
        type: "verse",
        key,
      });
      showReaderToast(copy.verseFavoriteRemovedToast);
    } else {
      didAdd = true;
      state.favorites = [
        {
          key,
          versionId,
          verse,
          text,
          bookId: state.frontState.bookId,
          chapter: Number(state.frontState.chapter || 1),
          createdAt: Date.now(),
        },
        ...(state.favorites || []),
      ];
      unit.classList.add("is-favorited");
      unit.classList.remove("favorite-flash");
      void unit.offsetWidth;
      unit.classList.add("favorite-flash");
      window.setTimeout(() => {
        unit.classList.remove("favorite-flash");
      }, VERSE_FAVORITE_FLASH_MS);
      reportGlobalFavoriteToggle({
        action: "add",
        type: "verse",
        key,
        bookId: state.frontState.bookId,
        chapter: Number(state.frontState.chapter || 1),
        verse,
        title: formatBookChapterLabel(state.frontState.bookId, Number(state.frontState.chapter || 1)) + ":" + verse,
        content: text,
      });
      showReaderToast(copy.verseFavoriteAddedToast);
    }
    saveFavorites();
    renderToolbarTriggers();
    renderFavoritesPanel();
    if (didAdd) {
      cueFavoritesListItemSaved({
        clientX: event.clientX,
        clientY: event.clientY,
      });
    }
  });

  document.addEventListener("dblclick", (event) => {
    const item = event.target?.closest?.("[data-question-fav-key]");
    if (!item) return;
    const key = item.getAttribute("data-question-fav-key");
    const question = item.getAttribute("data-question-fav-text") || "";
    const title = item.getAttribute("data-question-fav-title") || "";
    if (!key || !question) return;

    const copy = getLocalizedCopy();
    let didAdd = false;

    if (state.questionFavoriteKeys.has(key)) {
      state.questionFavorites = (state.questionFavorites || []).filter(
        (x) => x.key !== key
      );
      item.classList.remove("is-favorited");
      reportGlobalFavoriteToggle({
        action: "remove",
        type: "question",
        key,
      });
      showReaderToast(copy.questionFavoriteRemovedToast);
    } else {
      didAdd = true;
      state.questionFavorites = [
        {
          key,
          question,
          title,
          bookId: state.frontState.bookId,
          chapter: Number(state.frontState.chapter || 1),
          contentVersion: state.frontState.contentVersion,
          contentLang: state.frontState.contentLang,
          createdAt: Date.now(),
        },
        ...(state.questionFavorites || []),
      ];
      item.classList.add("is-favorited");
      item.classList.remove("favorite-flash");
      void item.offsetWidth;
      item.classList.add("favorite-flash");
      window.setTimeout(() => {
        item.classList.remove("favorite-flash");
      }, VERSE_FAVORITE_FLASH_MS);
      reportGlobalFavoriteToggle({
        action: "add",
        type: "question",
        key,
        bookId: state.frontState.bookId,
        chapter: Number(state.frontState.chapter || 1),
        title: title || "",
        content: question,
        contentVersion: state.frontState.contentVersion,
        contentLang: state.frontState.contentLang,
      });
      showReaderToast(copy.questionFavoriteAddedToast);
    }
    saveQuestionFavorites();
    renderToolbarTriggers();
    renderFavoritesPanel();
    if (didAdd) {
      cueFavoritesListItemSaved({
        clientX: event.clientX,
        clientY: event.clientY,
      });
    }
  });

}

function renderSegmentCard(seg) {
  const title = cleanSegmentTitle(seg.title || "未命名段落");
  const start = Number(seg.rangeStart || 0);
  const end = Number(seg.rangeEnd || 0);
  const showQuestions = state.frontState.showQuestions !== false;
  const showScripture = state.frontState.showScripture !== false;

  const scriptureHtml =
    !showScripture || !start || !end
      ? ""
      : renderVerseRangeBlock(start, end);

  const questionHtml = !showQuestions
    ? ""
    : `<div class="mini-section">
        <ul class="plain-list">
          ${(seg.questions || [])
            .map((q, idx) => {
              const qText = transformQuestionDisplayText(q);
              const qKey = buildQuestionFavoriteKey(seg, qText, idx);
              const active = state.questionFavoriteKeys.has(qKey) ? " is-favorited" : "";
              const presetQaId = `preset_${qKey}`;
              const presetQaHtml = renderPresetQuestionInlineActions(
                seg,
                idx,
                qText,
                presetQaId
              );
              return `<li class="question-fav-item${active}" data-question-fav-key="${escapeHtml(
                qKey
              )}" data-question-fav-text="${escapeHtml(qText)}" data-question-fav-title="${escapeHtml(
                title
              )}">${escapeHtml(qText)}${presetQaHtml}</li>`;
            })
            .join("")}
        </ul>
      </div>`;

  return `
    <article class="flow-card ${
      !showScripture
        ? "flow-card-no-scripture"
        : !showQuestions
        ? "flow-card-scripture-only"
        : ""
    }">
      <h3>${escapeHtml(title)}</h3>
      ${scriptureHtml}
      ${questionHtml}
    </article>
  `;
}

function renderPresetQuestionInlineActions(seg, questionIndex, questionText, questionId) {
  const interaction = getQaInteractionById(questionId);
  const likeBaseCount = 0;
  const likeCount = likeBaseCount + (interaction.liked ? 1 : 0);
  const bookId = state.frontState.bookId;
  const chapter = Number(state.frontState.chapter || 0);
  const contentVersion = state.frontState.contentVersion;
  const contentLang = state.frontState.contentLang;
  const rangeStart = Number(seg?.rangeStart || 0);
  const rangeEnd = Number(seg?.rangeEnd || 0);
  const segmentTitle = String(seg?.title || "");
  const sk = stablePresetCorrectionKeyClient(
    bookId,
    chapter,
    contentVersion,
    contentLang,
    rangeStart,
    rangeEnd,
    segmentTitle,
    questionIndex
  );
  return ` <span class="preset-qa-inline">
      <button type="button" class="preset-qa-action-btn ${
        interaction.liked ? "is-active" : ""
      }" data-action="like" data-question-id="${escapeHtml(
    questionId
  )}" data-base-count="${likeBaseCount}">
        <span class="preset-qa-action-icon">♥</span><span class="preset-qa-action-count">${likeCount}</span>
      </button>
      <button type="button" class="preset-qa-action-btn" data-action="reply" data-question-id="${escapeHtml(
        questionId
      )}" data-question-text="${escapeHtml(questionText)}">
        <span class="preset-qa-action-icon" aria-hidden="true">${REPLY_ACTION_GLYPH_SVG}</span><span>回复</span>
      </button>
      <button type="button" class="preset-qa-action-btn" data-action="correct" data-book-id="${escapeHtml(
        bookId
      )}" data-chapter="${escapeHtml(String(chapter))}" data-content-version="${escapeHtml(
        contentVersion
      )}" data-content-lang="${escapeHtml(
        contentLang
      )}" data-range-start="${escapeHtml(String(rangeStart))}" data-range-end="${escapeHtml(
        String(rangeEnd)
      )}" data-segment-title="${escapeHtml(segmentTitle)}" data-question-index="${escapeHtml(
        String(questionIndex)
      )}" data-original-text="${escapeHtml(questionText)}" data-stable-key="${escapeHtml(sk)}">
        <span class="preset-qa-action-icon">✎</span><span>纠错</span>
      </button>
    </span>`;
}

function flattenBooks() {
  return state.bootstrap?.testamentOptions || [];
}

function getCurrentBookIndex() {
  const allBooks = flattenBooks();
  return allBooks.findIndex((b) => b.bookId === state.frontState.bookId);
}

function getAdjacentChapterTarget(direction) {
  const allBooks = flattenBooks();
  const idx = getCurrentBookIndex();
  if (idx < 0) return null;

  const current = allBooks[idx];
  const currentChapter = Number(state.frontState.chapter || 1);

  if (direction < 0) {
    if (currentChapter > 1) {
      return {
        testament: current.testamentName,
        bookId: current.bookId,
        chapter: currentChapter - 1,
      };
    }

    const prevBook = allBooks[idx - 1];
    if (!prevBook) return null;

    return {
      testament: prevBook.testamentName,
      bookId: prevBook.bookId,
      chapter: prevBook.chapters,
    };
  }

  if (currentChapter < Number(current.chapters || 1)) {
    return {
      testament: current.testamentName,
      bookId: current.bookId,
      chapter: currentChapter + 1,
    };
  }

  const nextBook = allBooks[idx + 1];
  if (!nextBook) return null;

  return {
    testament: nextBook.testamentName,
    bookId: nextBook.bookId,
    chapter: 1,
  };
}

function initChapterNav() {
  document
    .getElementById("prevChapterBtnTop")
    ?.addEventListener("click", () => {
      goAdjacentChapter(-1);
    });

  document
    .getElementById("nextChapterBtnTop")
    ?.addEventListener("click", () => {
      goAdjacentChapter(1);
    });

  document
    .getElementById("prevChapterBtnBottom")
    ?.addEventListener("click", () => {
      goAdjacentChapter(-1);
    });

  document
    .getElementById("nextChapterBtnBottom")
    ?.addEventListener("click", () => {
      goAdjacentChapter(1);
    });
}

async function goAdjacentChapter(direction) {
  const target = getAdjacentChapterTarget(direction);
  if (!target) return;

  state.frontState.testament = target.testament;
  state.frontState.bookId = target.bookId;
  state.frontState.chapter = target.chapter;

  saveFrontState();
  renderAllSelectors();
  await refreshCurrentPage();
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function updateChapterNavUI() {
  const prevDisabled = !getAdjacentChapterTarget(-1);
  const nextDisabled = !getAdjacentChapterTarget(1);

  [
    document.getElementById("prevChapterBtnTop"),
    document.getElementById("prevChapterBtnBottom"),
  ].forEach((btn) => {
    if (btn) btn.disabled = prevDisabled;
  });

  [
    document.getElementById("nextChapterBtnTop"),
    document.getElementById("nextChapterBtnBottom"),
  ].forEach((btn) => {
    if (btn) btn.disabled = nextDisabled;
  });
}

/* =========================
   后台管理
   ========================= */
const ADMIN_MODAL_TOP_CSS_VAR = "--admin-modal-inset-top";

function syncAdminModalInsetTop() {
  const bar = document.querySelector(".site-topbar");
  const h = bar ? bar.getBoundingClientRect().height : 0;
  const px = Math.max(1, Math.round(h));
  document.documentElement.style.setProperty(ADMIN_MODAL_TOP_CSS_VAR, `${px}px`);
}

function clearAdminModalInsetTop() {
  document.documentElement.style.removeProperty(ADMIN_MODAL_TOP_CSS_VAR);
}

/** 从管理总览等跳转 `/#openAdmin` 时自动打开读经页大面板（规则、任务等） */
function tryConsumeAdminDeepLink() {
  function consume() {
    try {
      if (window.location.hash !== "#openAdmin") return false;
      const u = new URL(window.location.href);
      u.hash = "";
      history.replaceState({}, "", u.pathname + (u.search || ""));
      queueMicrotask(() => {
        document.getElementById("openAdminBtn")?.dispatchEvent(
          new MouseEvent("click", { bubbles: true, cancelable: true })
        );
      });
      return true;
    } catch {
      return false;
    }
  }
  consume();
  window.addEventListener("hashchange", () => {
    if (window.location.hash === "#openAdmin") consume();
  });
}

function initAdminModal() {
  const adminModal = document.getElementById("adminModal");
  const openAdminBtn = document.getElementById("openAdminBtn");
  const closeAdminBtn = document.getElementById("closeAdminBtn");

  window.addEventListener(
    "resize",
    () => {
      if (adminModal && adminModal.style.display === "block") {
        syncAdminModalInsetTop();
      }
    },
    { passive: true }
  );

  async function openAdminRealModal() {
    await fetchCurrentUser();
    await loadAdminBootstrap();
    bindAdminTabs();
    await initRuleEditorTab();
    await initTestGenerateTab();
    await initPublishedManagerTab();
    await initScriptureVersionManagerTab();
    await initContentVersionsManagerTab();
    await initDeployManagerTab();
    await initPointsSystemTab();
    initQuestionReviewTab();
    initAdminUsersTab();
    startJobsAutoRefresh();
    syncAdminModalInsetTop();
    if (adminModal) adminModal.style.display = "block";
    requestAnimationFrame(() => {
      syncAdminModalInsetTop();
    });
  }

  async function openAdminIfAuthorized() {
    await fetchCurrentUser();
    if (!userHasSiteAdminAccess(state.currentUser)) {
      window.alert("请使用已登录的管理员账号进入后台管理。");
      return;
    }
    try {
      await openAdminRealModal();
    } catch (e) {
      window.alert(String(e?.message || e) || "后台加载失败");
    }
  }

  openAdminBtn?.addEventListener("click", () => void openAdminIfAuthorized());

  closeAdminBtn?.addEventListener("click", () => {
    if (adminModal) adminModal.style.display = "none";
    clearAdminModalInsetTop();
    stopJobsAutoRefresh();
  });
}

/** Parse JSON from fetch; if body is HTML (SPA/404/login page), throw a clear error. */
async function parseFetchJsonResponse(res) {
  const text = await res.text();
  const head = text.trimStart().slice(0, 64).toLowerCase();
  if (
    head.startsWith("<!doctype") ||
    head.startsWith("<html") ||
    head.startsWith("<head")
  ) {
    throw new Error(
      "接口返回了网页而不是 JSON。请先在浏览器「网络」里查看该请求的 URL、状态码与响应正文。常见原因：① 本机 3000 端口不是本项目的 node server.js（请 lsof -i :3000 确认并重启 npm start）；② 反代把 /api 指错；③ 会话过期被重定向到登录页；④ 线上未部署含该接口的版本。若刚改过 server.js，务必完全重启 Node 后再试。"
    );
  }
  try {
    return JSON.parse(text);
  } catch {
    const preview = text.replace(/\s+/g, " ").slice(0, 120);
    throw new Error(
      `无效 JSON 响应（HTTP ${res.status}）${preview ? `：${preview}…` : ""}`
    );
  }
}

async function loadAdminBootstrap() {
  const token = getAuthToken();
  const res = await fetch("/api/admin/bootstrap", {
    cache: "no-store",
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  const data = await parseFetchJsonResponse(res);

  if (!res.ok) {
    throw new Error(data.error || "后台初始化失败");
  }

  adminState.bootstrap = data;
  adminState.scriptureVersions = data.scriptureVersions || [];
  adminState.currentPointsConfig = data.pointsConfig || null;
}

function bindAdminTabs() {
  ensurePublishedTabExists();
  ensureScriptureVersionManagerTabExists();
  ensureContentVersionsManagerTabExists();
  ensureDeployTabExists();
  ensurePointsSystemTabExists();
  ensureQuestionReviewTabExists();
  ensureAdminUsersTabExists();

  document.querySelectorAll(".admin-tab-btn").forEach((btn) => {
    btn.onclick = null;
    btn.addEventListener("click", () => {
      const tab = btn.dataset.adminTab;

      document.querySelectorAll(".admin-tab-btn").forEach((x) => {
        x.classList.toggle("active", x === btn);
      });

      document.querySelectorAll(".admin-tab-panel").forEach((panel) => {
        panel.classList.remove("active");
      });

      document.getElementById(`adminTab-${tab}`)?.classList.add("active");
    });
  });
}

function ensureAdminUsersTabExists() {
  if (document.querySelector('.admin-tab-btn[data-admin-tab="admin_users"]')) return;
  const tabsWrap = document.querySelector(".admin-tabs");
  if (!tabsWrap) return;

  const panel = document.createElement("div");
  panel.className = "admin-tab-panel";
  panel.id = "adminTab-admin_users";
  panel.innerHTML = `
    <div class="section-title">管理员权限</div>
    <div class="result-box" style="margin-bottom:10px;">按邮箱分配管理等级：十夫长 / 百夫长 / 千夫长。</div>
    <div class="admin-grid">
      <div>
        <div class="label">用户邮箱</div>
        <input id="adminUserEmailInput" class="custom-textarea single-input" placeholder="name@example.com" />
      </div>
      <div>
        <div class="label">管理员等级</div>
        <select id="adminUserRoleSelect">
          <option value="shifuzhang">十夫长（审核贡献）</option>
          <option value="baifuzhang">百夫长（审核 + 发布/成长体系）</option>
          <option value="qianfuzhang">千夫长（全量管理）</option>
          <option value="">无管理权限</option>
        </select>
      </div>
      <div style="grid-column:1 / -1;">
        <div class="label">初始化口令（仅首次设置千夫长时可填）</div>
        <input id="adminInitPasswordInput" type="password" class="custom-textarea single-input" placeholder="如已存在千夫长可留空" />
      </div>
    </div>
    <div class="modal-actions">
      <button id="saveAdminRoleBtn" class="primary-btn" type="button">保存管理等级</button>
      <button id="refreshAdminListBtn" class="secondary-btn" type="button">刷新管理员列表</button>
    </div>
    <div id="adminUsersResultBox" class="result-box">尚未操作。</div>
    <div class="section-title">当前管理员（只读）</div>
    <div id="adminUsersListBox" class="admin-preview-result"><div class="empty-state">暂无数据。</div></div>
  `;
  const modalCard = document.querySelector(".modal-card-admin");
  const existingPanels = modalCard?.querySelectorAll(".admin-tab-panel");
  if (existingPanels?.length) {
    existingPanels[existingPanels.length - 1].after(panel);
  } else {
    modalCard?.appendChild(panel);
  }

  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "admin-tab-btn";
  btn.dataset.adminTab = "admin_users";
  btn.textContent = "权限管理";
  tabsWrap.appendChild(btn);
}

function ensureQuestionReviewTabExists() {
  if (document.querySelector('.admin-tab-btn[data-admin-tab="question_review"]')) return;
  const tabsWrap = document.querySelector(".admin-tabs");
  if (!tabsWrap) return;

  const panel = document.createElement("div");
  panel.className = "admin-tab-panel";
  panel.id = "adminTab-question_review";
  panel.innerHTML = `
    <div class="section-title">贡献审核</div>
    <div id="questionReviewHint" class="result-box" style="margin-bottom:10px;">
      在这里直接审核用户贡献的问题（通过 / 拒绝）。
    </div>
    <iframe
      id="questionReviewFrame"
      src="about:blank"
      title="贡献审核"
      style="width:100%; min-height:560px; border:1px solid rgba(214,203,187,.72); border-radius:12px; background:#fff;"
    ></iframe>
  `;
  const modalCard = document.querySelector(".modal-card-admin");
  const existingPanels = modalCard?.querySelectorAll(".admin-tab-panel");
  if (existingPanels?.length) {
    existingPanels[existingPanels.length - 1].after(panel);
  } else {
    modalCard?.appendChild(panel);
  }

  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "admin-tab-btn";
  btn.dataset.adminTab = "question_review";
  btn.textContent = "贡献审核";
  tabsWrap.appendChild(btn);
}

function initQuestionReviewTab() {
  const frameEl = document.getElementById("questionReviewFrame");
  const hintEl = document.getElementById("questionReviewHint");
  if (!frameEl || !hintEl) return;
  if (!state.currentUser) {
    hintEl.textContent = "请先登录后再使用贡献审核。";
    frameEl.src = "about:blank";
    return;
  }
  const role = String(state.currentUser?.adminRole || "");
  if (!["shifuzhang", "baifuzhang", "qianfuzhang"].includes(role)) {
    hintEl.textContent = "你当前不是管理员，暂无审核权限。";
    frameEl.src = "about:blank";
    return;
  }
  const token = getAuthToken();
  if (!token) {
    hintEl.textContent = "登录状态缺失，请重新登录。";
    frameEl.src = "about:blank";
    return;
  }
  hintEl.textContent = "你拥有审核权限，可直接在下方审核用户贡献问题。";
  frameEl.src = `/admin/questions-review?token=${encodeURIComponent(token)}`;
}

function initAdminUsersTab() {
  const emailEl = document.getElementById("adminUserEmailInput");
  const roleEl = document.getElementById("adminUserRoleSelect");
  const initPwdEl = document.getElementById("adminInitPasswordInput");
  const saveBtn = document.getElementById("saveAdminRoleBtn");
  const refreshBtn = document.getElementById("refreshAdminListBtn");
  const resultEl = document.getElementById("adminUsersResultBox");
  const listEl = document.getElementById("adminUsersListBox");
  if (!emailEl || !roleEl || !resultEl || !listEl) return;

  const myRole = String(state.currentUser?.adminRole || "");
  if (myRole !== "qianfuzhang") {
    resultEl.textContent = "如果尚未初始化千夫长，请填写“初始化口令”后保存；否则仅千夫长可分配。";
  }

  async function loadAdminList() {
    listEl.innerHTML = `<div class="empty-state">加载中...</div>`;
    const token = getAuthToken();
    const res = await fetch(`/api/admin/users/admin-list`, {
      cache: "no-store",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
    const data = await res.json();
    if (!res.ok) {
      listEl.innerHTML = `<div class="empty-state">${escapeHtml(
        data.error || "读取失败"
      )}</div>`;
      return;
    }
    const items = Array.isArray(data.items) ? data.items : [];
    if (!items.length) {
      listEl.innerHTML = `<div class="empty-state">当前没有管理员。</div>`;
      return;
    }
    listEl.innerHTML = items
      .map(
        (x) => `
      <div class="test-result-seg">
        <div><strong>${escapeHtml(x.name || "未命名用户")}</strong></div>
        <div class="test-result-line">${escapeHtml(x.email || "")}</div>
        <div class="test-result-line">角色：${escapeHtml(x.adminRole || "—")}</div>
        <div class="test-result-line">创建：${escapeHtml(x.createdAt || "—")}</div>
      </div>
    `
      )
      .join("");
  }

  async function submitRole() {
    const email = String(emailEl.value || "").trim().toLowerCase();
    const role = String(roleEl.value || "").trim();
    const initPassword = String(initPwdEl?.value || "").trim();
    if (!email || !email.includes("@")) {
      resultEl.textContent = "请输入正确的用户邮箱。";
      return;
    }
    resultEl.textContent = "提交中...";
    const token = getAuthToken();
    const res = await fetch("/api/admin/users/set-admin", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify({
        email,
        role,
        adminPassword: initPassword,
      }),
    });
    const data = await res.json();
    if (!res.ok) {
      resultEl.textContent = data.error || "设置失败";
      return;
    }
    resultEl.textContent = `${data.user?.email || email} 角色已更新为 ${data.user?.adminRole || "无管理权限"}`;
    initQuestionReviewTab();
    await loadAdminList();
  }

  saveBtn?.addEventListener("click", async () => {
    await submitRole();
  });
  refreshBtn?.addEventListener("click", async () => {
    await loadAdminList();
  });
}

function ensurePointsSystemTabExists() {
  if (document.querySelector('.admin-tab-btn[data-admin-tab="points_system"]')) return;
  const tabsWrap = document.querySelector(".admin-tabs");
  if (!tabsWrap) return;

  const panel = document.createElement("div");
  panel.className = "admin-tab-panel";
  panel.id = "adminTab-points_system";
  panel.innerHTML = `
    <div class="admin-two-col">
      <div class="admin-left-col">
        <div class="section-title">学习成长命名体系</div>
        <div class="admin-grid">
          <div><div class="label">积分名称</div><input id="pointsNameInput" class="custom-textarea single-input" /></div>
          <div><div class="label">等级名称</div><input id="pointsLevelNameInput" class="custom-textarea single-input" /></div>
          <div><div class="label">记录名称</div><input id="pointsRecordNameInput" class="custom-textarea single-input" /></div>
          <div><div class="label">榜单名称</div><input id="pointsBoardNameInput" class="custom-textarea single-input" /></div>
        </div>
        <div class="section-title">说明文案</div>
        <textarea id="pointsNoteInput" class="custom-textarea"></textarea>
        <div class="section-title">等级称号（每行一个）</div>
        <textarea id="pointsLevelsInput" class="custom-textarea"></textarea>
        <div class="modal-actions">
          <button id="reloadPointsConfigBtn" class="secondary-btn" type="button">重新读取</button>
          <button id="savePointsConfigBtn" class="primary-btn" type="button">保存配置</button>
        </div>
      </div>
      <div class="admin-right-col">
        <div class="section-title">星级预览（仅星号）</div>
        <div id="pointsStarPreviewBox" class="result-box"></div>
        <div class="section-title">配置预览</div>
        <pre id="pointsConfigPreview" class="admin-preview-box">尚未读取。</pre>
      </div>
    </div>
  `;
  const modalCard = document.querySelector(".modal-card-admin");
  const existingPanels = modalCard?.querySelectorAll(".admin-tab-panel");
  if (existingPanels?.length) {
    existingPanels[existingPanels.length - 1].after(panel);
  } else {
    modalCard?.appendChild(panel);
  }

  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "admin-tab-btn";
  btn.dataset.adminTab = "points_system";
  btn.textContent = "成长体系";
  tabsWrap.appendChild(btn);
}

function ensureDeployTabExists() {
  if (document.querySelector('.admin-tab-btn[data-admin-tab="deploy"]')) return;
  const tabsWrap = document.querySelector(".admin-tabs");
  if (!tabsWrap) return;

  const panel = document.createElement("div");
  panel.className = "admin-tab-panel";
  panel.id = "adminTab-deploy";
  panel.innerHTML = `
    <div class="section-title">系统升级</div>
    <div class="admin-grid">
      <div>
        <div class="label">打包版本号（可选）</div>
        <input id="deployPackageVersionInput" class="custom-textarea single-input" placeholder="例如 2026.03.31-local" />
      </div>
      <div>
        <div class="label">本地打包</div>
        <div class="modal-actions">
          <button id="downloadUpgradePackageBtn" class="secondary-btn" type="button">下载升级包</button>
          <button id="downloadFullPackageBtn" class="secondary-btn" type="button">下载整站包</button>
          <button id="downloadFullSlimPackageBtn" class="secondary-btn" type="button" title="不含 content_published、data、content_builds、jobs、SQLite 等">
            下载整站精简包
          </button>
        </div>
      </div>
    </div>
    <div class="modal-actions">
      <button id="generateUpgradeCmdBtn" class="secondary-btn" type="button">生成升级包命令</button>
      <button id="generateFullCmdBtn" class="secondary-btn" type="button">生成整站包命令</button>
      <button id="generateFullSlimCmdBtn" class="secondary-btn" type="button">生成整站精简包命令</button>
      <button id="downloadChangedPackageBtn" class="primary-btn" type="button">按最近改动下载包</button>
    </div>
    <div id="deployPackageCommandBox" class="result-box">尚未生成打包命令。</div>
    <div class="admin-grid">
      <div>
        <div class="label">上传升级包（zip）</div>
        <input id="deployUploadInput" type="file" accept=".zip,application/zip" />
      </div>
      <div>
        <div class="label">已上传包</div>
        <select id="deployUploadSelect"></select>
      </div>
    </div>
    <div class="modal-actions">
      <button id="deployUploadBtn" class="secondary-btn" type="button">上传</button>
      <button id="deployApplyBtn" class="primary-btn" type="button">应用升级</button>
      <button id="deployRollbackBtn" class="secondary-btn" type="button">回滚</button>
      <button id="deployRefreshBtn" class="secondary-btn" type="button">刷新状态</button>
    </div>
    <div id="deployStatusBox" class="result-box">尚未读取部署状态。</div>
    <div class="section-title">数据备份与恢复</div>
    <div class="modal-actions">
      <button id="createDataBackupBtn" class="primary-btn" type="button">创建数据备份</button>
      <button id="refreshDataBackupBtn" class="secondary-btn" type="button">刷新备份列表</button>
      <button id="downloadDataBackupBtn" class="secondary-btn" type="button">下载选中备份(zip)</button>
      <button id="restoreDataBackupBtn" class="secondary-btn" type="button">恢复选中备份</button>
      <button id="pruneDataBackupBtn" class="secondary-btn" type="button">清理旧备份</button>
      <button id="saveDataBackupConfigBtn" class="secondary-btn" type="button">保存保留设置</button>
      <button id="runAutoBackupNowBtn" class="primary-btn" type="button">立即执行自动备份(测试)</button>
    </div>
    <div class="admin-grid">
      <div>
        <div class="label">保留最近 N 份</div>
        <input id="dataBackupKeepCountInput" type="number" min="1" max="200" class="custom-textarea single-input" value="20" />
      </div>
      <div>
        <div class="label">自动备份</div>
        <label style="display:flex; align-items:center; gap:8px; margin-top:8px;">
          <input id="autoBackupEnabledInput" type="checkbox" />
          <span>开启每天定时自动备份</span>
        </label>
      </div>
      <div>
        <div class="label">自动备份时间（24小时）</div>
        <div style="display:flex; gap:8px;">
          <input id="autoBackupHourInput" type="number" min="0" max="23" class="custom-textarea single-input" value="3" />
          <input id="autoBackupMinuteInput" type="number" min="0" max="59" class="custom-textarea single-input" value="0" />
        </div>
      </div>
      <div>
        <div class="label">可恢复备份</div>
        <select id="dataBackupSelect"></select>
      </div>
    </div>
    <div id="dataBackupStatusBox" class="result-box">尚未读取数据备份。</div>
    <div class="section-title">管理操作审计日志</div>
    <div class="modal-actions">
      <button id="refreshAuditLogBtn" class="secondary-btn" type="button">刷新审计日志</button>
    </div>
    <div id="auditLogBox" class="result-box">尚未读取审计日志。</div>
    <div class="section-title">系统密钥管理（OpenAI）</div>
    <div class="admin-grid">
      <div style="grid-column:1 / -1;">
        <div class="label">GPT Key（保存到服务器本地，仅管理员可见）</div>
        <input id="systemOpenAiKeyInput" type="password" class="custom-textarea single-input" placeholder="粘贴 sk-... 新密钥" />
      </div>
    </div>
    <div class="modal-actions">
      <button id="saveSystemOpenAiKeyBtn" class="primary-btn" type="button">保存 GPT Key</button>
      <button id="clearSystemOpenAiKeyBtn" class="secondary-btn" type="button">清空已保存 Key</button>
      <button id="refreshSystemOpenAiKeyBtn" class="secondary-btn" type="button">刷新状态</button>
    </div>
    <div id="systemOpenAiKeyStatusBox" class="result-box">尚未读取密钥状态。</div>
  `;
  const modalCard = document.querySelector(".modal-card-admin");
  const existingPanels = modalCard?.querySelectorAll(".admin-tab-panel");
  if (existingPanels?.length) {
    existingPanels[existingPanels.length - 1].after(panel);
  } else {
    modalCard?.appendChild(panel);
  }

  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "admin-tab-btn";
  btn.dataset.adminTab = "deploy";
  btn.textContent = "版本升级";
  tabsWrap.appendChild(btn);
}

function ensurePublishedTabExists() {
  if (document.querySelector('.admin-tab-btn[data-admin-tab="published"]'))
    return;

  const tabsWrap = document.querySelector(".admin-tabs");
  const existingPanel = document.getElementById("adminTab-published");

  if (!existingPanel) {
    const panel = document.createElement("div");
    panel.className = "admin-tab-panel";
    panel.id = "adminTab-published";
    panel.innerHTML = `
      <div class="admin-two-col">
        <div class="admin-left-col">
          <div class="section-title">已发布内容查询</div>

          <div class="admin-grid">
            <div>
              <div class="label">内容版本</div>
              <select id="publishedVersionSelect"></select>
            </div>
            <div>
              <div class="label">内容语言</div>
              <select id="publishedLangSelect"></select>
            </div>
          </div>

          <div class="modal-actions">
            <button id="loadPublishedOverviewBtn" class="primary-btn" type="button">读取发布概览</button>
          </div>

          <div class="section-title">整本发布</div>
          <div id="publishFeatureInfoBox" class="result-box"></div>
          <div class="admin-grid">
            <label style="display:flex; align-items:center; gap:8px;">
              <input id="publishOnlyChangedToggle" type="checkbox" checked />
              <span>仅发布改动</span>
            </label>
          </div>
          <div class="modal-actions">
            <button id="publishByVersionBtn" class="secondary-btn" type="button">按版本整本发布</button>
            <button id="publishByLangBtn" class="secondary-btn" type="button">按语言整本发布</button>
            <button id="publishByVersionLangBtn" class="secondary-btn" type="button">按版本+语言整本发布</button>
            <button id="publishAllVersionLangBtn" class="primary-btn" type="button">所有语言版本一键发布</button>
            <button id="autoRepublishMissingAllBtn" class="primary-btn" type="button">一键全部自动查漏补发</button>
            <button id="stopPublishedActionBtn" class="secondary-btn" type="button">停止当前发布</button>
            <button id="previewPublishBulkBtn" class="secondary-btn" type="button">增量预览</button>
            <button id="exportPublishChangesJsonBtn" class="secondary-btn" type="button">导出改动 JSON</button>
            <button id="exportPublishChangesCsvBtn" class="secondary-btn" type="button">导出改动 CSV</button>
          </div>
          <div id="publishedBulkActionStatus" class="result-box">尚未执行整本发布。</div>
          <div id="publishedLastActionBox" class="result-box">最近一次执行：尚无记录。</div>
          <div class="section-title">执行历史（最近 10 次）</div>
          <div id="publishedHistoryBox" class="result-box">暂无历史。</div>

          <div class="section-title">发布统计</div>
          <div id="publishedSummaryBox" class="result-box">尚未读取。</div>

          <div class="section-title">卷 / 章节列表</div>
          <div id="publishedBooksBox" class="admin-preview-result">
            <div class="empty-state">暂无数据。</div>
          </div>
        </div>

        <div class="admin-right-col">
          <div class="section-title">已发布章节详情</div>

          <div class="admin-grid">
            <div>
              <div class="label">书卷</div>
              <input id="publishedDetailBookInput" class="custom-textarea single-input" placeholder="例如 GEN" />
            </div>
            <div>
              <div class="label">章节</div>
              <input id="publishedDetailChapterInput" type="number" class="custom-textarea single-input" placeholder="例如 1" />
            </div>
          </div>

          <div class="modal-actions">
            <button id="loadPublishedChapterBtn" class="secondary-btn" type="button">载入已发布章节</button>
            <button id="deletePublishedChapterBtn" class="secondary-btn" type="button">删除已发布章节</button>
            <button id="savePublishedChapterRevisionBtn" class="primary-btn" type="button">保存并发布（已审核）</button>
          </div>

          <div class="section-title">章节 JSON（可编辑）</div>
          <p class="admin-hint-line">修改 <code>segments</code>、<code>questions</code>、<code>theme</code> 等字段后，点击下方保存即可覆盖线上该章（与「测试生成」保存同一套发布逻辑）。</p>
          <textarea
            id="publishedChapterJsonEditor"
            class="admin-json-editor"
            spellcheck="false"
            placeholder="先选择左侧「内容版本 / 语言」，输入书卷与章节，再点「载入已发布章节」…"
          ></textarea>
          <div class="label">审核备注（可选，写入操作日志）</div>
          <input
            id="publishedChapterReviewNote"
            class="custom-textarea single-input"
            type="text"
            placeholder="例如：校对第2段问题措辞"
          />
          <div id="publishedChapterEditorStatus" class="result-box">尚未载入。</div>
        </div>
      </div>
    `;

    const modalCard = document.querySelector(".modal-card-admin");
    const existingPanels = modalCard?.querySelectorAll(".admin-tab-panel");
    if (existingPanels?.length) {
      existingPanels[existingPanels.length - 1].after(panel);
    }
  }

  if (tabsWrap) {
    const btn = document.createElement("button");
    btn.className = "admin-tab-btn";
    btn.type = "button";
    btn.dataset.adminTab = "published";
    btn.textContent = "已发布内容";
    tabsWrap.appendChild(btn);
  }
}

function ensureContentVersionsManagerTabExists() {
  if (
    document.querySelector(
      '.admin-tab-btn[data-admin-tab="content_versions_menu"]'
    )
  ) {
    return;
  }

  const tabsWrap = document.querySelector(".admin-tabs");
  const panel = document.createElement("div");
  panel.className = "admin-tab-panel";
  panel.id = "adminTab-content_versions_menu";
  panel.innerHTML = `
    <div class="section-title">内容版本 · 前台菜单</div>
    <p class="admin-hint-muted">
      仅「启用」且勾选「前台菜单显示」的版本会出现在首页工具栏「版本」面板。关闭「前台菜单显示」后，用户无法在主页切换该类型，后台生成与规则编辑仍可使用该版本。
    </p>
    <div id="contentVersionsRows" class="content-versions-rows"></div>
    <div class="modal-actions" style="margin-top:14px;">
      <button id="reloadContentVersionsBtn" class="secondary-btn" type="button">重新载入</button>
      <button id="saveContentVersionsBtn" class="primary-btn" type="button">保存内容版本</button>
    </div>
    <div id="contentVersionsSaveResult" class="admin-preview-box" style="margin-top:12px;">尚未保存。</div>
  `;

  const modalCard = document.querySelector(".modal-card-admin");
  const existingPanels = modalCard?.querySelectorAll(".admin-tab-panel");
  if (existingPanels?.length) {
    existingPanels[existingPanels.length - 1].after(panel);
  }

  if (tabsWrap) {
    const btn = document.createElement("button");
    btn.className = "admin-tab-btn";
    btn.type = "button";
    btn.dataset.adminTab = "content_versions_menu";
    btn.textContent = "内容版本菜单";
    tabsWrap.appendChild(btn);
  }
}

function ensureScriptureVersionManagerTabExists() {
  if (
    document.querySelector(
      '.admin-tab-btn[data-admin-tab="scripture_versions"]'
    )
  ) {
    return;
  }

  const tabsWrap = document.querySelector(".admin-tabs");

  const panel = document.createElement("div");
  panel.className = "admin-tab-panel";
  panel.id = "adminTab-scripture_versions";
  panel.innerHTML = `
    <div class="admin-two-col">
      <div class="admin-left-col">
        <div class="section-title">圣经版本列表</div>
        <div class="modal-actions">
          <button id="refreshScriptureVersionsBtn" class="secondary-btn" type="button">刷新版本</button>
          <button id="newScriptureVersionBtn" class="secondary-btn" type="button">新建版本</button>
        </div>
        <div id="scriptureVersionsListBox" class="admin-preview-result">
          <div class="empty-state">暂无数据。</div>
        </div>
      </div>

      <div class="admin-right-col">
        <div class="section-title">圣经版本编辑</div>

        <div class="admin-grid">
          <div>
            <div class="label">ID</div>
            <input id="svId" class="custom-textarea single-input" placeholder="例如 web_en" />
          </div>
          <div>
            <div class="label">标签</div>
            <input id="svLabel" class="custom-textarea single-input" placeholder="例如 WEB English" />
          </div>
          <div>
            <div class="label">语言</div>
            <input id="svLang" class="custom-textarea single-input" placeholder="例如 en" />
          </div>
          <div>
            <div class="label">sourceType</div>
            <input id="svSourceType" class="custom-textarea single-input" placeholder="usfx" />
          </div>
          <div>
            <div class="label">sourceFile</div>
            <input id="svSourceFile" class="custom-textarea single-input" placeholder="data/eng-web.usfx.xml" />
          </div>
          <div>
            <div class="label">sortOrder</div>
            <input id="svSortOrder" type="number" class="custom-textarea single-input" placeholder="10" />
          </div>
          <div style="grid-column:1 / -1;">
            <div class="label">description</div>
            <input id="svDescription" class="custom-textarea single-input" placeholder="版本说明" />
          </div>
        </div>

        <div class="admin-grid" style="margin-top:10px;">
          <label><input id="svEnabled" type="checkbox" checked /> enabled</label>
          <label><input id="svUiEnabled" type="checkbox" checked /> uiEnabled</label>
          <label><input id="svContentEnabled" type="checkbox" checked /> contentEnabled</label>
          <label><input id="svScriptureEnabled" type="checkbox" checked /> scriptureEnabled</label>
        </div>

        <div class="admin-grid" style="margin-top:10px;">
          <div>
            <div class="label">contentMode</div>
            <input id="svContentMode" class="custom-textarea single-input" placeholder="native" />
          </div>
        </div>

        <div class="modal-actions">
          <button id="saveScriptureVersionBtn" class="primary-btn" type="button">保存版本</button>
          <button id="deleteScriptureVersionBtn" class="secondary-btn" type="button">删除当前版本</button>
        </div>

        <div class="section-title">结果</div>
        <div id="scriptureVersionEditorResult" class="admin-preview-box">尚未操作。</div>
      </div>
    </div>
  `;

  const modalCard = document.querySelector(".modal-card-admin");
  const existingPanels = modalCard?.querySelectorAll(".admin-tab-panel");
  if (existingPanels?.length) {
    existingPanels[existingPanels.length - 1].after(panel);
  }

  if (tabsWrap) {
    const btn = document.createElement("button");
    btn.className = "admin-tab-btn";
    btn.type = "button";
    btn.dataset.adminTab = "scripture_versions";
    btn.textContent = "圣经版本";
    tabsWrap.appendChild(btn);
  }
}

async function initRuleEditorTab() {
  const versionSelect = document.getElementById("adminRuleVersionSelect");
  if (!versionSelect) return;

  versionSelect.innerHTML = (adminState.bootstrap?.contentVersions || [])
    .map(
      (item) =>
        `<option value="${escapeHtml(item.id)}">${escapeHtml(
          item.label
        )}</option>`
    )
    .join("");

  versionSelect.value = adminState.currentRuleVersion;

  versionSelect.onchange = async () => {
    adminState.currentRuleVersion = versionSelect.value;
    await loadRuleIntoEditor(adminState.currentRuleVersion);
  };

  const saveRuleBtn = document.getElementById("saveRuleBtn");
  const reloadRuleBtn = document.getElementById("reloadRuleBtn");

  if (saveRuleBtn) {
    saveRuleBtn.onclick = async () => {
      await saveRuleFromEditor();
    };
  }

  if (reloadRuleBtn) {
    reloadRuleBtn.onclick = async () => {
      await loadRuleIntoEditor(adminState.currentRuleVersion);
    };
  }

  await loadRuleIntoEditor(adminState.currentRuleVersion);
}

async function loadRuleIntoEditor(versionId) {
  const res = await fetch(
    `/api/admin/rule?version=${encodeURIComponent(versionId)}`,
    {
      cache: "no-store",
    }
  );
  const data = await res.json();

  if (!res.ok) {
    throw new Error(data.error || "读取规则失败");
  }

  adminState.currentRuleConfig = data;
  fillRuleEditor(data);
  renderRulePreview(data);
}

function fillRuleEditor(ruleConfig) {
  const base = ruleConfig?.baseRules || {};
  const langProfiles = ruleConfig?.languageProfiles || {};

  document.getElementById("adminRuleLabel").value = ruleConfig?.label || "";
  document.getElementById("adminRuleScene").value = ruleConfig?.scene || "";
  document.getElementById("adminRuleTemplate").value =
    ruleConfig?.template || "";
  document.getElementById("adminRuleStyleTags").value = (
    ruleConfig?.styleTags || []
  ).join(", ");

  document.getElementById("adminMinQuestions").value =
    base.minQuestionsPerSegment ?? 2;
  document.getElementById("adminMaxQuestions").value =
    base.maxQuestionsPerSegment ?? 4;
  document.getElementById("adminChapterQuestionMin").value =
    base.chapterQuestionMin ?? 15;
  document.getElementById("adminChapterQuestionMax").value =
    base.chapterQuestionMax ?? 20;

  document.getElementById("adminLeaderHint").checked = !!base.leaderHint;
  document.getElementById("adminAvoidRepeat").checked = !!base.avoidRepeat;
  document.getElementById("adminAllowLightApplication").checked =
    !!base.allowLightApplication;
  document.getElementById("adminAllowGospelEmphasis").checked =
    !!base.allowGospelEmphasis;
  document.getElementById("adminAllowChildrenTone").checked =
    !!base.allowChildrenTone;
  document.getElementById("adminAllowYouthTone").checked =
    !!base.allowYouthTone;
  document.getElementById("adminAllowCoupleTone").checked =
    !!base.allowCoupleTone;
  document.getElementById("adminAllowWorkplaceTone").checked =
    !!base.allowWorkplaceTone;

  document.getElementById("adminPromptZh").value =
    langProfiles?.zh?.customPrompt || "";
  document.getElementById("adminPromptEn").value =
    langProfiles?.en?.customPrompt || "";
  document.getElementById("adminPromptEs").value =
    langProfiles?.es?.customPrompt || "";

  document.getElementById("adminSystemPromptOverride").value =
    ruleConfig?.systemPromptOverride || "";
}

function collectRuleFromEditor() {
  return {
    id: adminState.currentRuleVersion,
    label: document.getElementById("adminRuleLabel")?.value.trim() || "",
    scene: document.getElementById("adminRuleScene")?.value.trim() || "",
    template: document.getElementById("adminRuleTemplate")?.value.trim() || "",
    styleTags: (document.getElementById("adminRuleStyleTags")?.value || "")
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean),
    baseRules: {
      leaderHint: !!document.getElementById("adminLeaderHint")?.checked,
      avoidRepeat: !!document.getElementById("adminAvoidRepeat")?.checked,
      allowLightApplication: !!document.getElementById(
        "adminAllowLightApplication"
      )?.checked,
      allowGospelEmphasis: !!document.getElementById("adminAllowGospelEmphasis")
        ?.checked,
      allowChildrenTone: !!document.getElementById("adminAllowChildrenTone")
        ?.checked,
      allowYouthTone: !!document.getElementById("adminAllowYouthTone")?.checked,
      allowCoupleTone: !!document.getElementById("adminAllowCoupleTone")
        ?.checked,
      allowWorkplaceTone: !!document.getElementById("adminAllowWorkplaceTone")
        ?.checked,
      minQuestionsPerSegment: Number(
        document.getElementById("adminMinQuestions")?.value || 2
      ),
      maxQuestionsPerSegment: Number(
        document.getElementById("adminMaxQuestions")?.value || 4
      ),
      chapterQuestionMin: Number(
        document.getElementById("adminChapterQuestionMin")?.value || 15
      ),
      chapterQuestionMax: Number(
        document.getElementById("adminChapterQuestionMax")?.value || 20
      ),
    },
    languageProfiles: {
      zh: {
        customPrompt:
          document.getElementById("adminPromptZh")?.value.trim() || "",
      },
      en: {
        customPrompt:
          document.getElementById("adminPromptEn")?.value.trim() || "",
      },
      es: {
        customPrompt:
          document.getElementById("adminPromptEs")?.value.trim() || "",
      },
    },
    systemPromptOverride:
      document.getElementById("adminSystemPromptOverride")?.value.trim() || "",
  };
}

async function saveRuleFromEditor() {
  const nextRule = collectRuleFromEditor();

  const res = await fetch("/api/admin/rule/save", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      version: adminState.currentRuleVersion,
      ruleConfig: nextRule,
    }),
  });

  const data = await res.json();
  if (!res.ok) {
    alert(data.error || "保存规则失败");
    return;
  }

  adminState.currentRuleConfig = nextRule;
  renderRulePreview(nextRule);
  alert("规则已保存。");
}

function renderRulePreview(ruleConfig) {
  const el = document.getElementById("adminRulePreview");
  if (!el) return;
  el.textContent = JSON.stringify(ruleConfig, null, 2);
}

async function initTestGenerateTab() {
  renderTestVersionOptions();
  renderTestLangOptions();
  renderTestBookOptions();
  renderTestChapterOptions();
  ensureJobPanelExists();

  const testBookSelect = document.getElementById("testBookSelect");
  const runTestGenerateBtn = document.getElementById("runTestGenerateBtn");
  const saveTestResultBtn = document.getElementById("saveTestResultBtn");
  const createBookJobBtn = document.getElementById("createBookJobBtn");
  const createOldJobBtn = document.getElementById("createOldJobBtn");
  const createNewJobBtn = document.getElementById("createNewJobBtn");
  const createBibleJobBtn = document.getElementById("createBibleJobBtn");
  const refreshJobsBtn = document.getElementById("refreshJobsBtn");
  const mergePartialJobsBtn = document.getElementById("mergePartialJobsBtn");

  if (testBookSelect) {
    testBookSelect.onchange = () => {
      renderTestChapterOptions();
      syncRangeInputsWithSelectedBook();
    };
  }

  if (runTestGenerateBtn) {
    runTestGenerateBtn.onclick = async () => {
      await runTestGenerate();
    };
  }

  if (saveTestResultBtn) {
    saveTestResultBtn.onclick = async () => {
      await saveTestResultToContent();
    };
  }

  if (createBookJobBtn) {
    createBookJobBtn.onclick = async () => {
      await createBulkJobFromUI("book");
    };
  }

  if (createOldJobBtn) {
    createOldJobBtn.onclick = async () => {
      await createBulkJobFromUI("old_testament");
    };
  }

  if (createNewJobBtn) {
    createNewJobBtn.onclick = async () => {
      await createBulkJobFromUI("new_testament");
    };
  }

  if (createBibleJobBtn) {
    createBibleJobBtn.onclick = async () => {
      await createBulkJobFromUI("bible");
    };
  }

  if (refreshJobsBtn) {
    refreshJobsBtn.onclick = async () => {
      await refreshJobsList();
    };
  }

  if (mergePartialJobsBtn) {
    mergePartialJobsBtn.onclick = async () => {
      await mergePublishAllPartialBuildsFromUI();
    };
  }

  syncRangeInputsWithSelectedBook();
  await refreshJobsList();
}

function ensureJobPanelExists() {
  if (document.getElementById("jobManagerWrap")) return;

  const anchor = document.getElementById("saveTestResultStatus");
  if (!anchor) return;

  const wrap = document.createElement("div");
  wrap.id = "jobManagerWrap";
  wrap.innerHTML = `
    <div class="section-title">批量生成任务</div>

    <div class="admin-grid" style="margin-bottom:12px;">
      <div>
        <div class="label">起始章（仅整卷范围生效）</div>
        <input id="jobStartChapterInput" type="number" class="custom-textarea single-input" placeholder="例如 1" />
      </div>
      <div>
        <div class="label">结束章（仅整卷范围生效）</div>
        <input id="jobEndChapterInput" type="number" class="custom-textarea single-input" placeholder="例如 10" />
      </div>
    </div>

    <div class="result-box" style="margin-bottom:12px;">
      <label style="display:flex;align-items:flex-start;gap:8px;cursor:pointer;">
        <input id="jobSkipPublishOverwriteCheck" type="checkbox" style="margin-top:3px;" />
        <span>批量任务在勾选「自动合并发布」时：若该章在读者端<strong>已有发布文件</strong>且与本次生成<strong>内容不一致</strong>，则<strong>跳过覆盖</strong>（仍会写入 build，便于之后手动合并）。与线上一致时会自动跳过发布。</span>
      </label>
    </div>

    <div class="modal-actions">
      <button id="createBookJobBtn" class="secondary-btn" type="button">生成整卷 / 范围</button>
      <button id="createOldJobBtn" class="secondary-btn" type="button">生成旧约</button>
      <button id="createNewJobBtn" class="secondary-btn" type="button">生成新约</button>
      <button id="createBibleJobBtn" class="secondary-btn" type="button">生成整本</button>
      <button id="refreshJobsBtn" class="secondary-btn" type="button">刷新任务</button>
      <button id="mergePartialJobsBtn" class="primary-btn" type="button">合并发布未完成任务的产物</button>
    </div>

    <div class="section-title">任务状态</div>
    <div id="jobCreateStatus" class="result-box">尚未创建任务。</div>

    <div class="section-title">任务列表</div>
    <div id="jobsListBox" class="admin-preview-result">
      <div class="empty-state">暂无任务。</div>
    </div>
  `;

  anchor.parentElement?.appendChild(wrap);
}

function renderTestVersionOptions() {
  const el = document.getElementById("testVersionSelect");
  if (!el) return;

  el.innerHTML = (adminState.bootstrap?.contentVersions || [])
    .map(
      (item) =>
        `<option value="${escapeHtml(item.id)}">${escapeHtml(
          item.label
        )}</option>`
    )
    .join("");

  el.value = adminState.currentRuleVersion || "default";
}

function renderTestLangOptions() {
  const el = document.getElementById("testLangSelect");
  if (!el) return;

  el.innerHTML = (adminState.bootstrap?.languages || [])
    .filter((x) => x.enabled)
    .map(
      (item) =>
        `<option value="${escapeHtml(item.id)}">${escapeHtml(
          item.label
        )}</option>`
    )
    .join("");

  el.value = "zh";
}

function renderTestBookOptions() {
  const el = document.getElementById("testBookSelect");
  if (!el) return;

  el.innerHTML = (adminState.bootstrap?.books || [])
    .map(
      (book) =>
        `<option value="${escapeHtml(book.bookId)}">${escapeHtml(
          book.bookCn
        )}</option>`
    )
    .join("");

  el.value = "GEN";
}

function renderTestChapterOptions() {
  const bookSelect = document.getElementById("testBookSelect");
  const chapterSelect = document.getElementById("testChapterSelect");
  if (!bookSelect || !chapterSelect) return;

  const book = (adminState.bootstrap?.books || []).find(
    (x) => x.bookId === bookSelect.value
  );

  const chapterCount = Number(book?.chapters || 1);
  chapterSelect.innerHTML = Array.from({ length: chapterCount }, (_, i) => {
    const n = i + 1;
    return `<option value="${n}">${n}</option>`;
  }).join("");

  chapterSelect.value = "1";
}

function syncRangeInputsWithSelectedBook() {
  const startInput = document.getElementById("jobStartChapterInput");
  const endInput = document.getElementById("jobEndChapterInput");
  const bookId = document.getElementById("testBookSelect")?.value;
  const book = (adminState.bootstrap?.books || []).find(
    (x) => x.bookId === bookId
  );

  if (!book || !startInput || !endInput) return;

  if (!startInput.value) startInput.value = "1";
  if (!endInput.value) endInput.value = String(book.chapters || 1);

  startInput.min = "1";
  endInput.min = "1";
  startInput.max = String(book.chapters || 1);
  endInput.max = String(book.chapters || 1);
}

async function runTestGenerate() {
  const version = document.getElementById("testVersionSelect")?.value;
  const lang = document.getElementById("testLangSelect")?.value;
  const bookId = document.getElementById("testBookSelect")?.value;
  const chapter = Number(
    document.getElementById("testChapterSelect")?.value || 1
  );

  const statusEl = document.getElementById("testGenerateStatus");
  const resultEl = document.getElementById("testGenerateResult");
  const saveStatusEl = document.getElementById("saveTestResultStatus");

  if (statusEl) statusEl.textContent = "正在生成，请稍候...";
  if (saveStatusEl) saveStatusEl.textContent = "尚未保存。";
  if (resultEl) {
    resultEl.innerHTML = `<div class="empty-state">正在请求模型生成内容...</div>`;
  }

  const res = await fetch("/api/admin/test-generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      version,
      lang,
      bookId,
      chapter,
      primaryScriptureVersionId: state.frontState.primaryScriptureVersionId || "",
    }),
  });

  const data = await res.json();

  if (!res.ok) {
    if (statusEl)
      statusEl.textContent = `生成失败：${data.error || "未知错误"}`;
    if (resultEl) {
      resultEl.innerHTML = `<div class="empty-state">生成失败：${escapeHtml(
        data.error || "未知错误"
      )}</div>`;
    }
    return;
  }

  adminState.testResult = data;

  if (statusEl) {
    statusEl.textContent = `生成成功：${
      data.bookLabel || bookId
    } ${chapter} 章｜${data.versionLabel || version}｜${lang}`;
  }

  renderTestGenerateResult(data);
}

function renderTestGenerateResult(data) {
  const el = document.getElementById("testGenerateResult");
  if (!el) return;

  const repeatedWordsText = (data.repeatedWords || [])
    .map((x) => `${x.word}${x.count ? ` × ${x.count}` : ""}`)
    .join("　");

  el.innerHTML = `
    <div class="test-result-title">${escapeHtml(data.title || "测试结果")}</div>
    <div class="test-result-line"><strong>主题：</strong>${escapeHtml(
      data.theme || "—"
    )}</div>
    <div class="test-result-line"><strong>重复词：</strong>${escapeHtml(
      repeatedWordsText || "—"
    )}</div>

    ${(data.segments || [])
      .map(
        (seg) => `
          <div class="test-result-seg">
            <h4>${escapeHtml(seg.title || "未命名段落")}</h4>
            <div class="test-result-line">
              <strong>范围：</strong>${escapeHtml(
                `${seg.rangeStart || "?"}-${seg.rangeEnd || "?"}`
              )}
            </div>
            <ul>
              ${(seg.questions || [])
                .map((q) => `<li>${escapeHtml(q)}</li>`)
                .join("")}
            </ul>
          </div>
        `
      )
      .join("")}

    <div class="test-result-line"><strong>结尾：</strong>${escapeHtml(
      data.closing || "—"
    )}</div>
  `;
}

async function saveTestResultToContent() {
  const saveStatusEl = document.getElementById("saveTestResultStatus");

  if (!adminState.testResult) {
    if (saveStatusEl) saveStatusEl.textContent = "请先测试生成，再保存。";
    return;
  }

  if (saveStatusEl) saveStatusEl.textContent = "正在保存此章内容并合并发布...";

  const token = getAuthToken();
  const res = await fetch("/api/admin/save-test-result", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify({
      studyContent: adminState.testResult,
      reviewNote: "",
    }),
  });

  const data = await res.json();

  if (!res.ok) {
    if (saveStatusEl) {
      saveStatusEl.textContent = `保存失败：${data.error || "未知错误"}`;
    }
    return;
  }

  if (saveStatusEl) {
    saveStatusEl.textContent = `保存成功：build = ${data.buildId}（已合并发布）`;
  }

  const sameVersion =
    state.frontState.contentVersion === adminState.testResult.version;
  const sameLang =
    state.frontState.contentLang === adminState.testResult.contentLang;
  const sameBook = state.frontState.bookId === adminState.testResult.bookId;
  const sameChapter =
    Number(state.frontState.chapter) === Number(adminState.testResult.chapter);

  if (sameVersion && sameLang && sameBook && sameChapter) {
    await loadStudyContent();
    renderStudyContent();
  }
}

function collectJobPayload(scope) {
  const version =
    document.getElementById("testVersionSelect")?.value || "default";
  const lang = document.getElementById("testLangSelect")?.value || "zh";
  const bookId = document.getElementById("testBookSelect")?.value || "GEN";
  const startChapter = Number(
    document.getElementById("jobStartChapterInput")?.value || 0
  );
  const endChapter = Number(
    document.getElementById("jobEndChapterInput")?.value || 0
  );

  const payload = {
    scope,
    versionMode: "single",
    version,
    langMode: "single",
    lang,
    bookId,
    autoPublish: true,
    skipPublishOverwrite:
      document.getElementById("jobSkipPublishOverwriteCheck")?.checked === true,
  };

  if (scope === "book" && startChapter > 0 && endChapter > 0) {
    payload.startChapter = startChapter;
    payload.endChapter = endChapter;
  }

  return payload;
}

async function createBulkJobFromUI(scope) {
  const statusEl = document.getElementById("jobCreateStatus");
  const payload = collectJobPayload(scope);

  if (statusEl) statusEl.textContent = "正在创建任务...";

  const res = await fetch("/api/admin/job/create", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const data = await res.json();
  if (!res.ok) {
    if (statusEl)
      statusEl.textContent = `创建失败：${data.error || "未知错误"}`;
    return;
  }

  const scopeLabelMap = {
    book: "整卷/范围",
    old_testament: "旧约",
    new_testament: "新约",
    bible: "整本",
  };

  const rangeText =
    payload.scope === "book" && payload.startChapter && payload.endChapter
      ? `｜范围：${payload.startChapter}-${payload.endChapter}章`
      : "";

  if (statusEl) {
    statusEl.textContent = `任务已创建：${data.job.id}｜范围：${
      scopeLabelMap[data.job.scope] || data.job.scope
    }${rangeText}｜总数：${data.job.total}`;
  }

  await refreshJobsList(true);
}

async function retryFailedJob(jobId) {
  const statusEl = document.getElementById("jobCreateStatus");
  if (statusEl) statusEl.textContent = `正在创建失败重跑任务：${jobId}`;

  const res = await fetch(
    `/api/admin/job/${encodeURIComponent(jobId)}/retry-failed`,
    {
      method: "POST",
    }
  );

  const data = await res.json();
  if (!res.ok) {
    if (statusEl)
      statusEl.textContent = `重跑失败：${data.error || "未知错误"}`;
    return;
  }

  if (statusEl) {
    statusEl.textContent = `失败章节重跑任务已创建：${data.job.id}｜来源：${jobId}｜总数：${data.job.total}`;
  }

  await refreshJobsList(true);
}

async function refreshJobsList(forceRefreshFront = false) {
  const box = document.getElementById("jobsListBox");
  if (!box) return;

  const res = await fetch("/api/admin/jobs", { cache: "no-store" });
  const parsed = await readJsonResponse(res);

  if (parsed.parseError) {
    box.innerHTML = `<div class="empty-state">读取任务失败：服务端返回非 JSON（HTTP ${escapeHtml(
      String(parsed.status)
    )}），多为网关超时或服务暂时不可用。</div>`;
    return;
  }

  const data = parsed.data;

  if (!res.ok) {
    box.innerHTML = `<div class="empty-state">读取任务失败：${escapeHtml(
      data?.error || "未知错误"
    )}</div>`;
    return;
  }

  const jobs = data.jobs || [];
  const snapshotKey = jobs
    .map(
      (job) =>
        `${job.id}:${job.status}:${job.done}:${job.progressText}:${
          job.buildId
        }:${job.completionSummary || ""}:${job.errors?.length || 0}`
    )
    .join("|");

  const snapshotChanged = snapshotKey !== adminState.lastJobsSnapshotKey;
  adminState.lastJobsSnapshotKey = snapshotKey;

  renderJobsList(jobs);

  if (snapshotChanged || forceRefreshFront) {
    await maybeRefreshFrontAfterJobs(jobs);
  }
}

function getScopeLabel(scope) {
  const map = {
    chapter: "当前章",
    book: "整卷/范围",
    old_testament: "旧约",
    new_testament: "新约",
    bible: "整本",
  };
  return map[scope] || scope;
}

function formatJobIsoLocal(iso) {
  const s = String(iso || "").trim();
  if (!s) return "—";
  const t = Date.parse(s);
  if (Number.isNaN(t)) return s;
  return new Date(t).toLocaleString("zh-CN", { hour12: false });
}

/** 执行时长：从 startIso 到 endIso；endIso 缺省且 useNow 为 true 时用 nowMs */
function formatJobDuration(startIso, endIso, { useNow = false, nowMs = Date.now() } = {}) {
  const a = Date.parse(String(startIso || ""));
  let end;
  if (endIso != null && String(endIso).trim() !== "") {
    end = Date.parse(String(endIso));
  } else if (useNow) {
    end = nowMs;
  } else {
    return "—";
  }
  if (Number.isNaN(a) || Number.isNaN(end) || end < a) return "—";
  const sec = Math.round((end - a) / 1000);
  if (sec < 60) return `${sec} 秒`;
  const m = Math.floor(sec / 60);
  const s2 = sec % 60;
  if (m < 60) return s2 ? `${m} 分 ${s2} 秒` : `${m} 分`;
  const h = Math.floor(m / 60);
  const m2 = m % 60;
  return m2 ? `${h} 小时 ${m2} 分` : `${h} 小时`;
}

function renderJobsList(jobs) {
  const box = document.getElementById("jobsListBox");
  if (!box) return;

  if (!jobs.length) {
    box.innerHTML = `<div class="empty-state">暂无任务。</div>`;
    return;
  }

  box.innerHTML = jobs
    .map((job) => {
      const canCancel = job.status === "queued" || job.status === "running";
      const mergedPublished =
        job.status === "completed" &&
        String(job.progressText || "").includes("自动合并发布");
      const errorCount = Number(job.errors?.length || 0);
      const canRetryFailed = job.status === "completed" && errorCount > 0;
      const doneNum = Number(job.done || 0);
      const canMergePublishFromBuild =
        Boolean(job.buildId) &&
        Array.isArray(job.targets) &&
        job.targets.length > 0 &&
        doneNum > 0 &&
        (job.status === "cancelled" || job.status === "completed");
      const rangeText =
        job.scope === "book" && job.startChapter && job.endChapter
          ? `（${job.startChapter}-${job.endChapter}章）`
          : "";

      const errorHtml =
        errorCount > 0
          ? `
            <details style="margin-top:10px;">
              <summary style="cursor:pointer; font-weight:700;">查看错误详情（${errorCount}）</summary>
              <div style="margin-top:8px;">
                ${job.errors
                  .map(
                    (err) => `
                      <div class="result-box" style="margin-bottom:8px;">
                        <div><strong>目标：</strong>${escapeHtml(
                          `${err.target?.versionId || ""} / ${
                            err.target?.lang || ""
                          } / ${err.target?.bookId || ""} / ${
                            err.target?.chapter || ""
                          }`
                        )}</div>
                        <div><strong>错误：</strong>${escapeHtml(
                          err.message || "未知错误"
                        )}</div>
                      </div>
                    `
                  )
                  .join("")}
              </div>
            </details>
          `
          : "";

      const startedAt = String(job.startedAt || "").trim();
      const createdAt = String(job.createdAt || "").trim();
      const finishedAt = String(job.finishedAt || "").trim();
      const isRunning = job.status === "running";
      const startLabel = startedAt
        ? formatJobIsoLocal(startedAt)
        : createdAt
          ? `尚未开始执行（创建于 ${formatJobIsoLocal(createdAt)}）`
          : "—";
      const endLabel = finishedAt
        ? formatJobIsoLocal(finishedAt)
        : isRunning
          ? "进行中"
          : "—";
      const durationPlain = startedAt
        ? formatJobDuration(startedAt, finishedAt, {
            useNow: isRunning,
            nowMs: Date.now(),
          })
        : "—";
      const durationLabel =
        isRunning && startedAt && durationPlain !== "—"
          ? `已运行 ${durationPlain}`
          : durationPlain;

      return `
        <div class="test-result-seg">
          <h4>${escapeHtml(job.id)}</h4>
          <div class="test-result-line"><strong>状态：</strong>${escapeHtml(
            job.status || "—"
          )}</div>
          <div class="test-result-line"><strong>开始：</strong>${escapeHtml(
            startLabel
          )}</div>
          <div class="test-result-line"><strong>完成：</strong>${escapeHtml(
            endLabel
          )}</div>
          <div class="test-result-line"><strong>执行时间：</strong>${escapeHtml(
            durationLabel
          )}</div>
          <div class="test-result-line"><strong>范围：</strong>${escapeHtml(
            getScopeLabel(job.scope || "—")
          )}${escapeHtml(rangeText)}</div>
          <div class="test-result-line"><strong>进度：</strong>${escapeHtml(
            String(job.done || 0)
          )} / ${escapeHtml(String(job.total || 0))}</div>
          <div class="test-result-line"><strong>说明：</strong>${escapeHtml(
            job.progressText || "—"
          )}</div>
          <div class="test-result-line"><strong>build：</strong>${escapeHtml(
            job.buildId || "—"
          )}</div>
          ${
            job.retryOfJobId
              ? `<div class="test-result-line"><strong>重跑来源：</strong>${escapeHtml(
                  job.retryOfJobId
                )}</div>`
              : ""
          }
          ${
            mergedPublished
              ? `<div class="test-result-line"><strong>发布：</strong>已自动合并发布</div>`
              : ""
          }
          ${
            job.completionSummary
              ? `<div class="test-result-line"><strong>完成提示：</strong>${escapeHtml(
                  job.completionSummary
                )}</div>`
              : ""
          }
          ${errorHtml}
          <div class="modal-actions" style="margin-top:10px;">
            ${
              canCancel
                ? `<button class="secondary-btn" type="button" data-cancel-job-id="${escapeHtml(
                    job.id
                  )}">取消任务</button>`
                : ""
            }
            ${
              canRetryFailed
                ? `<button class="secondary-btn" type="button" data-retry-job-id="${escapeHtml(
                    job.id
                  )}">重跑失败章节</button>`
                : ""
            }
            ${
              canMergePublishFromBuild
                ? `<button class="primary-btn" type="button" data-merge-publish-job-id="${escapeHtml(
                    job.id
                  )}">合并发布已生成</button>`
                : ""
            }
          </div>
        </div>
      `;
    })
    .join("");

  box.querySelectorAll("[data-cancel-job-id]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const jobId = btn.getAttribute("data-cancel-job-id");
      if (!jobId) return;
      await cancelJob(jobId);
    });
  });

  box.querySelectorAll("[data-retry-job-id]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const jobId = btn.getAttribute("data-retry-job-id");
      if (!jobId) return;
      await retryFailedJob(jobId);
    });
  });

  box.querySelectorAll("[data-merge-publish-job-id]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const jobId = btn.getAttribute("data-merge-publish-job-id");
      if (!jobId) return;
      await mergePublishJobBuild(jobId);
    });
  });
}

function countReplacesExistingInMergeDetails(details) {
  let n = 0;
  for (const d of details || []) {
    for (const t of d.changedTargets || []) {
      if (t.replacesExisting) n += 1;
    }
  }
  return n;
}

/**
 * @returns {Promise<'skip'|'all'|'cancel'>}
 */
function showMergePublishChoiceDialog({
  introParagraphs,
  pub,
  skipSame,
  overwrite,
}) {
  return new Promise((resolve) => {
    const mask = document.createElement("div");
    mask.className = "modal-mask";
    mask.style.display = "flex";
    mask.style.alignItems = "center";
    mask.style.justifyContent = "center";
    mask.style.zIndex = "30000";
    mask.setAttribute("role", "dialog");
    mask.setAttribute("aria-modal", "true");

    const paras = (introParagraphs || [])
      .map(
        (line) =>
          `<p class="merge-publish-dialog-p">${escapeHtml(line)}</p>`
      )
      .join("");
    const summary = `<p class="merge-publish-dialog-p">${escapeHtml(
      `预检：将写入读者端 ${pub} 章；跳过（与线上一致）${skipSame} 项。`
    )}</p>`;
    const warn =
      overwrite > 0
        ? `<p class="merge-publish-dialog-warn">${escapeHtml(
            `其中 ${overwrite} 章将覆盖读者端已有内容（与 build 不一致）。`
          )}</p>`
        : "";
    const hasConflict = overwrite > 0;

    const actions = hasConflict
      ? `
        <div class="modal-actions merge-publish-dialog-actions">
          <button type="button" class="primary-btn" data-merge-choice="skip">
            仅发布不冲突章节（跳过 ${overwrite} 章覆盖）
          </button>
          <button type="button" class="secondary-btn" data-merge-choice="all">
            全部覆盖并发布
          </button>
          <button type="button" class="secondary-btn" data-merge-choice="cancel">
            取消
          </button>
        </div>`
      : `
        <div class="modal-actions merge-publish-dialog-actions">
          <button type="button" class="primary-btn" data-merge-choice="all">
            执行合并发布
          </button>
          <button type="button" class="secondary-btn" data-merge-choice="cancel">
            取消
          </button>
        </div>`;

    mask.innerHTML = `
      <div class="modal-card modal-card-sm merge-publish-dialog-card">
        <div class="modal-head">确认合并发布</div>
        <div class="modal-body merge-publish-dialog-body">
          ${paras}
          ${summary}
          ${warn}
          <p class="merge-publish-dialog-hint">请选择操作：</p>
        </div>
        ${actions}
      </div>
    `;

    function cleanup(choice) {
      mask.remove();
      resolve(choice);
    }

    mask.addEventListener("click", (e) => {
      if (e.target === mask) cleanup("cancel");
    });
    mask.querySelectorAll("[data-merge-choice]").forEach((btn) => {
      btn.addEventListener("click", () => {
        cleanup(btn.getAttribute("data-merge-choice") || "cancel");
      });
    });

    document.body.appendChild(mask);
  });
}

async function mergePublishJobBuild(jobId) {
  const statusEl = document.getElementById("jobCreateStatus");
  if (statusEl) statusEl.textContent = `正在比对 build 与已发布：${jobId}…`;
  const dryRes = await fetch(
    `/api/admin/job/${encodeURIComponent(jobId)}/merge-publish-build`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ onlyChanged: true, dryRun: true }),
    }
  );
  const dry = await dryRes.json().catch(() => ({}));
  if (!dryRes.ok) {
    const msg = dry.error || "预检失败";
    if (statusEl) statusEl.textContent = msg;
    alert(msg);
    return;
  }
  const pub = Number(dry.totalPublishedCount || 0);
  const skip = Number(dry.totalSkippedCount || 0);
  const overwrite = countReplacesExistingInMergeDetails(dry.details);
  if (pub === 0 && overwrite === 0) {
    if (statusEl) statusEl.textContent = "无可发布内容（与线上一致或 build 中无章节）。";
    return;
  }
  const choice = await showMergePublishChoiceDialog({
    introParagraphs: [
      "该任务将把 build 中已生成的章节合并到读者端「已发布」内容。",
    ],
    pub,
    skipSame: skip,
    overwrite,
  });
  if (choice === "cancel") {
    if (statusEl) statusEl.textContent = "已取消合并发布。";
    return;
  }
  const skipReplacesExisting = choice === "skip";
  if (statusEl) statusEl.textContent = `正在合并发布：${jobId}…`;
  const res = await fetch(
    `/api/admin/job/${encodeURIComponent(jobId)}/merge-publish-build`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ onlyChanged: true, skipReplacesExisting }),
    }
  );
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = data.error || "合并发布失败";
    if (statusEl) statusEl.textContent = msg;
    alert(msg);
    return;
  }
  const pubDone = Number(data.totalPublishedCount || 0);
  const skipDone = Number(data.totalSkippedCount || 0);
  const skipReplace = Number(data.totalSkippedWouldReplaceCount || 0);
  if (statusEl) {
    let line = `合并发布完成：${jobId}｜新发布或更新 ${pubDone} 章｜跳过（已与线上一致）${skipDone} 项`;
    if (skipReplace > 0) line += `｜跳过（避免覆盖）${skipReplace} 章`;
    statusEl.textContent = line;
  }
  await refreshJobsList(true);
}

async function mergePublishAllPartialBuildsFromUI() {
  const statusEl = document.getElementById("jobCreateStatus");
  if (statusEl) statusEl.textContent = "正在预检批量合并发布…";
  const dryRes = await fetch("/api/admin/jobs/merge-publish-partial-builds", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ onlyChanged: true, dryRun: true }),
  });
  const dry = await dryRes.json().catch(() => ({}));
  if (!dryRes.ok) {
    const msg = dry.error || "预检失败";
    if (statusEl) statusEl.textContent = msg;
    alert(msg);
    return;
  }
  const n = Number(dry.jobCount || 0);
  const pub = Number(dry.totalPublishedCount || 0);
  const skip = Number(dry.totalSkippedCount || 0);
  let overwrite = 0;
  for (const r of dry.results || []) {
    overwrite += countReplacesExistingInMergeDetails(r.details);
  }
  if (n === 0) {
    if (statusEl) statusEl.textContent = "当前没有可合并的任务。";
    return;
  }
  if (pub === 0 && overwrite === 0) {
    if (statusEl) statusEl.textContent = "无可发布内容（与线上一致）。";
    return;
  }
  const choice = await showMergePublishChoiceDialog({
    introParagraphs: [
      "将按创建时间从旧到新合并下列任务的 build：",
      "· 已取消且已生成过章节（进度 > 0）",
      "· 或已完成但未勾选「自动合并发布」",
      `同一章多任务时以后处理的为准。涉及 ${n} 个任务。`,
    ],
    pub,
    skipSame: skip,
    overwrite,
  });
  if (choice === "cancel") {
    if (statusEl) statusEl.textContent = "已取消批量合并发布。";
    return;
  }
  const skipReplacesExisting = choice === "skip";
  if (statusEl) statusEl.textContent = "正在批量合并发布…";
  const res = await fetch("/api/admin/jobs/merge-publish-partial-builds", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ onlyChanged: true, skipReplacesExisting }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = data.error || "批量合并发布失败";
    if (statusEl) statusEl.textContent = msg;
    alert(msg);
    return;
  }
  const nDone = Number(data.jobCount || 0);
  const pubDone = Number(data.totalPublishedCount || 0);
  const skipDone = Number(data.totalSkippedCount || 0);
  const skipReplace = Number(data.totalSkippedWouldReplaceCount || 0);
  if (statusEl) {
    let line = `批量合并完成：处理 ${nDone} 个任务｜新发布或更新 ${pubDone} 章｜跳过（已与线上一致）${skipDone} 项`;
    if (skipReplace > 0) line += `｜跳过（避免覆盖）${skipReplace} 章`;
    statusEl.textContent = line;
  }
  await refreshJobsList(true);
}

async function cancelJob(jobId) {
  const res = await fetch(
    `/api/admin/job/${encodeURIComponent(jobId)}/cancel`,
    {
      method: "POST",
    }
  );

  const data = await res.json();
  if (!res.ok) {
    alert(data.error || "取消任务失败");
    return;
  }

  await refreshJobsList(true);
}

function scopeCoversCurrentChapter(job) {
  if (job.scope === "bible") return true;

  const currentBook = getCurrentBookMeta();
  const isCurrentOld = currentBook?.testamentName === "旧约";
  const isCurrentNew = currentBook?.testamentName === "新约";

  if (job.scope === "old_testament") return isCurrentOld;
  if (job.scope === "new_testament") return isCurrentNew;
  if (job.scope === "book") return job.bookId === state.frontState.bookId;
  if (job.scope === "chapter") {
    return (
      job.bookId === state.frontState.bookId &&
      Number(job.chapter) === Number(state.frontState.chapter)
    );
  }

  return false;
}

async function maybeRefreshFrontAfterJobs(jobs) {
  const relevantJobs = jobs.filter((job) => {
    if (job.status !== "completed") return false;
    if (!String(job.progressText || "").includes("自动合并发布")) return false;

    const sameVersion = job.version === state.frontState.contentVersion;
    const sameLang = job.lang === state.frontState.contentLang;
    const coversCurrent = scopeCoversCurrentChapter(job);

    return sameVersion && sameLang && coversCurrent;
  });

  if (!relevantJobs.length) return;

  await loadStudyContent();
  renderStudyContent();
}

/* =========================
   已发布内容管理
   ========================= */
async function initPublishedManagerTab() {
  const versionSelect = document.getElementById("publishedVersionSelect");
  const langSelect = document.getElementById("publishedLangSelect");
  const loadBtn = document.getElementById("loadPublishedOverviewBtn");
  const publishByVersionBtn = document.getElementById("publishByVersionBtn");
  const publishByLangBtn = document.getElementById("publishByLangBtn");
  const publishByVersionLangBtn = document.getElementById(
    "publishByVersionLangBtn"
  );
  const publishAllVersionLangBtn = document.getElementById(
    "publishAllVersionLangBtn"
  );
  const autoRepublishMissingAllBtn = document.getElementById(
    "autoRepublishMissingAllBtn"
  );
  const stopPublishedActionBtn = document.getElementById("stopPublishedActionBtn");
  const previewPublishBulkBtn = document.getElementById("previewPublishBulkBtn");
  const exportPublishChangesJsonBtn = document.getElementById(
    "exportPublishChangesJsonBtn"
  );
  const exportPublishChangesCsvBtn = document.getElementById(
    "exportPublishChangesCsvBtn"
  );
  const loadChapterBtn = document.getElementById("loadPublishedChapterBtn");
  const deleteChapterBtn = document.getElementById("deletePublishedChapterBtn");
  const saveRevisionBtn = document.getElementById("savePublishedChapterRevisionBtn");

  if (!versionSelect || !langSelect) return;

  versionSelect.innerHTML = (adminState.bootstrap?.contentVersions || [])
    .map(
      (item) =>
        `<option value="${escapeHtml(item.id)}">${escapeHtml(
          item.label
        )}</option>`
    )
    .join("");

  langSelect.innerHTML = (adminState.bootstrap?.languages || [])
    .filter((item) => item.enabled)
    .map(
      (item) =>
        `<option value="${escapeHtml(item.id)}">${escapeHtml(
          item.label
        )}</option>`
    )
    .join("");

  versionSelect.value = "default";
  langSelect.value = "zh";
  renderPublishFeatureInfo();
  renderLastPublishedAction();
  renderPublishHistory();

  loadBtn?.addEventListener("click", async () => {
    await loadPublishedOverview();
  });

  publishByVersionBtn?.addEventListener("click", async () => {
    await runPublishedBulkAction("version");
  });

  publishByLangBtn?.addEventListener("click", async () => {
    await runPublishedBulkAction("lang");
  });

  publishByVersionLangBtn?.addEventListener("click", async () => {
    await runPublishedBulkAction("version_lang");
  });

  publishAllVersionLangBtn?.addEventListener("click", async () => {
    await runPublishedBulkAction("all");
  });

  autoRepublishMissingAllBtn?.addEventListener("click", async () => {
    await runPublishedAutoRepublishMissingBulk("all");
  });

  stopPublishedActionBtn?.addEventListener("click", async () => {
    await stopCurrentPublishedAction();
  });

  previewPublishBulkBtn?.addEventListener("click", async () => {
    await runPublishedBulkAction("all", true);
  });
  exportPublishChangesJsonBtn?.addEventListener("click", () => {
    exportLastPublishChanges("json");
  });
  exportPublishChangesCsvBtn?.addEventListener("click", () => {
    exportLastPublishChanges("csv");
  });

  loadChapterBtn?.addEventListener("click", async () => {
    await loadPublishedChapterDetail();
  });

  deleteChapterBtn?.addEventListener("click", async () => {
    await deletePublishedChapterAction();
  });

  saveRevisionBtn?.addEventListener("click", async () => {
    await savePublishedChapterRevision();
  });
}

async function runPublishedBulkAction(mode, dryRun = false) {
  const version = document.getElementById("publishedVersionSelect")?.value || "";
  const lang = document.getElementById("publishedLangSelect")?.value || "";
  const onlyChanged =
    document.getElementById("publishOnlyChangedToggle")?.checked !== false;
  const statusBox = document.getElementById("publishedBulkActionStatus");

  if (statusBox)
    statusBox.textContent = dryRun
      ? "正在预览增量变更，请稍候..."
      : "正在执行整本发布，请稍候...";

  const res = await fetch("/api/admin/published/republish-bulk", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      mode,
      version,
      lang,
      onlyChanged,
      dryRun,
    }),
  });

  const data = await res.json();
  adminState.lastPublishedBulkResult = data;
  adminState.lastPublishedPanelKind = "bulk";
  adminState.lastPublishedChanges = getFlattenPublishedChanges(data);
  saveLastPublishedChanges();

  if (!res.ok) {
    if (statusBox)
      statusBox.textContent = `执行失败：${data.error || "未知错误"}`;
    return;
  }

  if (statusBox) {
    const changedEntries = (data.details || [])
      .flatMap((item) =>
        (item.changedTargets || []).map((x) => ({
          version: item.version,
          lang: item.lang,
          bookId: x.bookId,
          chapter: x.chapter,
        }))
      )
      .slice(0, 120);
    statusBox.innerHTML = `
      <div><strong>执行模式：</strong>${escapeHtml(String(data.mode || mode))}</div>
      <div><strong>仅发布改动：</strong>${onlyChanged ? "是" : "否"}</div>
      <div><strong>运行方式：</strong>${dryRun ? "增量预览（不落盘）" : "正式发布"}</div>
      <div><strong>命中版本语言组：</strong>${escapeHtml(String(data.matchedPairs || 0))}</div>
      <div><strong>成功发布章节：</strong>${escapeHtml(String(data.totalPublishedCount || 0))}</div>
      <div><strong>跳过未变化章节：</strong>${escapeHtml(String(data.totalSkippedCount || 0))}</div>
      <div style="margin-top:8px;"><strong>${dryRun ? "预览改动清单（最多 120 条）" : "本次发布清单（最多 120 条）"}：</strong></div>
      <div style="margin-top:6px; line-height:1.6;">
        ${
          changedEntries.length
            ? changedEntries
                .map(
                  (x) =>
                    `${escapeHtml(String(x.version))} / ${escapeHtml(String(x.lang))} / ${escapeHtml(String(x.bookId))} ${escapeHtml(String(x.chapter))}章`
                )
                .join("<br/>")
            : "无改动章节"
        }
      </div>
    `;
  }

  if (!dryRun) {
    await loadPublishedOverview();
  }
  renderLastPublishedAction();
  recordPublishedActionHistory(data);
  renderPublishHistory();
}

async function runPublishedAutoRepublishMissingBulk(mode, dryRun = false) {
  const version = document.getElementById("publishedVersionSelect")?.value || "";
  const lang = document.getElementById("publishedLangSelect")?.value || "";
  const onlyChanged =
    document.getElementById("publishOnlyChangedToggle")?.checked !== false;
  const statusBox = document.getElementById("publishedBulkActionStatus");
  if (statusBox) {
    statusBox.textContent = dryRun
      ? "正在预览查漏补发，请稍候..."
      : "正在自动查漏补发，请稍候...";
  }
  const res = await fetch("/api/admin/published/auto-republish-missing-bulk", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      mode,
      version,
      lang,
      onlyChanged,
      dryRun,
    }),
  });
  const parsed = await readJsonResponse(res);
  if (parsed.parseError) {
    if (statusBox) {
      statusBox.textContent = `查漏补发失败：服务端返回异常（HTTP ${parsed.status}）。若为 502/504，多为单次处理时间过长导致网关超时；已优化服务端批量性能，若仍超时请改用「按版本+语言」缩小范围或联系运维调大 proxy_read_timeout。`;
    }
    return;
  }
  const data = parsed.data;
  if (!res.ok) {
    if (statusBox) {
      statusBox.textContent = `查漏补发失败：${data?.error || "未知错误"}`;
    }
    return;
  }
  adminState.lastPublishedAutoMissingResult = data;
  adminState.lastPublishedPanelKind = "auto_missing";
  renderLastPublishedAction();
  recordAutoRepublishMissingHistory(data);
  renderPublishHistory();
  if (statusBox) {
    const changedEntries = (data.details || [])
      .flatMap((item) =>
        (item.republishedTargets || []).map((x) => ({
          version: item.version,
          lang: item.lang,
          bookId: x.bookId,
          chapter: x.chapter,
        }))
      )
      .slice(0, 120);
    statusBox.innerHTML = `
      <div><strong>执行模式：</strong>${escapeHtml(String(data.mode || mode))}</div>
      <div><strong>仅发布改动：</strong>${onlyChanged ? "是" : "否"}</div>
      <div><strong>运行方式：</strong>${dryRun ? "预览（不落盘）" : "正式补发"}</div>
      <div><strong>命中版本语言组：</strong>${escapeHtml(String(data.matchedPairs || 0))}</div>
      <div><strong>补发前缺失章节：</strong>${escapeHtml(String(data.totalMissingBefore || 0))}</div>
      <div><strong>已尝试章节：</strong>${escapeHtml(String(data.totalAttempted || 0))}</div>
      <div><strong>补发成功：</strong>${escapeHtml(String(data.totalRepublished || 0))}</div>
      <div><strong>跳过：</strong>${escapeHtml(String(data.totalSkipped || 0))}</div>
      <div><strong>失败：</strong>${escapeHtml(String(data.totalFailed || 0))}（无来源 ${escapeHtml(
      String(data.totalNoSource || 0)
    )}）</div>
      ${
        Number(data.totalNoSource || 0) > 0
          ? `<div style="margin-top:8px;opacity:.95;">无来源表示在 <code>content_builds</code> 下找不到对应章节 JSON（含未完成任务或从未生成的卷章）。查漏补发只会把<strong>已有构建产物</strong>写入已发布目录，不会自动生成内容；请先对相关任务跑完生成或补建。</div>`
          : ""
      }
      <div style="margin-top:8px;"><strong>补发清单（最多 120 条）：</strong></div>
      <div style="margin-top:6px; line-height:1.6;">
        ${
          changedEntries.length
            ? changedEntries
                .map(
                  (x) =>
                    `${escapeHtml(String(x.version))} / ${escapeHtml(String(x.lang))} / ${escapeHtml(String(x.bookId))} ${escapeHtml(String(x.chapter))}章`
                )
                .join("<br/>")
            : "无可补发章节"
        }
      </div>
    `;
  }

  if (!dryRun) {
    await loadPublishedOverview();
  }
}

async function stopCurrentPublishedAction() {
  const statusBox = document.getElementById("publishedBulkActionStatus");
  if (statusBox) statusBox.textContent = "正在请求停止当前发布任务...";
  const res = await fetch("/api/admin/published/stop-current", {
    method: "POST",
  });
  const data = await res.json();
  if (!res.ok) {
    if (statusBox) {
      statusBox.textContent = `停止失败：${data.error || "未知错误"}`;
    }
    return;
  }
  if (statusBox) {
    statusBox.textContent = data.stopped
      ? "已发送停止请求，当前发布将尽快中断。"
      : "当前没有进行中的发布任务。";
  }
}

function getFlattenPublishedChanges(data) {
  if (!data || !Array.isArray(data.details)) return [];
  return data.details.flatMap((item) =>
    (item.changedTargets || []).map((x) => ({
      version: String(item.version || ""),
      lang: String(item.lang || ""),
      bookId: String(x.bookId || ""),
      chapter: Number(x.chapter || 0),
    }))
  );
}

function downloadTextFile(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function exportLastPublishChanges(format) {
  const data = adminState.lastPublishedBulkResult;
  const rows = getFlattenPublishedChanges(data);
  if (!rows.length) {
    alert("暂无可导出的改动清单，请先执行一次增量预览或发布。");
    return;
  }
  const ts = new Date().toISOString().replaceAll(":", "-");
  if (format === "json") {
    const payload = {
      exportedAt: new Date().toISOString(),
      mode: data.mode || "all",
      onlyChanged: data.onlyChanged !== false,
      dryRun: data.dryRun === true,
      totalPublishedCount: Number(data.totalPublishedCount || 0),
      totalSkippedCount: Number(data.totalSkippedCount || 0),
      changes: rows,
    };
    downloadTextFile(
      `publish-changes-${ts}.json`,
      JSON.stringify(payload, null, 2),
      "application/json;charset=utf-8"
    );
    return;
  }
  const header = ["version", "lang", "bookId", "chapter"];
  const csvLines = [header.join(",")].concat(
    rows.map((row) =>
      [row.version, row.lang, row.bookId, row.chapter].map((v) => `"${String(v).replaceAll('"', '""')}"`).join(",")
    )
  );
  downloadTextFile(
    `publish-changes-${ts}.csv`,
    csvLines.join("\n"),
    "text/csv;charset=utf-8"
  );
}

function renderPublishFeatureInfo() {
  const box = document.getElementById("publishFeatureInfoBox");
  if (!box) return;
  box.innerHTML = `
    <div><strong>增量发布能力版本：</strong>${escapeHtml(PUBLISH_MANAGER_FEATURE_VERSION)}</div>
    <div><strong>入口：</strong>管理后台 > 已发布内容</div>
    <div><strong>能力：</strong>所有版本一键发布 / 查看发布内容 / 一键全部自动查漏补发 / 增量预览（dryRun）/ 改动清单导出（JSON, CSV）</div>
  `;
}

function renderLastPublishedAction() {
  const box = document.getElementById("publishedLastActionBox");
  if (!box) return;
  if (adminState.lastPublishedPanelKind === "auto_missing") {
    const d = adminState.lastPublishedAutoMissingResult;
    if (!d || typeof d !== "object") {
      box.textContent = "最近一次执行：尚无记录。";
      return;
    }
    box.innerHTML = `
    <div><strong>最近一次执行：</strong>${d.dryRun ? "查漏补发预览" : "一键自动查漏补发"}</div>
    <div><strong>执行模式：</strong>${escapeHtml(String(d.mode || "all"))}</div>
    <div><strong>仅发布改动：</strong>${d.onlyChanged !== false ? "是" : "否"}</div>
    <div><strong>补发前缺失章节：</strong>${escapeHtml(String(d.totalMissingBefore || 0))}</div>
    <div><strong>补发成功：</strong>${escapeHtml(String(d.totalRepublished || 0))}</div>
    <div><strong>失败：</strong>${escapeHtml(String(d.totalFailed || 0))}（无来源 ${escapeHtml(
      String(d.totalNoSource || 0)
    )}）</div>
  `;
    return;
  }
  const data = adminState.lastPublishedBulkResult;
  if (!data || !Array.isArray(data.details)) {
    box.textContent = "最近一次执行：尚无记录。";
    return;
  }
  const rows = getFlattenPublishedChanges(data);
  box.innerHTML = `
    <div><strong>最近一次执行：</strong>${data.dryRun ? "增量预览" : "正式发布"}</div>
    <div><strong>执行模式：</strong>${escapeHtml(String(data.mode || "all"))}</div>
    <div><strong>仅发布改动：</strong>${data.onlyChanged !== false ? "是" : "否"}</div>
    <div><strong>改动章节数：</strong>${escapeHtml(String(rows.length))}</div>
    <div><strong>跳过未变化章节：</strong>${escapeHtml(String(data.totalSkippedCount || 0))}</div>
  `;
}

function recordPublishedActionHistory(data) {
  if (!data || !Array.isArray(data.details)) return;
  const entry = {
    at: new Date().toISOString(),
    actionType: "bulk",
    mode: String(data.mode || "all"),
    dryRun: data.dryRun === true,
    onlyChanged: data.onlyChanged !== false,
    matchedPairs: Number(data.matchedPairs || 0),
    totalPublishedCount: Number(data.totalPublishedCount || 0),
    totalSkippedCount: Number(data.totalSkippedCount || 0),
    changeCount: getFlattenPublishedChanges(data).length,
  };
  adminState.publishHistory = [entry].concat(adminState.publishHistory || []).slice(0, 10);
  savePublishHistory();
}

function recordAutoRepublishMissingHistory(data) {
  if (!data || typeof data !== "object") return;
  const entry = {
    at: new Date().toISOString(),
    actionType: "auto_republish_missing",
    mode: String(data.mode || "all"),
    dryRun: data.dryRun === true,
    onlyChanged: data.onlyChanged !== false,
    matchedPairs: Number(data.matchedPairs || 0),
    totalMissingBefore: Number(data.totalMissingBefore || 0),
    totalRepublished: Number(data.totalRepublished || 0),
    totalSkipped: Number(data.totalSkipped || 0),
    totalFailed: Number(data.totalFailed || 0),
    totalNoSource: Number(data.totalNoSource || 0),
  };
  adminState.publishHistory = [entry].concat(adminState.publishHistory || []).slice(0, 10);
  savePublishHistory();
}

function renderPublishHistory() {
  const box = document.getElementById("publishedHistoryBox");
  if (!box) return;
  const rows = adminState.publishHistory || [];
  if (!rows.length) {
    box.textContent = "暂无历史。";
    return;
  }
  box.innerHTML = rows
    .map((x, idx) => {
      const isAuto = x.actionType === "auto_republish_missing";
      const title = isAuto
        ? `${x.dryRun ? "查漏预览" : "查漏补发"}`
        : `${x.dryRun ? "增量预览" : "正式发布"}`;
      const detail = isAuto
        ? `缺失 ${escapeHtml(String(x.totalMissingBefore))}，补发 ${escapeHtml(
            String(x.totalRepublished)
          )}，跳过 ${escapeHtml(String(x.totalSkipped))}，失败 ${escapeHtml(
            String(x.totalFailed)
          )}（无来源 ${escapeHtml(String(x.totalNoSource))}），版本语言组 ${escapeHtml(
            String(x.matchedPairs)
          )}`
        : `改动 ${escapeHtml(String(x.changeCount))}，发布 ${escapeHtml(
            String(x.totalPublishedCount)
          )}，跳过 ${escapeHtml(String(x.totalSkippedCount))}，版本语言组 ${escapeHtml(
            String(x.matchedPairs)
          )}`;
      return `
      <div style="padding:6px 0; ${idx ? "border-top:1px solid rgba(214,203,187,.56);" : ""}">
        <div><strong>${idx + 1}.</strong> ${title}｜mode=${escapeHtml(String(x.mode))}｜仅改动=${
          x.onlyChanged ? "是" : "否"
        }</div>
        <div style="opacity:.88;">${detail}</div>
        <div style="opacity:.75;">${escapeHtml(new Date(x.at).toLocaleString())}</div>
      </div>
    `;
    })
    .join("");
}

async function loadPublishedOverview() {
  const version = document.getElementById("publishedVersionSelect")?.value;
  const lang = document.getElementById("publishedLangSelect")?.value;
  const summaryBox = document.getElementById("publishedSummaryBox");
  const booksBox = document.getElementById("publishedBooksBox");

  if (summaryBox) summaryBox.textContent = "正在读取...";
  if (booksBox)
    booksBox.innerHTML = `<div class="empty-state">正在读取...</div>`;

  const params = new URLSearchParams({
    version,
    lang,
  });

  const res = await fetch(
    `/api/admin/published/overview?${params.toString()}`,
    {
      cache: "no-store",
    }
  );
  const data = await res.json();

  if (!res.ok) {
    if (summaryBox)
      summaryBox.textContent = `读取失败：${data.error || "未知错误"}`;
    if (booksBox)
      booksBox.innerHTML = `<div class="empty-state">读取失败。</div>`;
    return;
  }

  adminState.publishedOverview = data;
  renderPublishedOverview(data);
}

function renderPublishedOverview(data) {
  const summaryBox = document.getElementById("publishedSummaryBox");
  const booksBox = document.getElementById("publishedBooksBox");

  if (summaryBox) {
    summaryBox.innerHTML = `
      <div><strong>总卷数：</strong>${escapeHtml(
        String(data.summary?.totalBooks || 0)
      )}</div>
      <div><strong>已有发布内容的卷数：</strong>${escapeHtml(
        String(data.summary?.booksWithAnyPublished || 0)
      )}</div>
      <div><strong>已发布章节总数：</strong>${escapeHtml(
        String(data.summary?.totalPublishedChapters || 0)
      )}</div>
      <div><strong>缺失章节总数：</strong>${escapeHtml(
        String(data.summary?.totalMissingChapters || 0)
      )}</div>
    `;
  }

  if (booksBox) {
    booksBox.innerHTML = (data.books || [])
      .map((book) => {
        return `
          <div class="test-result-seg">
            <h4>${escapeHtml(book.bookCn || book.bookId)} (${escapeHtml(
          book.bookId
        )})</h4>
            <div class="test-result-line"><strong>总章数：</strong>${escapeHtml(
              String(book.totalChapters || 0)
            )}</div>
            <div class="test-result-line"><strong>已发布章数：</strong>${escapeHtml(
              String(book.publishedCount || 0)
            )}</div>

            <div class="test-result-line" style="margin-top:10px;"><strong>已发布章节：</strong></div>
            <div style="display:flex; flex-wrap:wrap; gap:8px; margin-top:8px;">
              ${
                (book.publishedChapters || []).length
                  ? book.publishedChapters
                      .map(
                        (chapter) => `
                          <div style="display:flex; gap:6px; align-items:center; flex-wrap:wrap; border:1px solid rgba(214,203,187,.72); border-radius:999px; padding:6px 10px;">
                            <span>${escapeHtml(String(chapter))}</span>
                            <button class="secondary-btn small-btn" type="button"
                              data-published-view-book="${escapeHtml(
                                book.bookId
                              )}"
                              data-published-view-chapter="${escapeHtml(
                                String(chapter)
                              )}">
                              查看
                            </button>
                            <button class="secondary-btn small-btn" type="button"
                              data-published-delete-book="${escapeHtml(
                                book.bookId
                              )}"
                              data-published-delete-chapter="${escapeHtml(
                                String(chapter)
                              )}">
                              删除
                            </button>
                          </div>
                        `
                      )
                      .join("")
                  : `<span>—</span>`
              }
            </div>

            <div class="test-result-line" style="margin-top:14px;"><strong>缺失章节：</strong></div>
            <div style="display:flex; flex-wrap:wrap; gap:8px; margin-top:8px;">
              ${
                (book.missingChapters || []).length
                  ? book.missingChapters
                      .map(
                        (chapter) => `
                          <div style="display:flex; gap:6px; align-items:center; flex-wrap:wrap; border:1px dashed rgba(214,203,187,.72); border-radius:999px; padding:6px 10px;">
                            <span>${escapeHtml(String(chapter))}</span>
                            <button class="secondary-btn small-btn" type="button"
                              data-published-auto-book="${escapeHtml(
                                book.bookId
                              )}"
                              data-published-auto-chapter="${escapeHtml(
                                String(chapter)
                              )}">
                              自动补发
                            </button>
                          </div>
                        `
                      )
                      .join("")
                  : `<span>—</span>`
              }
            </div>
          </div>
        `;
      })
      .join("");

    bindPublishedOverviewButtons();
  }
}

function bindPublishedOverviewButtons() {
  document.querySelectorAll("[data-published-view-book]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const bookId = btn.getAttribute("data-published-view-book");
      const chapter = btn.getAttribute("data-published-view-chapter");
      fillPublishedDetailInputs(bookId, chapter);
      await loadPublishedChapterDetail();
    });
  });

  document.querySelectorAll("[data-published-delete-book]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const bookId = btn.getAttribute("data-published-delete-book");
      const chapter = btn.getAttribute("data-published-delete-chapter");
      fillPublishedDetailInputs(bookId, chapter);
      await deletePublishedChapterAction();
    });
  });

  document.querySelectorAll("[data-published-auto-book]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const bookId = btn.getAttribute("data-published-auto-book");
      const chapter = btn.getAttribute("data-published-auto-chapter");
      await autoRepublishMissingChapter(bookId, chapter);
    });
  });
}

function fillPublishedDetailInputs(bookId, chapter) {
  const bookInput = document.getElementById("publishedDetailBookInput");
  const chapterInput = document.getElementById("publishedDetailChapterInput");
  if (bookInput) bookInput.value = bookId || "";
  if (chapterInput) chapterInput.value = chapter || "";
}

async function loadPublishedChapterDetail() {
  const version = document.getElementById("publishedVersionSelect")?.value;
  const lang = document.getElementById("publishedLangSelect")?.value;
  const bookId = document
    .getElementById("publishedDetailBookInput")
    ?.value.trim();
  const chapter = document
    .getElementById("publishedDetailChapterInput")
    ?.value.trim();
  const editor = document.getElementById("publishedChapterJsonEditor");
  const statusEl = document.getElementById("publishedChapterEditorStatus");

  if (!bookId || !chapter) {
    if (statusEl) statusEl.textContent = "请先输入书卷和章节。";
    return;
  }

  if (statusEl) statusEl.textContent = "正在读取章节…";

  const params = new URLSearchParams({
    version,
    lang,
    bookId,
    chapter,
  });

  const res = await fetch(`/api/admin/published/chapter?${params.toString()}`, {
    cache: "no-store",
  });
  const data = await res.json();

  if (!res.ok) {
    if (statusEl)
      statusEl.textContent = `读取失败：${data.error || "未知错误"}`;
    if (editor) editor.value = "";
    return;
  }

  if (editor) {
    editor.value = JSON.stringify(data, null, 2);
  }
  if (statusEl) {
    statusEl.textContent = `已载入 ${bookId} 第${chapter}章，可编辑后点「保存并发布（已审核）」。`;
  }
}

async function savePublishedChapterRevision() {
  const editor = document.getElementById("publishedChapterJsonEditor");
  const statusEl = document.getElementById("publishedChapterEditorStatus");
  const noteInput = document.getElementById("publishedChapterReviewNote");
  if (!editor) return;

  let parsed;
  try {
    parsed = JSON.parse(String(editor.value || "").trim() || "{}");
  } catch (err) {
    if (statusEl) {
      statusEl.textContent = `JSON 格式错误：${err?.message || err}`;
    }
    return;
  }

  if (!parsed || typeof parsed !== "object") {
    if (statusEl) statusEl.textContent = "内容不是有效的 JSON 对象。";
    return;
  }

  if (
    !confirm(
      "确认已完成人工审核？将立即写入构建并合并到已发布内容，读者端会读到新版本。"
    )
  ) {
    return;
  }

  const token = getAuthToken();
  if (statusEl) statusEl.textContent = "正在保存并发布…";

  const res = await fetch("/api/admin/save-test-result", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify({
      studyContent: parsed,
      reviewNote: String(noteInput?.value || "").trim(),
    }),
  });

  const data = await res.json();

  if (!res.ok) {
    if (statusEl) {
      statusEl.textContent = `保存失败：${data.error || "未知错误"}`;
    }
    return;
  }

  const saved = data.savedContent || {};
  if (statusEl) {
    statusEl.textContent = `已发布：${saved.bookId || ""} 第${saved.chapter || ""}章（build ${data.buildId || ""}）`;
  }

  if (editor && saved && typeof saved === "object") {
    editor.value = JSON.stringify(saved, null, 2);
  }

  const sameVersion = state.frontState.contentVersion === saved.version;
  const sameLang = state.frontState.contentLang === saved.contentLang;
  const sameBook = state.frontState.bookId === saved.bookId;
  const sameChapter = Number(state.frontState.chapter) === Number(saved.chapter);

  if (sameVersion && sameLang && sameBook && sameChapter) {
    await loadStudyContent();
    renderStudyContent();
  }

  await loadPublishedOverview();
}

async function deletePublishedChapterAction() {
  const version = document.getElementById("publishedVersionSelect")?.value;
  const lang = document.getElementById("publishedLangSelect")?.value;
  const bookId = document
    .getElementById("publishedDetailBookInput")
    ?.value.trim();
  const chapter = document
    .getElementById("publishedDetailChapterInput")
    ?.value.trim();
  const editor = document.getElementById("publishedChapterJsonEditor");
  const statusEl = document.getElementById("publishedChapterEditorStatus");

  if (!bookId || !chapter) {
    if (statusEl) statusEl.textContent = "请先输入书卷和章节。";
    return;
  }

  if (!confirm(`确认删除已发布内容：${bookId} ${chapter}章？`)) return;

  if (statusEl) statusEl.textContent = "正在删除…";

  const params = new URLSearchParams({
    version,
    lang,
    bookId,
    chapter,
  });

  const res = await fetch(`/api/admin/published/chapter?${params.toString()}`, {
    method: "DELETE",
  });
  const data = await res.json();

  if (!res.ok) {
    if (statusEl)
      statusEl.textContent = `删除失败：${data.error || "未知错误"}`;
    return;
  }

  if (editor) editor.value = "";
  if (statusEl) {
    statusEl.textContent = `已删除：${bookId} 第${chapter}章`;
  }

  await loadPublishedOverview();
}

async function autoRepublishMissingChapter(bookId, chapter) {
  const version = document.getElementById("publishedVersionSelect")?.value;
  const lang = document.getElementById("publishedLangSelect")?.value;
  const editor = document.getElementById("publishedChapterJsonEditor");
  const statusEl = document.getElementById("publishedChapterEditorStatus");

  if (statusEl) {
    statusEl.textContent = `正在自动补发：${bookId} 第${chapter}章…`;
  }

  const res = await fetch("/api/admin/published/auto-republish-chapter", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      version,
      lang,
      bookId,
      chapter: Number(chapter),
    }),
  });

  const data = await res.json();

  if (!res.ok) {
    if (statusEl) {
      statusEl.textContent = `自动补发失败：${data.error || "未知错误"}`;
    }
    return;
  }

  fillPublishedDetailInputs(bookId, chapter);

  if (editor) {
    editor.value = JSON.stringify(data, null, 2);
  }
  if (statusEl) {
    statusEl.textContent = `已补发并载入 ${bookId} 第${chapter}章。`;
  }

  await loadPublishedOverview();

  const sameVersion = state.frontState.contentVersion === version;
  const sameLang = state.frontState.contentLang === lang;
  const sameBook = state.frontState.bookId === bookId;
  const sameChapter = Number(state.frontState.chapter) === Number(chapter);

  if (sameVersion && sameLang && sameBook && sameChapter) {
    await loadStudyContent();
    renderStudyContent();
  }
}

/* =========================
   圣经版本管理
   ========================= */
function renderContentVersionsEditorFromBootstrap() {
  const box = document.getElementById("contentVersionsRows");
  if (!box) return;
  const rows = (adminState.bootstrap?.contentVersions || [])
    .slice()
    .sort((a, b) => Number(a.order || 999) - Number(b.order || 999));
  if (!rows.length) {
    box.innerHTML = `<div class="empty-state">无内容版本数据。</div>`;
    return;
  }
  box.innerHTML = rows
    .map((item) => {
      const id = escapeHtml(item.id);
      const label = escapeHtml(item.label || item.id);
      const order = Number(item.order) || 0;
      const en = item.enabled !== false;
      const menu = item.showInMenu !== false;
      return `<div class="content-version-row" data-cv-id="${id}">
        <div class="cv-cell cv-id"><span class="label">ID</span><div class="cv-id-text">${id}</div></div>
        <div class="cv-cell"><span class="label">显示名称</span><input type="text" class="custom-textarea single-input cv-label" value="${label}" /></div>
        <div class="cv-cell cv-order"><span class="label">排序</span><input type="number" class="custom-textarea single-input cv-order-input" value="${order}" /></div>
        <label class="cv-cell cv-check"><input type="checkbox" class="cv-enabled" ${en ? "checked" : ""} /> <span>启用（系统）</span></label>
        <label class="cv-cell cv-check"><input type="checkbox" class="cv-show-menu" ${menu ? "checked" : ""} /> <span>前台菜单显示</span></label>
      </div>`;
    })
    .join("");
}

function collectContentVersionsFromEditor() {
  return Array.from(
    document.querySelectorAll("#contentVersionsRows .content-version-row")
  ).map((row) => {
    const id = String(row.getAttribute("data-cv-id") || "").trim();
    const label = String(row.querySelector(".cv-label")?.value || "").trim();
    const order = Number(row.querySelector(".cv-order-input")?.value) || 0;
    const enabled = row.querySelector(".cv-enabled")?.checked === true;
    const showInMenu = row.querySelector(".cv-show-menu")?.checked === true;
    return { id, label: label || id, order, enabled, showInMenu };
  });
}

async function initContentVersionsManagerTab() {
  renderContentVersionsEditorFromBootstrap();

  const resultEl = document.getElementById("contentVersionsSaveResult");
  const setResult = (text) => {
    if (resultEl) resultEl.textContent = text;
  };

  const reloadBtn = document.getElementById("reloadContentVersionsBtn");
  if (reloadBtn && !reloadBtn.dataset.bound) {
    reloadBtn.dataset.bound = "1";
    reloadBtn.addEventListener("click", async () => {
      try {
        await loadAdminBootstrap();
        renderContentVersionsEditorFromBootstrap();
        await initRuleEditorTab();
        renderTestVersionOptions();
        setResult("已从服务器重新载入。");
      } catch (e) {
        setResult(e?.message || String(e));
      }
    });
  }

  const saveBtn = document.getElementById("saveContentVersionsBtn");
  if (!saveBtn || saveBtn.dataset.bound) return;
  saveBtn.dataset.bound = "1";
  saveBtn.addEventListener("click", async () => {
    const list = collectContentVersionsFromEditor();
    if (!list.length) {
      alert("没有可保存的数据");
      return;
    }
    try {
      const res = await fetch("/api/admin/content-versions/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ contentVersions: list }),
      });
      const data = await parseFetchJsonResponse(res);
      if (!res.ok) throw new Error(data.error || "保存失败");
      adminState.bootstrap = adminState.bootstrap || {};
      adminState.bootstrap.contentVersions = data.contentVersions || list;
      renderContentVersionsEditorFromBootstrap();
      await initRuleEditorTab();
      renderTestVersionOptions();
      setResult("已保存。刷新首页后读经菜单将更新。");
      alert("内容版本已保存。");
    } catch (e) {
      alert(e?.message || String(e));
      setResult(e?.message || String(e));
    }
  });
}

async function initScriptureVersionManagerTab() {
  await refreshScriptureVersionsList();

  document
    .getElementById("refreshScriptureVersionsBtn")
    ?.addEventListener("click", async () => {
      await refreshScriptureVersionsList();
    });

  document
    .getElementById("newScriptureVersionBtn")
    ?.addEventListener("click", () => {
      clearScriptureVersionEditor();
    });

  document
    .getElementById("saveScriptureVersionBtn")
    ?.addEventListener("click", async () => {
      await saveScriptureVersionFromEditor();
    });

  document
    .getElementById("deleteScriptureVersionBtn")
    ?.addEventListener("click", async () => {
      await deleteCurrentScriptureVersion();
    });
}

function renderPointsConfigForm(config) {
  const safe = config || {};
  const naming = safe.naming || {};
  const setValue = (id, value) => {
    const el = document.getElementById(id);
    if (el) el.value = String(value || "");
  };
  setValue("pointsNameInput", naming.pointName || "");
  setValue("pointsLevelNameInput", naming.levelName || "");
  setValue("pointsRecordNameInput", naming.recordName || "");
  setValue("pointsBoardNameInput", naming.leaderboardName || "");
  setValue("pointsNoteInput", naming.note || "");
  setValue("pointsLevelsInput", Array.isArray(safe.levels) ? safe.levels.join("\n") : "");
  const previewEl = document.getElementById("pointsConfigPreview");
  if (previewEl) previewEl.textContent = JSON.stringify(safe, null, 2);
  renderPointsStarPreview();
}

function getStarDisplay(level) {
  const normalized = Math.max(1, Math.min(12, Number(level) || 1));
  const group = Math.ceil(normalized / 3);
  const stars = ((normalized - 1) % 3) + 1;
  return { group, stars };
}

/** 与 styles.css :root --star-g* 一致；SVG fill 不受链接色 / emoji 字体的 color 影响 */
const STAR_LEVEL_HEX_BY_GROUP = {
  1: ["#ede6d8", "#e4d7be", "#dcc8a5"],
  2: ["#d4b77d", "#c9a863", "#be9a4a"],
  3: ["#b38b3e", "#a27c34", "#916e2b"],
  4: ["#7a5c24", "#5f471b", "#453313"],
};

const STAR_LEVEL_SVG_PATH =
  "M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z";

function renderLevelStarSvgs(count, hexFill) {
  const n = Math.max(1, Math.min(5, Number(count) || 1));
  const fill = String(hexFill || "#c9a863");
  let html = "";
  for (let i = 0; i < n; i++) {
    html += `<svg class="star-level-svg" viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path fill="${fill}" d="${STAR_LEVEL_SVG_PATH}"/></svg>`;
  }
  return html;
}

function renderStars(level, starTitle) {
  const { group, stars } = getStarDisplay(level);
  const color = STAR_LEVEL_HEX_BY_GROUP[group][stars - 1];
  const t =
    starTitle != null && String(starTitle).trim() !== ""
      ? String(starTitle)
      : "Learning progress";
  return `<span class="star-level star-level--svgs" title="${escapeHtml(
    t
  )}">${renderLevelStarSvgs(stars, color)}</span>`;
}

function renderPointsStarPreview() {
  const box = document.getElementById("pointsStarPreviewBox");
  if (!box) return;
  const rows = Array.from({ length: 12 }, (_, i) => i + 1)
    .map(
      (level) => `
      <span style="display:inline-flex; align-items:center; justify-content:center; min-width:46px; padding:6px 8px; border-radius:10px; background:rgba(255,253,248,.68); border:1px solid rgba(214,203,187,.52); margin:4px;">
        ${renderStars(level)}
      </span>`
    )
    .join("");
  box.innerHTML = `<div style="display:flex; flex-wrap:wrap;">${rows}</div>`;
}

function collectPointsConfigForm() {
  const getValue = (id) => String(document.getElementById(id)?.value || "").trim();
  return {
    naming: {
      pointName: getValue("pointsNameInput"),
      levelName: getValue("pointsLevelNameInput"),
      recordName: getValue("pointsRecordNameInput"),
      leaderboardName: getValue("pointsBoardNameInput"),
      note: getValue("pointsNoteInput"),
    },
    levels: getValue("pointsLevelsInput")
      .split("\n")
      .map((x) => x.trim())
      .filter(Boolean),
  };
}

async function initPointsSystemTab() {
  const reloadBtn = document.getElementById("reloadPointsConfigBtn");
  const saveBtn = document.getElementById("savePointsConfigBtn");
  const previewEl = document.getElementById("pointsConfigPreview");
  if (!previewEl) return;

  async function reloadConfig() {
    const res = await fetch("/api/admin/points/config", { cache: "no-store" });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "读取成长体系配置失败");
    adminState.currentPointsConfig = data;
    renderPointsConfigForm(data);
  }

  reloadBtn?.addEventListener("click", async () => {
    try {
      await reloadConfig();
    } catch (error) {
      previewEl.textContent = error?.message || "读取失败";
    }
  });

  saveBtn?.addEventListener("click", async () => {
    try {
      const pointsConfig = collectPointsConfigForm();
      const res = await fetch("/api/admin/points/config/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pointsConfig }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "保存成长体系配置失败");
      adminState.currentPointsConfig = data.pointsConfig || pointsConfig;
      renderPointsConfigForm(adminState.currentPointsConfig);
    } catch (error) {
      previewEl.textContent = error?.message || "保存失败";
    }
  });

  if (adminState.currentPointsConfig) {
    renderPointsConfigForm(adminState.currentPointsConfig);
  } else {
    await reloadConfig();
  }
}

async function initDeployManagerTab() {
  const uploadInput = document.getElementById("deployUploadInput");
  const uploadSelect = document.getElementById("deployUploadSelect");
  const packageVersionInput = document.getElementById("deployPackageVersionInput");
  const downloadUpgradePackageBtn = document.getElementById(
    "downloadUpgradePackageBtn"
  );
  const downloadFullPackageBtn = document.getElementById("downloadFullPackageBtn");
  const downloadFullSlimPackageBtn = document.getElementById("downloadFullSlimPackageBtn");
  const generateUpgradeCmdBtn = document.getElementById("generateUpgradeCmdBtn");
  const generateFullCmdBtn = document.getElementById("generateFullCmdBtn");
  const generateFullSlimCmdBtn = document.getElementById("generateFullSlimCmdBtn");
  const downloadChangedPackageBtn = document.getElementById(
    "downloadChangedPackageBtn"
  );
  const commandBox = document.getElementById("deployPackageCommandBox");
  const uploadBtn = document.getElementById("deployUploadBtn");
  const applyBtn = document.getElementById("deployApplyBtn");
  const rollbackBtn = document.getElementById("deployRollbackBtn");
  const refreshBtn = document.getElementById("deployRefreshBtn");
  const statusBox = document.getElementById("deployStatusBox");
  const createDataBackupBtn = document.getElementById("createDataBackupBtn");
  const refreshDataBackupBtn = document.getElementById("refreshDataBackupBtn");
  const downloadDataBackupBtn = document.getElementById("downloadDataBackupBtn");
  const restoreDataBackupBtn = document.getElementById("restoreDataBackupBtn");
  const pruneDataBackupBtn = document.getElementById("pruneDataBackupBtn");
  const saveDataBackupConfigBtn = document.getElementById("saveDataBackupConfigBtn");
  const runAutoBackupNowBtn = document.getElementById("runAutoBackupNowBtn");
  const dataBackupKeepCountInput = document.getElementById("dataBackupKeepCountInput");
  const autoBackupEnabledInput = document.getElementById("autoBackupEnabledInput");
  const autoBackupHourInput = document.getElementById("autoBackupHourInput");
  const autoBackupMinuteInput = document.getElementById("autoBackupMinuteInput");
  const dataBackupSelect = document.getElementById("dataBackupSelect");
  const dataBackupStatusBox = document.getElementById("dataBackupStatusBox");
  const refreshAuditLogBtn = document.getElementById("refreshAuditLogBtn");
  const auditLogBox = document.getElementById("auditLogBox");
  const systemOpenAiKeyInput = document.getElementById("systemOpenAiKeyInput");
  const saveSystemOpenAiKeyBtn = document.getElementById("saveSystemOpenAiKeyBtn");
  const clearSystemOpenAiKeyBtn = document.getElementById("clearSystemOpenAiKeyBtn");
  const refreshSystemOpenAiKeyBtn = document.getElementById("refreshSystemOpenAiKeyBtn");
  const systemOpenAiKeyStatusBox = document.getElementById("systemOpenAiKeyStatusBox");
  if (!uploadSelect || !statusBox) return;

  function getPackageVersion() {
    return String(packageVersionInput?.value || "").trim();
  }

  async function loadPackageCommand(kind) {
    const params = new URLSearchParams({ kind });
    const version = getPackageVersion();
    if (version) params.set("version", version);
    const res = await fetch(`/api/admin/deploy/package-command?${params.toString()}`, {
      cache: "no-store",
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "生成打包命令失败");
    if (commandBox) {
      commandBox.textContent = `${data.command || ""}`;
    }
  }

  function downloadPackage(kind) {
    const params = new URLSearchParams({ kind });
    const version = getPackageVersion();
    if (version) params.set("version", version);
    window.open(`/api/admin/deploy/package/download?${params.toString()}`, "_blank");
  }

  async function downloadChangedPackage() {
    const changes = Array.isArray(adminState.lastPublishedChanges)
      ? adminState.lastPublishedChanges
      : [];
    if (!changes.length) {
      statusBox.textContent =
        "没有可用的最近改动清单，请先在“已发布内容”里执行一次增量预览/发布。";
      return;
    }
    statusBox.textContent = "正在生成改动升级包...";
    const version = getPackageVersion();
    const res = await fetch("/api/admin/deploy/package/download-changed", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        version,
        changes,
      }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      statusBox.textContent = err.error || "生成改动升级包失败";
      return;
    }
    const blob = await res.blob();
    const ts = new Date().toISOString().replaceAll(":", "-");
    const fileVersion = version || `changed-${ts}`;
    const fileName = `askbible-changed-${fileVersion}.zip`;
    downloadTextFile(fileName, blob, "application/zip");
    statusBox.textContent = `改动升级包已下载：${fileName}`;
  }

  async function loadStatus() {
    const res = await fetch("/api/admin/deploy/status", { cache: "no-store" });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "读取部署状态失败");
    const uploads = Array.isArray(data.uploads) ? data.uploads : [];
    uploadSelect.innerHTML = uploads.length
      ? uploads
          .map(
            (x) =>
              `<option value="${escapeHtml(String(x.id || ""))}">${escapeHtml(
                `${x.version || x.id} (${x.uploadedAt || ""})`
              )}</option>`
          )
          .join("")
      : `<option value="">暂无上传包</option>`;
    const latestHistory = Array.isArray(data.history) ? data.history[0] : null;
    statusBox.textContent = `当前版本：${data.currentVersion || "未设置"}
服务版本号（启动时间）：${data.runtime?.bootIso || "未知"}${data.runtime?.pid ? ` (PID ${data.runtime.pid})` : ""}${
      latestHistory
        ? `\n最近操作：${latestHistory.action} ${latestHistory.version || ""} ${latestHistory.at || ""}`
        : ""
    }`;
  }

  async function loadDataBackups() {
    if (!dataBackupSelect || !dataBackupStatusBox) return;
    const res = await fetch("/api/admin/data-backups", { cache: "no-store" });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "读取数据备份失败");
    const keepCount = Math.max(
      1,
      Math.min(200, Number(data.keepCount || data.defaultKeepCount || 20) || 20)
    );
    if (dataBackupKeepCountInput) {
      dataBackupKeepCountInput.value = String(keepCount);
    }
    if (autoBackupEnabledInput) autoBackupEnabledInput.checked = Boolean(data.autoBackupEnabled);
    if (autoBackupHourInput) {
      autoBackupHourInput.value = String(
        Math.max(0, Math.min(23, Number(data.autoBackupHour ?? 3) || 3))
      );
    }
    if (autoBackupMinuteInput) {
      autoBackupMinuteInput.value = String(
        Math.max(0, Math.min(59, Number(data.autoBackupMinute ?? 0) || 0))
      );
    }
    const items = Array.isArray(data.items) ? data.items : [];
    dataBackupSelect.innerHTML = items.length
      ? items
          .map(
            (x) =>
              `<option value="${escapeHtml(String(x.id || ""))}">${escapeHtml(
                `${x.id || ""} (${x.createdAt || ""})`
              )}</option>`
          )
          .join("")
      : `<option value="">暂无备份</option>`;
    dataBackupStatusBox.textContent = `共 ${items.length} 份备份。${
      data.lastAutoBackupDate ? ` 最近一次自动备份日期：${data.lastAutoBackupDate}` : ""
    }`;
  }

  async function loadAuditLog() {
    if (!auditLogBox) return;
    const res = await fetch("/api/admin/audit-log?limit=80", { cache: "no-store" });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "读取审计日志失败");
    const items = Array.isArray(data.items) ? data.items : [];
    if (!items.length) {
      auditLogBox.textContent = "暂无审计记录。";
      return;
    }
    auditLogBox.textContent = items
      .map(
        (x) =>
          `${x.at || ""} | ${x.action || ""} | ${x.actorName || x.actorEmail || "未知"}${
            x.actorRole ? `(${x.actorRole})` : ""
          } | ${JSON.stringify(x.detail || {})}`
      )
      .join("\n");
  }

  async function loadSystemOpenAiKeyStatus() {
    if (!systemOpenAiKeyStatusBox) return;
    const res = await fetch("/api/admin/system/openai-key/status", {
      cache: "no-store",
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "读取密钥状态失败");
    const sourceMap = {
      system: "后台系统密钥（文件）",
      env: "环境变量 OPENAI_API_KEY（优先）",
      none: "未配置",
    };
    const shadowHint = data.systemSecretShadowed
      ? "（后台仍存有一份密钥，当前实际使用环境变量）"
      : "";
    const envBlock =
      data.envOverridesSystem && data.source === "env"
        ? `【重要】服务器上设置了 OPENAI_API_KEY，会覆盖后台保存的 Key。环境变量尾号：${
            data.envMasked || "?"
          }；后台文件尾号：${data.systemMasked || "无"}。若你只在后台换了 Key 仍 401，请到部署环境修改或删除 OPENAI_API_KEY 后重启。`
        : "";
    systemOpenAiKeyStatusBox.textContent = `当前状态：${
      data.configured ? "已配置" : "未配置"
    }；生效来源：${sourceMap[data.source] || data.source || "未知"}${shadowHint}${
      data.masked ? `；当前生效 Key 尾号：${data.masked}` : ""
    }${envBlock ? `\n${envBlock}` : ""}`;
  }

  uploadBtn?.addEventListener("click", async () => {
    const file = uploadInput?.files?.[0];
    if (!file) {
      statusBox.textContent = "请先选择 zip 包";
      return;
    }
    statusBox.textContent = "上传中...";
    const form = new FormData();
    form.append("package", file);
    const res = await fetch("/api/admin/deploy/upload", {
      method: "POST",
      body: form,
    });
    const data = await res.json();
    if (!res.ok) {
      statusBox.textContent = data.error || "上传失败";
      return;
    }
    statusBox.textContent = `上传成功：${data.version || data.uploadId}`;
    await loadStatus();
  });

  generateUpgradeCmdBtn?.addEventListener("click", async () => {
    try {
      await loadPackageCommand("upgrade");
    } catch (error) {
      if (commandBox) commandBox.textContent = error?.message || "生成命令失败";
    }
  });

  generateFullCmdBtn?.addEventListener("click", async () => {
    try {
      await loadPackageCommand("full");
    } catch (error) {
      if (commandBox) commandBox.textContent = error?.message || "生成命令失败";
    }
  });

  generateFullSlimCmdBtn?.addEventListener("click", async () => {
    try {
      await loadPackageCommand("full-slim");
    } catch (error) {
      if (commandBox) commandBox.textContent = error?.message || "生成命令失败";
    }
  });

  downloadUpgradePackageBtn?.addEventListener("click", () => {
    downloadPackage("upgrade");
  });

  downloadFullPackageBtn?.addEventListener("click", () => {
    downloadPackage("full");
  });

  downloadFullSlimPackageBtn?.addEventListener("click", () => {
    downloadPackage("full-slim");
  });

  downloadChangedPackageBtn?.addEventListener("click", async () => {
    await downloadChangedPackage();
  });

  applyBtn?.addEventListener("click", async () => {
    const uploadId = String(uploadSelect.value || "");
    if (!uploadId) {
      statusBox.textContent = "请选择上传包";
      return;
    }
    statusBox.textContent = "应用升级中...";
    const res = await fetch("/api/admin/deploy/apply", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ uploadId }),
    });
    const data = await res.json();
    if (!res.ok) {
      statusBox.textContent = data.error || "应用升级失败";
      return;
    }
    statusBox.textContent = `升级成功：${data.version || ""}`;
    await loadStatus();
  });

  rollbackBtn?.addEventListener("click", async () => {
    statusBox.textContent = "回滚中...";
    const res = await fetch("/api/admin/deploy/rollback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok) {
      statusBox.textContent = data.error || "回滚失败";
      return;
    }
    statusBox.textContent = `回滚成功：${data.backupId || ""}`;
    await loadStatus();
  });

  createDataBackupBtn?.addEventListener("click", async () => {
    if (!dataBackupStatusBox) return;
    dataBackupStatusBox.textContent = "正在创建备份...";
    const keepCount = Math.max(
      1,
      Math.min(200, Number(dataBackupKeepCountInput?.value || 20) || 20)
    );
    const res = await fetch("/api/admin/data-backups/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ keepCount }),
    });
    const data = await res.json();
    if (!res.ok) {
      dataBackupStatusBox.textContent = data.error || "创建备份失败";
      return;
    }
    dataBackupStatusBox.textContent = `备份已创建：${data.backup?.id || ""}；已保留最近 ${
      data.keepCount || keepCount
    } 份，清理 ${data.prune?.removedCount || 0} 份。`;
    await loadDataBackups();
    await loadAuditLog();
  });

  refreshDataBackupBtn?.addEventListener("click", async () => {
    try {
      await loadDataBackups();
    } catch (error) {
      if (dataBackupStatusBox) dataBackupStatusBox.textContent = error?.message || "读取失败";
    }
  });

  downloadDataBackupBtn?.addEventListener("click", () => {
    const backupId = String(dataBackupSelect?.value || "");
    if (!backupId) {
      if (dataBackupStatusBox) dataBackupStatusBox.textContent = "请先选择备份。";
      return;
    }
    const params = new URLSearchParams({ backupId });
    window.open(`/api/admin/data-backups/download?${params.toString()}`, "_blank");
    if (dataBackupStatusBox) dataBackupStatusBox.textContent = `开始下载：${backupId}`;
  });

  restoreDataBackupBtn?.addEventListener("click", async () => {
    const backupId = String(dataBackupSelect?.value || "");
    if (!backupId) {
      if (dataBackupStatusBox) dataBackupStatusBox.textContent = "请先选择备份。";
      return;
    }
    if (!confirm(`确认恢复该备份？恢复后将覆盖当前数据：${backupId}`)) return;
    if (dataBackupStatusBox) dataBackupStatusBox.textContent = "恢复中...";
    const res = await fetch("/api/admin/data-backups/restore", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ backupId }),
    });
    const data = await res.json();
    if (!res.ok) {
      if (dataBackupStatusBox) dataBackupStatusBox.textContent = data.error || "恢复失败";
      return;
    }
    if (dataBackupStatusBox)
      dataBackupStatusBox.textContent = `恢复完成：${data.backupId || backupId}`;
    await loadStatus();
    await loadAuditLog();
  });

  pruneDataBackupBtn?.addEventListener("click", async () => {
    const keepCount = Math.max(
      1,
      Math.min(200, Number(dataBackupKeepCountInput?.value || 20) || 20)
    );
    if (!confirm(`确认清理旧备份？仅保留最近 ${keepCount} 份。`)) return;
    if (dataBackupStatusBox) dataBackupStatusBox.textContent = "正在清理旧备份...";
    const res = await fetch("/api/admin/data-backups/prune", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ keepCount }),
    });
    const data = await res.json();
    if (!res.ok) {
      if (dataBackupStatusBox) dataBackupStatusBox.textContent = data.error || "清理失败";
      return;
    }
    if (dataBackupStatusBox)
      dataBackupStatusBox.textContent = `清理完成：保留 ${data.keepCount} 份，删除 ${data.removedCount} 份。`;
    await loadDataBackups();
    await loadAuditLog();
  });

  saveDataBackupConfigBtn?.addEventListener("click", async () => {
    const keepCount = Math.max(
      1,
      Math.min(200, Number(dataBackupKeepCountInput?.value || 20) || 20)
    );
    const autoBackupEnabled = Boolean(autoBackupEnabledInput?.checked);
    const autoBackupHour = Math.max(0, Math.min(23, Number(autoBackupHourInput?.value || 3) || 3));
    const autoBackupMinute = Math.max(
      0,
      Math.min(59, Number(autoBackupMinuteInput?.value || 0) || 0)
    );
    if (dataBackupStatusBox) dataBackupStatusBox.textContent = "正在保存保留设置...";
    const res = await fetch("/api/admin/data-backups/config/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        keepCount,
        autoBackupEnabled,
        autoBackupHour,
        autoBackupMinute,
      }),
    });
    const data = await res.json();
    if (!res.ok) {
      if (dataBackupStatusBox) dataBackupStatusBox.textContent = data.error || "保存失败";
      return;
    }
    if (dataBackupKeepCountInput) dataBackupKeepCountInput.value = String(data.keepCount || keepCount);
    if (dataBackupStatusBox)
      dataBackupStatusBox.textContent = `设置已保存：默认保留最近 ${data.keepCount || keepCount} 份；自动备份${
        data.autoBackupEnabled ? "已开启" : "已关闭"
      }（${String(data.autoBackupHour ?? 3).padStart(2, "0")}:${String(
        data.autoBackupMinute ?? 0
      ).padStart(2, "0")}）。`;
    await loadAuditLog();
  });

  runAutoBackupNowBtn?.addEventListener("click", async () => {
    if (dataBackupStatusBox) dataBackupStatusBox.textContent = "正在执行自动备份测试...";
    const res = await fetch("/api/admin/data-backups/auto-run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok) {
      if (dataBackupStatusBox) dataBackupStatusBox.textContent = data.error || "执行失败";
      return;
    }
    if (dataBackupStatusBox) {
      dataBackupStatusBox.textContent = `测试执行完成：新备份 ${data.backup?.id || ""}；保留 ${
        data.keepCount || 0
      } 份，清理 ${data.prune?.removedCount || 0} 份。`;
    }
    await loadDataBackups();
    await loadAuditLog();
  });

  refreshAuditLogBtn?.addEventListener("click", async () => {
    try {
      await loadAuditLog();
    } catch (error) {
      if (auditLogBox) auditLogBox.textContent = error?.message || "读取失败";
    }
  });

  saveSystemOpenAiKeyBtn?.addEventListener("click", async () => {
    const apiKey = String(systemOpenAiKeyInput?.value || "").trim();
    if (!apiKey) {
      if (systemOpenAiKeyStatusBox) systemOpenAiKeyStatusBox.textContent = "请先输入 GPT Key。";
      return;
    }
    if (systemOpenAiKeyStatusBox) systemOpenAiKeyStatusBox.textContent = "保存中...";
    const res = await fetch("/api/admin/system/openai-key/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ apiKey }),
    });
    const data = await res.json();
    if (!res.ok) {
      if (systemOpenAiKeyStatusBox) systemOpenAiKeyStatusBox.textContent = data.error || "保存失败";
      return;
    }
    if (systemOpenAiKeyInput) systemOpenAiKeyInput.value = "";
    if (systemOpenAiKeyStatusBox) {
      systemOpenAiKeyStatusBox.textContent = `保存成功：${data.masked || "已配置"}${
        data.warning ? `\n${data.warning}` : ""
      }`;
    }
    await loadSystemOpenAiKeyStatus();
  });

  clearSystemOpenAiKeyBtn?.addEventListener("click", async () => {
    if (!confirm("确认清空后台已保存的 GPT Key？")) return;
    if (systemOpenAiKeyStatusBox) systemOpenAiKeyStatusBox.textContent = "清空中...";
    const res = await fetch("/api/admin/system/openai-key/clear", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const data = await res.json();
    if (!res.ok) {
      if (systemOpenAiKeyStatusBox) systemOpenAiKeyStatusBox.textContent = data.error || "清空失败";
      return;
    }
    if (systemOpenAiKeyInput) systemOpenAiKeyInput.value = "";
    await loadSystemOpenAiKeyStatus();
  });

  refreshSystemOpenAiKeyBtn?.addEventListener("click", async () => {
    try {
      await loadSystemOpenAiKeyStatus();
    } catch (error) {
      if (systemOpenAiKeyStatusBox) {
        systemOpenAiKeyStatusBox.textContent = error?.message || "读取失败";
      }
    }
  });

  refreshBtn?.addEventListener("click", loadStatus);
  await loadStatus();
  await loadDataBackups().catch((error) => {
    if (dataBackupStatusBox) dataBackupStatusBox.textContent = error?.message || "读取失败";
  });
  await loadAuditLog().catch((error) => {
    if (auditLogBox) auditLogBox.textContent = error?.message || "读取失败";
  });
  await loadSystemOpenAiKeyStatus().catch((error) => {
    if (systemOpenAiKeyStatusBox) {
      systemOpenAiKeyStatusBox.textContent = error?.message || "读取失败";
    }
    });
}

async function refreshScriptureVersionsList() {
  const res = await fetch("/api/admin/scripture-versions", {
    cache: "no-store",
  });
  const data = await res.json();

  if (!res.ok) {
    const listBox = document.getElementById("scriptureVersionsListBox");
    if (listBox) {
      listBox.innerHTML = `<div class="empty-state">读取失败：${escapeHtml(
        data.error || "未知错误"
      )}</div>`;
    }
    return;
  }

  adminState.scriptureVersions = data.scriptureVersions || [];
  renderScriptureVersionsList();
}

function renderScriptureVersionsList() {
  const box = document.getElementById("scriptureVersionsListBox");
  if (!box) return;

  if (!adminState.scriptureVersions.length) {
    box.innerHTML = `<div class="empty-state">暂无圣经版本。</div>`;
    return;
  }

  box.innerHTML = adminState.scriptureVersions
    .slice()
    .sort((a, b) => Number(a.sortOrder || 999) - Number(b.sortOrder || 999))
    .map((item) => {
      return `
        <div class="test-result-seg">
          <h4>${escapeHtml(item.label || item.id)} (${escapeHtml(item.id)})</h4>
          <div class="test-result-line"><strong>语言：</strong>${escapeHtml(
            item.lang || "—"
          )}</div>
          <div class="test-result-line"><strong>sourceType：</strong>${escapeHtml(
            item.sourceType || "—"
          )}</div>
          <div class="test-result-line"><strong>sourceFile：</strong>${escapeHtml(
            item.sourceFile || "—"
          )}</div>
          <div class="test-result-line"><strong>状态：</strong>
            enabled=${escapeHtml(String(item.enabled !== false))}
            ，uiEnabled=${escapeHtml(String(item.uiEnabled !== false))}
            ，scriptureEnabled=${escapeHtml(
              String(item.scriptureEnabled !== false)
            )}
          </div>
          <div class="modal-actions" style="margin-top:10px;">
            <button class="secondary-btn" type="button" data-edit-scripture-version="${escapeHtml(
              item.id
            )}">编辑</button>
          </div>
        </div>
      `;
    })
    .join("");

  box.querySelectorAll("[data-edit-scripture-version]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = btn.getAttribute("data-edit-scripture-version");
      const item = adminState.scriptureVersions.find((x) => x.id === id);
      if (item) fillScriptureVersionEditor(item);
    });
  });
}

function clearScriptureVersionEditor() {
  adminState.editingScriptureVersionId = "";

  setVal("svId", "");
  setVal("svLabel", "");
  setVal("svLang", "");
  setVal("svSourceType", "usfx");
  setVal("svSourceFile", "");
  setVal("svDescription", "");
  setVal("svSortOrder", "999");
  setVal("svContentMode", "native");

  setChecked("svEnabled", true);
  setChecked("svUiEnabled", true);
  setChecked("svContentEnabled", true);
  setChecked("svScriptureEnabled", true);

  const resultBox = document.getElementById("scriptureVersionEditorResult");
  if (resultBox) resultBox.textContent = "已切换到新建模式。";
}

function fillScriptureVersionEditor(item) {
  adminState.editingScriptureVersionId = item.id || "";

  setVal("svId", item.id || "");
  setVal("svLabel", item.label || "");
  setVal("svLang", item.lang || "");
  setVal("svSourceType", item.sourceType || "usfx");
  setVal("svSourceFile", item.sourceFile || "");
  setVal("svDescription", item.description || "");
  setVal("svSortOrder", String(item.sortOrder ?? 999));
  setVal("svContentMode", item.contentMode || "native");

  setChecked("svEnabled", item.enabled !== false);
  setChecked("svUiEnabled", item.uiEnabled !== false);
  setChecked("svContentEnabled", item.contentEnabled !== false);
  setChecked("svScriptureEnabled", item.scriptureEnabled !== false);

  const resultBox = document.getElementById("scriptureVersionEditorResult");
  if (resultBox) resultBox.textContent = `已载入版本：${item.id}`;
}

function collectScriptureVersionFromEditor() {
  return {
    id: getVal("svId"),
    label: getVal("svLabel"),
    lang: getVal("svLang"),
    sourceType: getVal("svSourceType"),
    sourceFile: getVal("svSourceFile"),
    description: getVal("svDescription"),
    sortOrder: Number(getVal("svSortOrder") || 999),
    contentMode: getVal("svContentMode") || "native",
    enabled: getChecked("svEnabled"),
    uiEnabled: getChecked("svUiEnabled"),
    contentEnabled: getChecked("svContentEnabled"),
    scriptureEnabled: getChecked("svScriptureEnabled"),
  };
}

async function saveScriptureVersionFromEditor() {
  const payload = collectScriptureVersionFromEditor();
  const resultBox = document.getElementById("scriptureVersionEditorResult");
  if (resultBox) resultBox.textContent = "正在保存圣经版本...";

  const res = await fetch("/api/admin/scripture-version/save", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      scriptureVersion: payload,
    }),
  });

  const data = await res.json();

  if (!res.ok) {
    if (resultBox)
      resultBox.textContent = `保存失败：${data.error || "未知错误"}`;
    return;
  }

  if (resultBox) {
    resultBox.textContent = `保存成功：${
      data.scriptureVersion?.id || payload.id
    }`;
  }

  await reloadScriptureVersionsEverywhere();
  adminState.editingScriptureVersionId =
    data.scriptureVersion?.id || payload.id;
}

async function deleteCurrentScriptureVersion() {
  const id = getVal("svId");
  const resultBox = document.getElementById("scriptureVersionEditorResult");

  if (!id) {
    if (resultBox) resultBox.textContent = "请先载入一个版本再删除。";
    return;
  }

  if (!confirm(`确认删除圣经版本：${id}？`)) return;

  if (resultBox) resultBox.textContent = "正在删除圣经版本...";

  const params = new URLSearchParams({ id });
  const res = await fetch(`/api/admin/scripture-version?${params.toString()}`, {
    method: "DELETE",
  });

  const data = await res.json();

  if (!res.ok) {
    if (resultBox)
      resultBox.textContent = `删除失败：${data.error || "未知错误"}`;
    return;
  }

  if (resultBox) resultBox.textContent = `删除成功：${id}`;
  clearScriptureVersionEditor();
  await reloadScriptureVersionsEverywhere();
}

async function reloadScriptureVersionsEverywhere() {
  await loadBootstrap();

  if (adminState.bootstrap) {
    const token = getAuthToken();
    const adminRes = await fetch("/api/admin/bootstrap", {
      cache: "no-store",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
    const adminData = await adminRes.json();
    if (adminRes.ok) {
      adminState.bootstrap = adminData;
      adminState.scriptureVersions = adminData.scriptureVersions || [];
    }
  }

  renderAllSelectors();
  await refreshCurrentPage();
  renderScriptureVersionsList();
}

/* =========================
   小工具
   ========================= */
function getVal(id) {
  return document.getElementById(id)?.value?.trim?.() || "";
}

function setVal(id, value) {
  const el = document.getElementById(id);
  if (el) el.value = value;
}

function getChecked(id) {
  return !!document.getElementById(id)?.checked;
}

function setChecked(id, value) {
  const el = document.getElementById(id);
  if (el) el.checked = !!value;
}

/* =========================
   定时刷新
   ========================= */
function startJobsAutoRefresh() {
  stopJobsAutoRefresh();
  adminState.jobsRefreshTimer = setInterval(() => {
    refreshJobsList().catch((error) => {
      console.error("刷新任务失败:", error);
    });
  }, 3000);
}

function stopJobsAutoRefresh() {
  if (adminState.jobsRefreshTimer) {
    clearInterval(adminState.jobsRefreshTimer);
    adminState.jobsRefreshTimer = null;
  }
}

function registerServiceWorker() {
  if (!("serviceWorker" in navigator)) return;
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/sw.js").catch((error) => {
      console.warn("Service worker register failed:", error);
    });
  });
}

registerServiceWorker();
init();
