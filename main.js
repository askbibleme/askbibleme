import * as OpenCC from "/node_modules/opencc-js/dist/esm/cn2t.js";

const FRONT_STATE_KEY = "bible_front_state_v4";
const FONT_SCALE_KEY = "bible_font_scale_v1";
const VIEWPORT_SCROLL_KEY = "bible_viewport_scroll_v1";
const FAVORITES_KEY = "bible_verse_favorites_v1";
const QUESTION_FAVORITES_KEY = "bible_question_favorites_v1";
const PENDING_FAVORITE_FOCUS_KEY = "bible_pending_favorite_focus_v1";
const PENDING_QUESTION_FOCUS_KEY = "bible_pending_question_focus_v1";
const GLOBAL_SYNC_VERSE_KEYS = "bible_global_sync_verse_keys_v1";
const GLOBAL_SYNC_QUESTION_KEYS = "bible_global_sync_question_keys_v1";
const LAST_QUESTION_SUBMIT_AT_KEY = "bible_last_question_submit_at_v1";
const USER_AUTH_TOKEN_KEY = "bible_user_auth_token_v1";
const ADMIN_PASSWORD = "0777";
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
  questionFavorites: loadQuestionFavorites(),
  questionFavoriteKeys: new Set(),
  approvedChapterQuestions: [],
  currentUser: null,
};

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

function loadPublishHistory() {
  const parsed = safeJsonParse(localStorage.getItem(PUBLISH_HISTORY_KEY), []);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .map((x) => ({
      at: String(x?.at || ""),
      mode: String(x?.mode || "all"),
      dryRun: x?.dryRun === true,
      onlyChanged: x?.onlyChanged !== false,
      matchedPairs: Number(x?.matchedPairs || 0),
      totalPublishedCount: Number(x?.totalPublishedCount || 0),
      totalSkippedCount: Number(x?.totalSkippedCount || 0),
      changeCount: Number(x?.changeCount || 0),
    }))
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

function escapeHtml(str) {
  return String(str ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
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
      : ["bbe_en"],
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
      triggerBook: "Book",
      triggerTranslation: "Translation",
      triggerVersion: "Version",
      bookChapter: "Book / Chapter",
      display: "Display",
      type: "Type",
      bibleVersion: "Bible Version",
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
      clearFavorites: "Clear Favorites",
      emptyFavorites: "No favorites yet. Double-click a verse to save it.",
      brandSubtitle:
        "Ask, and it will be given to you; seek, and you will find; knock, and the door will be opened to you. Matthew 7:7-8",
      prevChapter: "Previous",
      nextChapter: "Next",
      noContent: "No content yet for this chapter in the selected version/language.",
    };
  }
  if (lang === "es") {
    return {
      triggerBook: "Libro",
      triggerTranslation: "Traduccion",
      triggerVersion: "Version",
      bookChapter: "Libro / Capitulo",
      display: "Mostrar",
      type: "Tipo",
      bibleVersion: "Version biblica",
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
      clearFavorites: "Limpiar favoritos",
      emptyFavorites: "Aun no hay favoritos. Haz doble clic en un versiculo para guardarlo.",
      brandSubtitle:
        "Pidan, y se les dara; busquen, y encontraran; llamen, y se les abrira. Mateo 7:7-8",
      prevChapter: "Anterior",
      nextChapter: "Siguiente",
      noContent: "Aun no hay contenido para este capitulo en la version/idioma seleccionados.",
    };
  }
  if (lang === "he") {
    return {
      triggerBook: "ספר",
      triggerTranslation: "תרגום",
      triggerVersion: "גרסה",
      bookChapter: "ספר / פרק",
      display: "תצוגה",
      type: "סוג",
      bibleVersion: "גרסת מקרא",
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
      clearFavorites: "נקה מועדפים",
      emptyFavorites: "אין עדיין מועדפים. לחיצה כפולה על פסוק תשמור אותו.",
      brandSubtitle:
        "בקשו וינתן לכם; חפשו ותמצאו; דפקו ויפתח לכם. מתי 7:7-8",
      prevChapter: "הקודם",
      nextChapter: "הבא",
      noContent: "עדיין אין תוכן לפרק זה בגרסה או בשפה שנבחרו.",
    };
  }
  return {
    triggerBook: "书卷",
    triggerTranslation: "译本",
    triggerVersion: "版本",
    bookChapter: "书卷章节",
    display: "显示",
    type: "类型",
    bibleVersion: "圣经版本",
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
    clearFavorites: "清空收藏",
    emptyFavorites: "还没有收藏，双击经文即可收藏。",
    brandSubtitle:
      "你们祈求，就⋯ 寻找，就⋯ 叩门，就⋯ 马太福音 7:7-8",
    prevChapter: "上一章",
    nextChapter: "下一章",
    noContent: "这一章还没有该版本 / 该语言的内容。",
  };
}

function applyReaderI18n() {
  const copy = getLocalizedCopy();
  const setText = (selector, value) => {
    const el = document.querySelector(selector);
    if (el) el.textContent = value;
  };

  setText("#bookChapterPanel .toolbar-panel-title", copy.bookChapter);
  setText("#contentTypePanel .toolbar-panel-title", copy.triggerVersion || "版本");
  setText("#qaViewPanel .toolbar-panel-title", copy.displayContent);
  setText("#primaryVersionPanel .toolbar-panel-title", copy.bibleVersion);
  setText('#bookChapterPanel [data-testament-tab="旧约"]', copy.oldTestament);
  setText('#bookChapterPanel [data-testament-tab="新约"]', copy.newTestament);
  setText("#bookChapterPanel .chapter-grid-title", copy.chapters);
  setText("#qaViewPanel .chapter-grid-title", copy.quickActions);
  setText("#primaryVersionSectionTitle", copy.primaryVersionSingle);
  setText("#compareVersionSectionTitle", copy.compareVersionMulti);
  setText("#exportPrettyPdfBtn", copy.export);
  setText("#exportPrintPdfBtn", copy.print);
  const brandEl = document.querySelector("#brandSubtitle");
  if (brandEl) {
    const safe = escapeHtml(copy.brandSubtitle || "");
    brandEl.innerHTML = safe
      .replaceAll("⋯", '<span class="brand-ellipsis">⋯</span>')
      .replaceAll("…", '<span class="brand-ellipsis">⋯</span>');
  }
  setText("#favoritesPanelTitle", copy.favoritesTitle);
  setText("#clearFavoritesBtn", copy.clearFavorites);

  document
    .querySelectorAll(
      "#bookChapterPanel .toolbar-panel-close, #contentTypePanel .toolbar-panel-close, #qaViewPanel .toolbar-panel-close, #primaryVersionPanel .toolbar-panel-close, #favoritesPanel .toolbar-panel-close"
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

function renderFavoritesPanel() {
  const list = document.getElementById("favoritesList");
  if (!list) return;
  const copy = getLocalizedCopy();
  const items = [...(state.favorites || [])].sort(
    (a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0)
  );
  if (!items.length) {
    list.innerHTML = `<div class="empty-state">${escapeHtml(copy.emptyFavorites)}</div>`;
  } else {
    list.innerHTML = items
      .map((item) => {
        const title = `${getLocalizedBookLabelById(item.bookId)} ${item.chapter}:${item.verse}`;
        const versionLabel = getScriptureVersionById(item.versionId)?.label || item.versionId;
        return `<div class="favorite-item">
          <button type="button" class="favorite-jump-btn" data-favorite-jump="${escapeHtml(
            item.key
          )}">
            <div class="favorite-ref">${escapeHtml(title)}</div>
            <div class="favorite-text">${escapeHtml(item.text)}</div>
            <div class="favorite-meta">${escapeHtml(versionLabel)}</div>
          </button>
          <button type="button" class="favorite-remove-btn" data-favorite-remove="${escapeHtml(
            item.key
          )}">×</button>
        </div>`;
      })
      .join("");
  }

  list.querySelectorAll("[data-favorite-jump]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.getAttribute("data-favorite-jump");
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

  list.querySelectorAll("[data-favorite-remove]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      const key = btn.getAttribute("data-favorite-remove");
      if (!key) return;
      state.favorites = (state.favorites || []).filter((x) => x.key !== key);
      saveFavorites();
      renderToolbarTriggers();
      renderFavoritesPanel();
    });
  });
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
    initChapterNav();
    initInlineDisplayToggles();
    initChapterQuestionCollector();
    initAuthModal();
    initAdminModal();
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

function getAuthToken() {
  return String(localStorage.getItem(USER_AUTH_TOKEN_KEY) || "");
}

function setAuthToken(token) {
  if (token) localStorage.setItem(USER_AUTH_TOKEN_KEY, token);
  else localStorage.removeItem(USER_AUTH_TOKEN_KEY);
}

async function fetchCurrentUser() {
  const token = getAuthToken();
  if (!token) {
    state.currentUser = null;
    renderAuthStatus();
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
      state.currentUser = data.user || null;
    }
  } catch {
    state.currentUser = null;
  }
  renderAuthStatus();
}

function renderAuthStatus() {
  const userNameEl = document.getElementById("authUserName");
  const openBtn = document.getElementById("openAuthBtn");
  const logoutBtn = document.getElementById("logoutAuthBtn");
  const sideUserTagEl = document.getElementById("sideUserTag");
  const name = String(state.currentUser?.name || "");
  const authed = Boolean(name);
  if (userNameEl) {
    userNameEl.style.display = authed ? "" : "none";
    userNameEl.textContent = authed ? `你好，${name}` : "";
  }
  if (openBtn) openBtn.style.display = authed ? "none" : "";
  if (logoutBtn) logoutBtn.style.display = authed ? "" : "none";
  if (sideUserTagEl) {
    sideUserTagEl.textContent = authed ? name : "更多";
    sideUserTagEl.setAttribute(
      "aria-label",
      authed ? `用户：${name}` : "用户注册/登录，解锁更多宝藏"
    );
    sideUserTagEl.setAttribute("href", authed ? "/notebook.html" : "#");
  }
}

function initAuthModal() {
  const modal = document.getElementById("authModal");
  const openBtn = document.getElementById("openAuthBtn");
  const sideUserTagEl = document.getElementById("sideUserTag");
  const closeBtn = document.getElementById("closeAuthBtn");
  const logoutBtn = document.getElementById("logoutAuthBtn");
  const modeLoginBtn = document.getElementById("authModeLoginBtn");
  const modeRegisterBtn = document.getElementById("authModeRegisterBtn");
  const submitBtn = document.getElementById("authSubmitBtn");
  const titleEl = document.getElementById("authModalTitle");
  const hintEl = document.getElementById("authHintBar");
  const nameFieldEl = document.getElementById("authNameField");
  const nameInput = document.getElementById("authNameInput");
  const emailInput = document.getElementById("authEmailInput");
  const passwordInput = document.getElementById("authPasswordInput");
  const errEl = document.getElementById("authErrorText");
  let authMode = "login";

  function applyAuthMode(mode) {
    authMode = mode === "register" ? "register" : "login";
    if (titleEl) titleEl.textContent = authMode === "register" ? "用户注册" : "用户登录";
    if (submitBtn) submitBtn.textContent = authMode === "register" ? "注册" : "登录";
    if (nameFieldEl) nameFieldEl.style.display = authMode === "register" ? "" : "none";
    if (modeLoginBtn) modeLoginBtn.classList.toggle("active", authMode === "login");
    if (modeRegisterBtn) modeRegisterBtn.classList.toggle("active", authMode === "register");
    if (errEl) errEl.textContent = "";
    if (hintEl) hintEl.textContent = "登录后会解锁更多宝藏";
  }

  const open = (mode = "login") => {
    applyAuthMode(mode);
    if (modal) modal.style.display = "block";
  };
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
  sideUserTagEl?.addEventListener("click", (event) => {
    if (state.currentUser?.name) return;
    event.preventDefault();
    open("register");
  });
  closeBtn?.addEventListener("click", close);
  modeLoginBtn?.addEventListener("click", () => applyAuthMode("login"));
  modeRegisterBtn?.addEventListener("click", () => applyAuthMode("register"));
  submitBtn?.addEventListener("click", submit);
  logoutBtn?.addEventListener("click", async () => {
    const token = getAuthToken();
    try {
      await fetch("/api/auth/logout", {
        method: "POST",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
    } catch {}
    setAuthToken("");
    state.currentUser = null;
    renderAuthStatus();
  });

  fetchCurrentUser();
  applyAuthMode("login");
}

function initChapterQuestionCollector() {
  const inputEl = document.getElementById("chapterQuestionInput");
  const submitBtn = document.getElementById("submitChapterQuestionBtn");
  const statusEl = document.getElementById("chapterQuestionStatus");
  if (!inputEl || !submitBtn || !statusEl) return;

  submitBtn.addEventListener("click", async () => {
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
      if (!state.currentUser) {
        throw new Error("请先登录后再提交");
      }
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
  normalizeFrontStateByBootstrap();
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

function initFontTools() {
  document.getElementById("fontDecreaseBtn")?.addEventListener("click", () => {
    state.frontState.fontScale = Math.max(
      0.85,
      Number((state.frontState.fontScale - 0.05).toFixed(2))
    );
    applyFontScale();
    saveFontScale();
  });

  document.getElementById("fontIncreaseBtn")?.addEventListener("click", () => {
    state.frontState.fontScale = Math.min(
      1.3,
      Number((state.frontState.fontScale + 0.05).toFixed(2))
    );
    applyFontScale();
    saveFontScale();
  });
}

function initExportButtons() {
  document
    .getElementById("exportPrettyPdfBtn")
    ?.addEventListener("click", () => {
      alert("这一步先保留按钮，后面再接导出新版内容。");
    });

  document
    .getElementById("exportPrintPdfBtn")
    ?.addEventListener("click", () => {
      window.print();
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
    { triggerId: "contentTypeTrigger", panelId: "contentTypePanel" },
    { triggerId: "primaryVersionTrigger", panelId: "primaryVersionPanel" },
    { triggerId: "favoritesTrigger", panelId: "favoritesPanel" },
  ];

  triggerMap.forEach(({ triggerId, panelId }) => {
    const trigger = document.getElementById(triggerId);
    const panel = document.getElementById(panelId);
    if (!trigger || !panel) return;

    trigger.addEventListener("click", (event) => {
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
}

function toggleToolbarPanel(panelId) {
  const panel = document.getElementById(panelId);
  if (!panel) return;

  const willOpen = panel.hasAttribute("hidden");
  closeAllToolbarPanels();

  if (willOpen) {
    panel.removeAttribute("hidden");
    markToolbarTriggerActive(panelId, true);
  }
}

function closeToolbarPanel(panelId) {
  const panel = document.getElementById(panelId);
  if (!panel) return;
  panel.setAttribute("hidden", "");
  markToolbarTriggerActive(panelId, false);
}

function closeAllToolbarPanels() {
  ["bookChapterPanel", "contentTypePanel", "primaryVersionPanel", "favoritesPanel"].forEach(
    (panelId) => {
    const panel = document.getElementById(panelId);
    if (panel) panel.setAttribute("hidden", "");
    markToolbarTriggerActive(panelId, false);
    }
  );
}

function markToolbarTriggerActive(panelId, active) {
  const mapping = {
    bookChapterPanel: "bookChapterTrigger",
    contentTypePanel: "contentTypeTrigger",
    primaryVersionPanel: "primaryVersionTrigger",
    favoritesPanel: "favoritesTrigger",
  };

  const trigger = document.getElementById(mapping[panelId]);
  if (trigger) trigger.classList.toggle("active", !!active);
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

function renderToolbarTriggers() {
  const bookChapterTriggerText = document.getElementById(
    "bookChapterTriggerText"
  );
  const contentTypeTriggerText = document.getElementById(
    "contentTypeTriggerText"
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

  if (contentTypeTriggerText) {
    contentTypeTriggerText.textContent = copy.triggerVersion || "版本";
  }

  if (qaViewTriggerText) {
    qaViewTriggerText.textContent = copy.display;
  }

  if (primaryVersionTriggerText) {
    primaryVersionTriggerText.textContent = copy.triggerTranslation || "译本";
  }
  if (favoritesTriggerText) {
    favoritesTriggerText.textContent = `${copy.favorites} (${(state.favorites || []).length})`;
  }

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
  renderContentTypePanel();
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
  const bookGrid = document.getElementById("bookGrid");
  const chapterGrid = document.getElementById("chapterGrid");
  if (!bookGrid || !chapterGrid) return;

  document.querySelectorAll("[data-testament-tab]").forEach((btn) => {
    const isActive =
      btn.getAttribute("data-testament-tab") === state.frontState.testament;
    btn.classList.toggle("active", isActive);

    if (!btn.dataset.boundTab) {
      btn.dataset.boundTab = "1";
      btn.addEventListener("click", async () => {
        const nextTestament = btn.getAttribute("data-testament-tab");
        if (!nextTestament || nextTestament === state.frontState.testament)
          return;

        state.frontState.testament = nextTestament;
        const books = getBooksForCurrentTestament();
        if (books[0]) {
          state.frontState.bookId = books[0].bookId;
          state.frontState.chapter = 1;
        }

        saveFrontState();
        renderAllSelectors();
        await refreshCurrentPage();
      });
    }
  });

  const books = getBooksForCurrentTestament();
  const scriptureLang = getPrimaryScriptureLang();

  bookGrid.innerHTML = books
    .map((book) => {
      const label =
        scriptureLang === "en" || scriptureLang === "es" || scriptureLang === "he"
          ? BOOK_NAME_EN_BY_ID[book.bookId] || book.bookEn || book.bookCn || book.bookId
          : book.bookCn || book.bookEn || book.bookId;

      const active = book.bookId === state.frontState.bookId ? "active" : "";

      return `<button type="button" class="book-item ${active}" data-book-grid-id="${escapeHtml(
        book.bookId
      )}">${escapeHtml(label)}</button>`;
    })
    .join("");

  bookGrid.querySelectorAll("[data-book-grid-id]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const nextBookId = btn.getAttribute("data-book-grid-id");
      if (!nextBookId) return;

      state.frontState.bookId = nextBookId;
      state.frontState.chapter = 1;
      saveFrontState();
      renderAllSelectors();
    });
  });

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
      closeToolbarPanel("bookChapterPanel");
      await refreshCurrentPage();
    });
  });
}

function renderContentTypePanel() {
  const list = document.getElementById("contentTypeList");
  if (!list) return;

  const options = state.bootstrap?.contentVersions || [];

  list.innerHTML = options
    .map((item) => {
      const active =
        item.id === state.frontState.contentVersion ? "active" : "";
      return `<button type="button" class="option-item ${active}" data-content-type-id="${escapeHtml(
        item.id
      )}">${escapeHtml(getLocalizedContentVersionLabel(item))}</button>`;
    })
    .join("");

  list.querySelectorAll("[data-content-type-id]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const nextId = btn.getAttribute("data-content-type-id");
      if (!nextId) return;

      state.frontState.contentVersion = nextId;
      saveFrontState();
      renderAllSelectors();
      closeToolbarPanel("contentTypePanel");
      await loadStudyContent();
      renderStudyContent();
    });
  });

}

function renderPrimaryVersionPanel() {
  const list = document.getElementById("primaryVersionList");
  if (!list) return;
  const copy = getLocalizedCopy();
  const primaryTitleEl = document.getElementById("primaryVersionSectionTitle");
  const compareTitleEl = document.getElementById("compareVersionSectionTitle");
  if (primaryTitleEl) primaryTitleEl.textContent = copy.primaryVersionSingle;
  if (compareTitleEl) compareTitleEl.textContent = copy.compareVersionMulti;

  const options = getPrimaryVersionCandidates();

  list.innerHTML = options
    .map((item) => {
      const active =
        item.id === state.frontState.primaryScriptureVersionId ? "active" : "";
      const langTag = item.lang ? ` [${item.lang}]` : "";
      return `<button type="button" class="version-item ${active}" data-primary-version-id="${escapeHtml(
        item.id
      )}">${escapeHtml(item.label + langTag)}</button>`;
    })
    .join("");

  list.querySelectorAll("[data-primary-version-id]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const nextId = btn.getAttribute("data-primary-version-id");
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
  });
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
    bookChapterTriggerText.textContent = "书卷";
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
            .map(
              (item, idx) =>
                `<div class="chapter-approved-item">${idx + 1}. ${escapeHtml(
                  String(item.questionText || "")
                )}${renderApprovedContributorMeta(item)}</div>`
            )
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
          .map(
            (item, idx) =>
              `<div class="chapter-approved-item">${idx + 1}. ${escapeHtml(
                String(item.questionText || "")
              )}${renderApprovedContributorMeta(item)}</div>`
          )
          .join("")
      : "";
  }
}

function renderApprovedContributorMeta(item) {
  const nickname = String(item?.userNickname || item?.userName || "").trim();
  const level = Number(item?.userLevel || 0);
  if (!nickname && !level) return "";
  const bits = [];
  if (nickname) bits.push(nickname);
  if (level > 0) bits.push(`L${level}`);
  return ` <span class="chapter-approved-meta">（${escapeHtml(bits.join(" "))}）</span>`;
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

function initFavorites() {
  document.addEventListener("dblclick", (event) => {
    const unit = event.target?.closest?.("[data-favorite-key]");
    if (!unit) return;
    const key = unit.getAttribute("data-favorite-key");
    const versionId = unit.getAttribute("data-favorite-version-id");
    const verse = Number(unit.getAttribute("data-favorite-verse") || 0);
    const text = unit.getAttribute("data-favorite-text") || "";
    if (!key || !versionId || !verse || !text) return;

    if (state.favoriteKeys.has(key)) {
      state.favorites = (state.favorites || []).filter((x) => x.key !== key);
      unit.classList.remove("is-favorited");
      reportGlobalFavoriteToggle({
        action: "remove",
        type: "verse",
        key,
      });
    } else {
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
    }
    saveFavorites();
    renderToolbarTriggers();
    renderFavoritesPanel();
  });

  document.addEventListener("dblclick", (event) => {
    const item = event.target?.closest?.("[data-question-fav-key]");
    if (!item) return;
    const key = item.getAttribute("data-question-fav-key");
    const question = item.getAttribute("data-question-fav-text") || "";
    const title = item.getAttribute("data-question-fav-title") || "";
    if (!key || !question) return;

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
    } else {
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
    }
    saveQuestionFavorites();
  });

  document.getElementById("clearFavoritesBtn")?.addEventListener("click", () => {
    state.favorites = [];
    saveFavorites();
    renderToolbarTriggers();
    renderFavoritesPanel();
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
              return `<li class="question-fav-item${active}" data-question-fav-key="${escapeHtml(
                qKey
              )}" data-question-fav-text="${escapeHtml(qText)}" data-question-fav-title="${escapeHtml(
                title
              )}">${escapeHtml(qText)}</li>`;
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
function initAdminModal() {
  const passwordModal = document.getElementById("adminPasswordModal");
  const adminModal = document.getElementById("adminModal");
  const openAdminBtn = document.getElementById("openAdminBtn");
  const closeAdminPasswordBtn = document.getElementById(
    "closeAdminPasswordBtn"
  );
  const submitAdminPasswordBtn = document.getElementById(
    "submitAdminPasswordBtn"
  );
  const closeAdminBtn = document.getElementById("closeAdminBtn");
  const adminPasswordInput = document.getElementById("adminPasswordInput");
  const adminPasswordError = document.getElementById("adminPasswordError");

  async function openPasswordModal() {
    if (adminPasswordInput) adminPasswordInput.value = "";
    if (adminPasswordError) adminPasswordError.textContent = "";
    if (passwordModal) passwordModal.style.display = "block";
  }

  async function openAdminRealModal() {
    await fetchCurrentUser();
    await loadAdminBootstrap();
    bindAdminTabs();
    await initRuleEditorTab();
    await initTestGenerateTab();
    await initPublishedManagerTab();
    await initScriptureVersionManagerTab();
    await initDeployManagerTab();
    await initPointsSystemTab();
    initQuestionReviewTab();
    initAdminUsersTab();
    startJobsAutoRefresh();
    if (adminModal) adminModal.style.display = "block";
  }

  openAdminBtn?.addEventListener("click", openPasswordModal);

  closeAdminPasswordBtn?.addEventListener("click", () => {
    if (passwordModal) passwordModal.style.display = "none";
  });

  submitAdminPasswordBtn?.addEventListener("click", async () => {
    if (adminPasswordInput?.value === ADMIN_PASSWORD) {
      if (passwordModal) passwordModal.style.display = "none";
      await openAdminRealModal();
    } else if (adminPasswordError) {
      adminPasswordError.textContent = "密码不正确";
    }
  });

  closeAdminBtn?.addEventListener("click", () => {
    if (adminModal) adminModal.style.display = "none";
    stopJobsAutoRefresh();
  });
}

async function loadAdminBootstrap() {
  const res = await fetch("/api/admin/bootstrap", { cache: "no-store" });
  const data = await res.json();

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
        </div>
      </div>
    </div>
    <div class="modal-actions">
      <button id="generateUpgradeCmdBtn" class="secondary-btn" type="button">生成升级包命令</button>
      <button id="generateFullCmdBtn" class="secondary-btn" type="button">生成整站包命令</button>
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
            <button id="loadPublishedChapterBtn" class="secondary-btn" type="button">查看已发布章节</button>
            <button id="deletePublishedChapterBtn" class="secondary-btn" type="button">删除已发布章节</button>
          </div>

          <div class="section-title">结果</div>
          <div id="publishedDetailBox" class="admin-preview-box">尚未读取。</div>
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

    <div class="modal-actions">
      <button id="createBookJobBtn" class="secondary-btn" type="button">生成整卷 / 范围</button>
      <button id="createOldJobBtn" class="secondary-btn" type="button">生成旧约</button>
      <button id="createNewJobBtn" class="secondary-btn" type="button">生成新约</button>
      <button id="createBibleJobBtn" class="secondary-btn" type="button">生成整本</button>
      <button id="refreshJobsBtn" class="secondary-btn" type="button">刷新任务</button>
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

  const res = await fetch("/api/admin/save-test-result", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      studyContent: adminState.testResult,
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
  const data = await res.json();

  if (!res.ok) {
    box.innerHTML = `<div class="empty-state">读取任务失败：${escapeHtml(
      data.error || "未知错误"
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

      return `
        <div class="test-result-seg">
          <h4>${escapeHtml(job.id)}</h4>
          <div class="test-result-line"><strong>状态：</strong>${escapeHtml(
            job.status || "—"
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
  const previewPublishBulkBtn = document.getElementById("previewPublishBulkBtn");
  const exportPublishChangesJsonBtn = document.getElementById(
    "exportPublishChangesJsonBtn"
  );
  const exportPublishChangesCsvBtn = document.getElementById(
    "exportPublishChangesCsvBtn"
  );
  const loadChapterBtn = document.getElementById("loadPublishedChapterBtn");
  const deleteChapterBtn = document.getElementById("deletePublishedChapterBtn");

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
    <div><strong>能力：</strong>仅发布改动 / 增量预览（dryRun）/ 改动清单导出（JSON, CSV）</div>
  `;
}

function renderLastPublishedAction() {
  const box = document.getElementById("publishedLastActionBox");
  if (!box) return;
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

function renderPublishHistory() {
  const box = document.getElementById("publishedHistoryBox");
  if (!box) return;
  const rows = adminState.publishHistory || [];
  if (!rows.length) {
    box.textContent = "暂无历史。";
    return;
  }
  box.innerHTML = rows
    .map(
      (x, idx) => `
      <div style="padding:6px 0; ${idx ? "border-top:1px solid rgba(214,203,187,.56);" : ""}">
        <div><strong>${idx + 1}.</strong> ${x.dryRun ? "增量预览" : "正式发布"}｜mode=${escapeHtml(
        String(x.mode)
      )}｜仅改动=${x.onlyChanged ? "是" : "否"}</div>
        <div style="opacity:.88;">改动 ${escapeHtml(String(x.changeCount))}，发布 ${escapeHtml(
        String(x.totalPublishedCount)
      )}，跳过 ${escapeHtml(String(x.totalSkippedCount))}，版本语言组 ${escapeHtml(
        String(x.matchedPairs)
      )}</div>
        <div style="opacity:.75;">${escapeHtml(new Date(x.at).toLocaleString())}</div>
      </div>
    `
    )
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
  const detailBox = document.getElementById("publishedDetailBox");

  if (!bookId || !chapter) {
    if (detailBox) detailBox.textContent = "请先输入书卷和章节。";
    return;
  }

  if (detailBox) detailBox.textContent = "正在读取章节详情...";

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
    if (detailBox)
      detailBox.textContent = `读取失败：${data.error || "未知错误"}`;
    return;
  }

  if (detailBox) {
    detailBox.textContent = JSON.stringify(data, null, 2);
  }
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
  const detailBox = document.getElementById("publishedDetailBox");

  if (!bookId || !chapter) {
    if (detailBox) detailBox.textContent = "请先输入书卷和章节。";
    return;
  }

  if (!confirm(`确认删除已发布内容：${bookId} ${chapter}章？`)) return;

  if (detailBox) detailBox.textContent = "正在删除...";

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
    if (detailBox)
      detailBox.textContent = `删除失败：${data.error || "未知错误"}`;
    return;
  }

  if (detailBox) {
    detailBox.textContent = `删除成功：${bookId} ${chapter}章`;
  }

  await loadPublishedOverview();
}

async function autoRepublishMissingChapter(bookId, chapter) {
  const version = document.getElementById("publishedVersionSelect")?.value;
  const lang = document.getElementById("publishedLangSelect")?.value;
  const detailBox = document.getElementById("publishedDetailBox");

  if (detailBox) {
    detailBox.textContent = `正在自动补发：${bookId} ${chapter}章...`;
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
    if (detailBox) {
      detailBox.textContent = `自动补发失败：${data.error || "未知错误"}`;
    }
    return;
  }

  fillPublishedDetailInputs(bookId, chapter);

  if (detailBox) {
    detailBox.textContent = JSON.stringify(data, null, 2);
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

function renderStars(level) {
  const { group, stars } = getStarDisplay(level);
  const colorMap = {
    1: ["var(--star-g1-1)", "var(--star-g1-2)", "var(--star-g1-3)"],
    2: ["var(--star-g2-1)", "var(--star-g2-2)", "var(--star-g2-3)"],
    3: ["var(--star-g3-1)", "var(--star-g3-2)", "var(--star-g3-3)"],
    4: ["var(--star-g4-1)", "var(--star-g4-2)", "var(--star-g4-3)"],
  };
  const color = colorMap[group][stars - 1];
  return `<span class="star-level" style="color:${color}" title="Learning progress">${"★".repeat(
    stars
  )}</span>`;
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
  const generateUpgradeCmdBtn = document.getElementById("generateUpgradeCmdBtn");
  const generateFullCmdBtn = document.getElementById("generateFullCmdBtn");
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

  downloadUpgradePackageBtn?.addEventListener("click", () => {
    downloadPackage("upgrade");
  });

  downloadFullPackageBtn?.addEventListener("click", () => {
    downloadPackage("full");
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

  refreshBtn?.addEventListener("click", loadStatus);
  await loadStatus();
  await loadDataBackups().catch((error) => {
    if (dataBackupStatusBox) dataBackupStatusBox.textContent = error?.message || "读取失败";
  });
  await loadAuditLog().catch((error) => {
    if (auditLogBox) auditLogBox.textContent = error?.message || "读取失败";
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
    const adminRes = await fetch("/api/admin/bootstrap", { cache: "no-store" });
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

init();
