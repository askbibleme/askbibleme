/**
 * 章节插画流水线：载入 theme + 摘要 → Generate Scene（/api/chapter-illustration/scene）
 * → Generate Prompt（可自动补场景）→ Generate Illustration（OpenAI gpt-image-1）
 */
(function () {
  "use strict";

  const USER_AUTH_TOKEN_KEY = "bible_user_auth_token_v1";
  const DEFAULT_PRODUCTION_ORIGIN = "https://askbible.me";

  function getToken() {
    return String(localStorage.getItem(USER_AUTH_TOKEN_KEY) || "");
  }

  function authHeadersBare() {
    const t = getToken();
    return t ? { Authorization: "Bearer " + t } : {};
  }

  function normalizeApiOrigin(raw) {
    let s = String(raw || "").trim();
    if (!s) return "";
    s = s.replace(/\/+$/, "");
    if (/\/api$/i.test(s)) s = s.replace(/\/api$/i, "");
    return s.replace(/\/+$/, "");
  }

  function isPrivateLanHostname(h) {
    const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(
      String(h || "").toLowerCase()
    );
    if (!m) return false;
    const a = Number(m[1]);
    const b = Number(m[2]);
    if (![a, b, Number(m[3]), Number(m[4])].every((n) => n >= 0 && n <= 255))
      return false;
    if (a === 10) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 192 && b === 168) return true;
    return false;
  }

  function getApiBase() {
    const m = document.querySelector('meta[name="askbible-api-base"]');
    const fromMeta = m && String(m.getAttribute("content") || "").trim();
    if (fromMeta) return normalizeApiOrigin(fromMeta);
    if (typeof window !== "undefined" && window.__ASKBIBLE_API_BASE__) {
      return normalizeApiOrigin(window.__ASKBIBLE_API_BASE__);
    }
    try {
      const proto = window.location.protocol || "";
      if (proto === "capacitor:" || proto === "file:") {
        return DEFAULT_PRODUCTION_ORIGIN;
      }
      const h = String(window.location.hostname || "");
      const loopback =
        h === "localhost" || h === "127.0.0.1" || h === "::1";
      if (loopback || isPrivateLanHostname(h)) {
        const port = String(window.location.port || "");
        if (port && port !== "3000") {
          const scheme = proto === "https:" ? "https" : "http";
          return scheme + "://" + h + ":3000";
        }
      }
    } catch (_) {}
    return "";
  }

  function apiUrl(path) {
    const p = path.startsWith("/") ? path : "/" + path;
    const base = getApiBase();
    if (!base) return p;
    try {
      const origin = normalizeApiOrigin(base);
      return new URL(p, origin.endsWith("/") ? origin : origin + "/").href;
    } catch {
      return base.replace(/\/$/, "") + p;
    }
  }

  /** 将服务端返回的相对路径解析为可加载的绝对 URL（Capacitor / 跨端口同源时必需） */
  function resolveIllustrationSrc(pathOrUrl) {
    const s = String(pathOrUrl || "").trim();
    if (!s) return "";
    if (/^https?:\/\//i.test(s)) return s;
    if (s.startsWith("/")) return apiUrl(s);
    return s;
  }

  function clientThemeHasContent(raw) {
    if (raw == null) return false;
    if (typeof raw === "string") return raw.trim().length > 0;
    if (typeof raw === "object" && !Array.isArray(raw)) {
      const c = String(raw.core || "").trim();
      const r = String(raw.resolution || "").trim();
      return Boolean(c || r);
    }
    return false;
  }

  function formatThemeDisplay(raw) {
    if (raw == null) return "";
    if (typeof raw === "string") return raw;
    if (typeof raw === "object" && !Array.isArray(raw)) {
      const c = String(raw.core || "").trim();
      const r = String(raw.resolution || "").trim();
      const lines = [];
      if (r) lines.push("resolution：" + r);
      if (c) lines.push("core：" + c);
      return lines.length ? lines.join("\n") : JSON.stringify(raw, null, 2);
    }
    return String(raw);
  }

  const authEl = document.getElementById("cpAuth");
  const appEl = document.getElementById("cpApp");
  const versionSel = document.getElementById("cpVersion");
  const langSel = document.getElementById("cpLang");
  const bookSel = document.getElementById("cpBook");
  const chapterSel = document.getElementById("cpChapter");
  const themeDisplayEl = document.getElementById("cpThemeDisplay");
  const summaryDisplayEl = document.getElementById("cpSummaryDisplay");
  const sceneEl = document.getElementById("cpScene");
  const sceneZhEl = document.getElementById("cpSceneZh");
  const stylePresetEl = document.getElementById("cpStylePreset");
  const pipelineMetaEl = document.getElementById("cpPipelineMeta");
  const btnSceneEl = document.getElementById("cpGenerateSceneBtn");
  const btnRegenSceneEl = document.getElementById("cpRegenerateSceneBtn");
  const transparentEl = document.getElementById("cpTransparentBg");
  const opacityEl = document.getElementById("cpOpacity");
  const opacityLabelEl = document.getElementById("cpOpacityValue");
  const btnEl = document.getElementById("cpGenerateBtn");
  const btnIllustrationEl = document.getElementById("cpGenerateIllustrationBtn");
  const outEl = document.getElementById("cpOutput");
  const statusEl = document.getElementById("cpStatus");
  const specDetailsEl = document.getElementById("cpSpecDetails");
  const specPreEl = document.getElementById("cpSpecPre");
  const charLockDetailsEl = document.getElementById("cpCharLockDetails");
  const charLockPreEl = document.getElementById("cpCharLockPre");
  const illustrationLayer = document.getElementById("cpIllustrationLayer");
  const illustrationImg = document.getElementById("cpIllustrationImg");

  /** 来自 /api/admin/bootstrap 的书卷列表 */
  let books = [];
  /** 当前章 JSON 的 theme 原值：string 或 { core, resolution } */
  let loadedThemeRaw = null;
  /** 场景候选轮换（Regenerate Scene 递增） */
  let sceneVariant = 0;

  function setStatus(msg, kind) {
    statusEl.textContent = msg || "";
    statusEl.classList.remove("cp-status--err", "cp-status--ok");
    if (kind === "err") statusEl.classList.add("cp-status--err");
    if (kind === "ok") statusEl.classList.add("cp-status--ok");
  }

  function setOutput(text) {
    const t = String(text || "").trim();
    if (!t) {
      outEl.textContent = "生成的英文 prompt 将显示在这里。";
      outEl.classList.add("cp-output--empty");
      return;
    }
    outEl.textContent = t;
    outEl.classList.remove("cp-output--empty");
  }

  function applyCharacterLockDisplay(lines) {
    if (!charLockDetailsEl || !charLockPreEl) return;
    const arr = Array.isArray(lines) ? lines : [];
    if (arr.length) {
      charLockPreEl.textContent = arr.join("\n");
    } else {
      charLockPreEl.textContent =
        "（未生成锁定行：本章 theme/摘要中可能未匹配到人名，或尚未在 character_illustration_profiles.json 中填写 appearanceEn。）";
    }
    charLockDetailsEl.hidden = false;
  }

  function applyOverlayOpacity(percent) {
    const p = Math.max(0, Math.min(100, Number(percent) || 0));
    illustrationLayer.style.opacity = String(p / 100);
    opacityLabelEl.textContent = p + "%";
    opacityEl.setAttribute("aria-valuenow", String(p));
  }

  /**
   * @param {string} imageUrl
   * @param {string|boolean} transparentPng 若为 string 则优先作 src；boolean 时仅用 imageUrl
   */
  function applyIllustrationSource(imageUrl, transparentPng) {
    let raw = "";
    if (typeof transparentPng === "string" && String(transparentPng).trim()) {
      raw = String(transparentPng).trim();
    } else if (typeof imageUrl === "string" && String(imageUrl).trim()) {
      raw = String(imageUrl).trim();
    }
    const src = resolveIllustrationSrc(raw);
    if (!src) {
      illustrationLayer.dataset.hasImage = "0";
      illustrationImg.removeAttribute("src");
      illustrationLayer.dataset.state = "placeholder";
      return;
    }
    illustrationImg.src = src;
    illustrationLayer.dataset.hasImage = "1";
    illustrationLayer.dataset.state = "image";
  }

  function fillChapterSelectForBook(bookId) {
    const b = books.find((x) => x.bookId === bookId);
    const nRaw = b ? Number(b.chapters) : NaN;
    const n = Number.isFinite(nRaw) && nRaw >= 0 ? nRaw : 1;
    chapterSel.innerHTML = "";
    const introOpt = document.createElement("option");
    introOpt.value = "0";
    introOpt.textContent = "卷首页";
    chapterSel.appendChild(introOpt);
    for (let c = 1; c <= n; c += 1) {
      const opt = document.createElement("option");
      opt.value = String(c);
      opt.textContent = "第 " + c + " 章";
      chapterSel.appendChild(opt);
    }
  }

  function setSceneZhDisplay(text, isPlaceholder) {
    if (!sceneZhEl) return;
    const t = String(text || "").trim();
    sceneZhEl.textContent = t || "生成场景后将显示中文说明。";
    sceneZhEl.classList.toggle("cp-theme-readonly--placeholder", Boolean(isPlaceholder || !t));
  }

  function setThemePlaceholder(msg) {
    loadedThemeRaw = null;
    sceneVariant = 0;
    setSceneZhDisplay("", true);
    themeDisplayEl.textContent = msg;
    themeDisplayEl.classList.add("cp-theme-readonly--placeholder");
    if (summaryDisplayEl) {
      summaryDisplayEl.textContent = "请先选择并载入章节。";
      summaryDisplayEl.classList.add("cp-theme-readonly--placeholder");
    }
    if (pipelineMetaEl) {
      pipelineMetaEl.hidden = true;
      pipelineMetaEl.textContent = "";
    }
  }

  function applyLoadedTheme(themeField) {
    loadedThemeRaw = themeField;
    const text = formatThemeDisplay(themeField);
    themeDisplayEl.textContent = text || "（theme 为空）";
    themeDisplayEl.classList.toggle(
      "cp-theme-readonly--placeholder",
      !clientThemeHasContent(themeField)
    );
  }

  /** 读取已发布章节，取 data.theme */
  async function loadPublishedTheme() {
    const version = String(versionSel.value || "").trim();
    const lang = String(langSel.value || "").trim();
    const bookId = String(bookSel.value || "").trim();
    const chapter = String(chapterSel.value || "0");

    if (!version || !lang || !bookId) {
      setThemePlaceholder("请先选择内容版本、语言与书卷。");
      return;
    }

    themeDisplayEl.classList.remove("cp-theme-readonly--placeholder");
    themeDisplayEl.textContent = "正在读取已发布章节…";
    loadedThemeRaw = null;

    const q = new URLSearchParams({
      version,
      lang,
      bookId,
      chapter,
    });

    try {
      const res = await fetch(apiUrl("/api/admin/published/chapter?" + q.toString()), {
        headers: authHeadersBare(),
        cache: "no-store",
      });
      const raw = await res.text();
      let data = {};
      try {
        data = raw.trim() ? JSON.parse(raw) : {};
      } catch (_) {
        data = {};
      }
      if (!res.ok) {
        if (res.status === 404) {
          setThemePlaceholder("未找到已发布该章，无法读取 theme。请先在后台生成并发布此章节。");
        } else {
          setThemePlaceholder(data.error || "读取失败（HTTP " + res.status + "）");
        }
        return;
      }
      const tf = data.theme;
      if (clientThemeHasContent(tf)) {
        applyLoadedTheme(tf);
        sceneVariant = 0;
        const seg0 =
          Array.isArray(data.segments) && data.segments[0]
            ? String(data.segments[0].title || "").trim()
            : "";
        if (summaryDisplayEl) {
          summaryDisplayEl.textContent =
            seg0 || "（本章无段落标题，仅依据 theme 推断场景）";
          summaryDisplayEl.classList.toggle(
            "cp-theme-readonly--placeholder",
            !seg0
          );
        }
        if (pipelineMetaEl) {
          pipelineMetaEl.hidden = true;
          pipelineMetaEl.textContent = "";
        }
      } else {
        setThemePlaceholder(
          "该章已发布，但尚无有效 theme。请在发布 JSON 中补全 theme 正文，或 theme.core / theme.resolution。"
        );
      }
    } catch (e) {
      setThemePlaceholder(String(e && e.message ? e.message : e));
    }
  }

  /** 当前选中书卷的中文名 + id，写入 prompts.json 的 book 字段 */
  function selectedBookLabel() {
    const bookId = String(bookSel.value || "").trim();
    const b = books.find((x) => x.bookId === bookId);
    const cn = b && b.bookCn ? String(b.bookCn) : bookId;
    return cn + (bookId ? " (" + bookId + ")" : "");
  }

  function buildCommonRequestBody() {
    return {
      book: selectedBookLabel(),
      bookId: String(bookSel.value || "").trim(),
      version: String(versionSel.value || "").trim(),
      lang: String(langSel.value || "").trim(),
      chapter: String(chapterSel.value || ""),
      theme: loadedThemeRaw,
      scene: String(sceneEl.value || "").trim(),
      sceneVariant,
      stylePreset: stylePresetEl
        ? String(stylePresetEl.value || "biblical_copperplate_engraving")
        : "biblical_copperplate_engraving",
      transparentBackground: Boolean(transparentEl.checked),
      overlayOpacity: Number(opacityEl.value) || 0,
    };
  }

  function setPipelineMetaText(text, isWarn) {
    if (!pipelineMetaEl) return;
    const t = String(text || "").trim();
    if (!t) {
      pipelineMetaEl.hidden = true;
      pipelineMetaEl.textContent = "";
      pipelineMetaEl.classList.remove("cp-pipeline-meta--warn");
      return;
    }
    pipelineMetaEl.hidden = false;
    pipelineMetaEl.textContent = t;
    pipelineMetaEl.classList.toggle("cp-pipeline-meta--warn", Boolean(isWarn));
  }

  function applyPipelineFromResponse(pipeline) {
    if (!pipeline) {
      setPipelineMetaText("", false);
      return;
    }
    const parts = [];
    if (pipeline.chapterTypeZh)
      parts.push("章节类型：" + pipeline.chapterTypeZh);
    else if (pipeline.chapterType)
      parts.push("章节类型：" + pipeline.chapterType);
    if (pipeline.selection && pipeline.selection.sceneLabelZh)
      parts.push("择景：" + pipeline.selection.sceneLabelZh);
    if (typeof pipeline.confidence === "number")
      parts.push("置信度：" + pipeline.confidence.toFixed(2));
    const warnText = pipeline.warningZh || pipeline.warning;
    if (warnText) setPipelineMetaText(parts.join(" · ") + " — " + warnText, true);
    else setPipelineMetaText(parts.join(" · "), false);
  }

  function validateThemeForPrompt() {
    if (!clientThemeHasContent(loadedThemeRaw)) {
      setStatus(
        "请先完善 theme：本章需有 theme 正文，或 theme.core / theme.resolution。",
        "err"
      );
      return false;
    }
    return true;
  }

  function validateChapterContextForAuto() {
    const version = String(versionSel.value || "").trim();
    const lang = String(langSel.value || "").trim();
    const bookId = String(bookSel.value || "").trim();
    if (!version || !lang || !bookId) {
      setStatus("请先选择内容版本、语言与书卷。", "err");
      return false;
    }
    return true;
  }

  async function generateSceneFromServer(regenerate) {
    if (!validateThemeForPrompt()) return;
    if (!validateChapterContextForAuto()) return;
    if (regenerate) sceneVariant += 1;
    else sceneVariant = 0;
    const btn = regenerate ? btnRegenSceneEl : btnSceneEl;
    if (btn) btn.disabled = true;
    setStatus(regenerate ? "正在切换候选场景…" : "正在根据本章生成场景…");
    try {
      const res = await fetch(apiUrl("/api/chapter-illustration/scene"), {
        method: "POST",
        headers: {
          ...authHeadersBare(),
          "Content-Type": "application/json",
        },
        body: JSON.stringify(buildCommonRequestBody()),
      });
      const raw = await res.text();
      let data = {};
      try {
        data = raw.trim() ? JSON.parse(raw) : {};
      } catch (_) {
        data = {};
      }
      if (!res.ok) {
        setStatus(data.error || "生成场景失败（HTTP " + res.status + "）", "err");
        return;
      }
      const desc = String(data.sceneDescription || "").trim();
      if (!desc) {
        setStatus("服务器未返回英文场景描述。", "err");
        return;
      }
      sceneEl.value = desc;
      const descZh = String(data.sceneDescriptionZh || "").trim();
      setSceneZhDisplay(descZh || "（未返回中文说明）", !descZh);
      if (typeof data.selection?.variantIndex === "number")
        sceneVariant = data.selection.variantIndex;
      else if (typeof data.chapterState?.sceneVariant === "number")
        sceneVariant = data.chapterState.sceneVariant;
      applyPipelineFromResponse({
        chapterType: data.chapterType,
        chapterTypeZh: data.chapterTypeZh,
        confidence: data.confidence,
        warning: data.warning,
        warningZh: data.warningZh,
        selection: data.selection,
      });
      setStatus("已生成场景：上方为中文说明，下方英文可改；可点「生成 Prompt」。", "ok");
    } catch (e) {
      setStatus(String(e && e.message ? e.message : e), "err");
    } finally {
      if (btn) btn.disabled = false;
    }
  }

  async function generatePrompt() {
    if (!validateThemeForPrompt()) return;

    btnEl.disabled = true;
    setStatus("正在生成 Prompt…");
    try {
      const res = await fetch(apiUrl("/api/generate-prompt"), {
        method: "POST",
        headers: {
          ...authHeadersBare(),
          "Content-Type": "application/json",
        },
        body: JSON.stringify(buildCommonRequestBody()),
      });
      const raw = await res.text();
      let data = {};
      try {
        data = raw.trim() ? JSON.parse(raw) : {};
      } catch (_) {
        data = {};
      }
      if (!res.ok) {
        if (specDetailsEl) specDetailsEl.hidden = true;
        if (charLockDetailsEl) charLockDetailsEl.hidden = true;
        setStatus(data.error || "请求失败（HTTP " + res.status + "）", "err");
        return;
      }
      if (data.illustrationSpec && data.illustrationSpec.scene != null) {
        sceneEl.value = String(data.illustrationSpec.scene);
      }
      if (data.pipeline) applyPipelineFromResponse(data.pipeline);
      if (data.prompt) {
        setOutput(data.prompt);
        setStatus("已生成 Prompt 并写入记录；英文见下方输出区。", "ok");
        const zhFromPrompt = String(data.sceneDescriptionZh || "").trim();
        if (zhFromPrompt) setSceneZhDisplay(zhFromPrompt, false);
        applyCharacterLockDisplay(
          data.characterAppearanceLines || data.illustrationSpec?.characterAppearanceLines
        );
        if (data.illustrationSpec && specDetailsEl && specPreEl) {
          specPreEl.textContent = JSON.stringify(data.illustrationSpec, null, 2);
          specDetailsEl.hidden = false;
        }
      } else {
        setStatus("响应缺少 Prompt 正文。", "err");
      }
      if (data.transparentPng || data.imageUrl) {
        applyIllustrationSource(data.imageUrl || "", data.transparentPng || "");
      }
      if (data.overlayOpacity != null && Number.isFinite(Number(data.overlayOpacity))) {
        opacityEl.value = String(
          Math.max(0, Math.min(100, Math.round(Number(data.overlayOpacity))))
        );
        applyOverlayOpacity(opacityEl.value);
      }
    } catch (e) {
      setStatus(String(e && e.message ? e.message : e), "err");
    } finally {
      btnEl.disabled = false;
    }
  }

  async function generateIllustration() {
    if (!validateThemeForPrompt()) return;

    btnIllustrationEl.disabled = true;
    setStatus("正在生成 Prompt …");
    try {
      const resPrompt = await fetch(apiUrl("/api/generate-prompt"), {
        method: "POST",
        headers: {
          ...authHeadersBare(),
          "Content-Type": "application/json",
        },
        body: JSON.stringify(buildCommonRequestBody()),
      });
      const rawP = await resPrompt.text();
      let promptData = {};
      try {
        promptData = rawP.trim() ? JSON.parse(rawP) : {};
      } catch (_) {
        promptData = {};
      }
      if (!resPrompt.ok) {
        setStatus(
          promptData.error || "生成 Prompt 失败（HTTP " + resPrompt.status + "）",
          "err"
        );
        return;
      }
      const promptText = String(promptData.prompt || "").trim();
      if (!promptText) {
        setStatus("服务器未返回 Prompt 正文。", "err");
        return;
      }
      if (promptData.illustrationSpec && promptData.illustrationSpec.scene != null) {
        sceneEl.value = String(promptData.illustrationSpec.scene);
      }
      if (promptData.pipeline) applyPipelineFromResponse(promptData.pipeline);
      const zhP = String(promptData.sceneDescriptionZh || "").trim();
      if (zhP) setSceneZhDisplay(zhP, false);
      setOutput(promptText);
      applyCharacterLockDisplay(
        promptData.characterAppearanceLines ||
          promptData.illustrationSpec?.characterAppearanceLines
      );
      if (promptData.illustrationSpec && specDetailsEl && specPreEl) {
        specPreEl.textContent = JSON.stringify(promptData.illustrationSpec, null, 2);
        specDetailsEl.hidden = false;
      }
      if (
        promptData.overlayOpacity != null &&
        Number.isFinite(Number(promptData.overlayOpacity))
      ) {
        opacityEl.value = String(
          Math.max(0, Math.min(100, Math.round(Number(promptData.overlayOpacity))))
        );
        applyOverlayOpacity(opacityEl.value);
      }

      setStatus("正在调用 OpenAI 生成插画 …");
      const transparent = Boolean(transparentEl.checked);
      const resImg = await fetch(apiUrl("/api/generate-illustration"), {
        method: "POST",
        headers: {
          ...authHeadersBare(),
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          prompt: promptText,
          transparent,
          bookId: String(bookSel.value || "").trim(),
          chapter: String(chapterSel.value || ""),
          sceneDescription: String(sceneEl.value || "").trim(),
        }),
      });
      const rawI = await resImg.text();
      let imgData = {};
      try {
        imgData = rawI.trim() ? JSON.parse(rawI) : {};
      } catch (_) {
        imgData = {};
      }
      if (!resImg.ok) {
        setStatus(
          imgData.error || "generate-illustration 失败（HTTP " + resImg.status + "）",
          "err"
        );
        return;
      }
      if (!imgData.success) {
        setStatus(imgData.error || "出图失败", "err");
        return;
      }
      const url = String(imgData.imageUrl || "").trim();
      if (url) {
        applyIllustrationSource(url, imgData.transparentPng);
        setStatus("出图完成，预览已更新。", "ok");
      } else {
        setStatus("接口成功但未返回 imageUrl。", "err");
      }
    } catch (e) {
      setStatus(String(e && e.message ? e.message : e), "err");
    } finally {
      btnIllustrationEl.disabled = false;
    }
  }

  async function init() {
    const token = getToken();
    if (!token) {
      authEl.hidden = false;
      authEl.innerHTML =
        '请先 <a href="/">返回首页</a> 登录管理员账号，再使用本工具。';
      return;
    }
    authEl.hidden = true;

    const res = await fetch(apiUrl("/api/admin/bootstrap"), {
      headers: authHeadersBare(),
      cache: "no-store",
    });
    const raw = await res.text();
    let payload = {};
    try {
      payload = raw.trim() ? JSON.parse(raw) : {};
    } catch (_) {
      payload = {};
    }
    if (res.status === 401) {
      authEl.hidden = false;
      authEl.innerHTML = '请先 <a href="/">返回首页</a> 登录。';
      return;
    }
    if (res.status === 403) {
      authEl.hidden = false;
      authEl.innerHTML = "需要管理员权限。";
      return;
    }
    if (!res.ok) {
      authEl.hidden = false;
      authEl.innerHTML =
        "无法加载后台数据：" + (payload.error || "HTTP " + res.status);
      return;
    }

    const contentVersions = Array.isArray(payload.contentVersions)
      ? payload.contentVersions
      : [];
    const langs = Array.isArray(payload.languages) ? payload.languages : [];
    books = Array.isArray(payload.books) ? payload.books : [];

    versionSel.innerHTML = "";
    contentVersions
      .filter((x) => x && x.enabled !== false && x.id)
      .sort((a, b) => Number(a.order || 999) - Number(b.order || 999))
      .forEach((x) => {
        const opt = document.createElement("option");
        opt.value = String(x.id);
        opt.textContent = String(x.label || x.id);
        versionSel.appendChild(opt);
      });

    langSel.innerHTML = "";
    langs
      .filter((x) => x && x.enabled !== false && (x.id || x.code))
      .forEach((x) => {
        const id = String(x.id || x.code || "").trim();
        if (!id) return;
        const opt = document.createElement("option");
        opt.value = id;
        opt.textContent = String(x.label || id);
        langSel.appendChild(opt);
      });

    bookSel.innerHTML = "";
    books.forEach((b) => {
      const opt = document.createElement("option");
      opt.value = String(b.bookId);
      opt.textContent = String(b.bookCn || b.bookId) + " (" + String(b.bookId) + ")";
      bookSel.appendChild(opt);
    });

    if (bookSel.options.length) {
      fillChapterSelectForBook(bookSel.value);
    }

    appEl.hidden = false;

    versionSel.addEventListener("change", function () {
      void loadPublishedTheme();
    });
    langSel.addEventListener("change", function () {
      void loadPublishedTheme();
    });
    bookSel.addEventListener("change", function () {
      fillChapterSelectForBook(bookSel.value);
      void loadPublishedTheme();
    });
    chapterSel.addEventListener("change", function () {
      void loadPublishedTheme();
    });

    await loadPublishedTheme();
  }

  if (opacityEl && opacityLabelEl && illustrationLayer) {
    opacityEl.addEventListener("input", function () {
      applyOverlayOpacity(opacityEl.value);
    });
    applyOverlayOpacity(opacityEl.value);
  }

  if (btnEl) {
    btnEl.addEventListener("click", function () {
      void generatePrompt();
    });
  }

  if (btnSceneEl) {
    btnSceneEl.addEventListener("click", function () {
      void generateSceneFromServer(false);
    });
  }
  if (btnRegenSceneEl) {
    btnRegenSceneEl.addEventListener("click", function () {
      void generateSceneFromServer(true);
    });
  }

  if (btnIllustrationEl) {
    btnIllustrationEl.addEventListener("click", function () {
      void generateIllustration();
    });
  }

  void init();
})();
