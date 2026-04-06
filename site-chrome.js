/**
 * 全站顶栏 / 底栏：从 /api/site-chrome 拉取配置并写入 .askbible-chrome-* 占位槽。
 * 须在各页于 theme-apply.js 之后、defer 加载。
 *
 * 顶栏导航图标 SVG 注册在本文件内（不依赖 chrome-nav-icon-catalog.js），避免单独脚本 404 时保存已写入 icon 却不显示。
 */
(function registerAskBibleChromeNavIcons() {
  var g = typeof window !== "undefined" ? window : typeof globalThis !== "undefined" ? globalThis : null;
  if (!g || typeof g.askBibleChromeNavIconHtml === "function") return;
  var CLS = "askbible-chrome-nav-pill-icon";
  function svg(inner) {
    return (
      '<svg class="' +
      CLS +
      '" viewBox="0 0 24 24" width="22" height="22" fill="currentColor" aria-hidden="true">' +
      inner +
      "</svg>"
    );
  }
  var BY_ID = {
    user: svg(
      '<path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z"/>'
    ),
    home: svg('<path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8h5z"/>'),
    book: svg('<path d="M6 4h12v16H6V4zm2 2v12h8V6H8z"/>'),
    book_open: svg(
      '<path d="M12 4.5C9 4.5 6.38 5.6 4.5 7.31V19c0 .55.45 1 1 1 .31 0 .62-.11.88-.29C7.86 18.25 9.81 17.5 12 17.5c2.19 0 4.14.75 5.62 2.21.26.18.57.29.88.29.55 0 1-.45 1-1V7.31C17.62 5.6 15 4.5 12 4.5zm-1 12.25v-8.1c.85-.13 1.72-.2 2.6-.2 1.1 0 2.16.13 3.13.38v8.49c-.87-.35-1.79-.57-2.73-.57-.87 0-1.7.13-2.5.38v-.48z"/>'
    ),
    info: svg(
      '<path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-6h2v6zm0-8h-2V7h2v2z"/>'
    ),
    settings: svg(
      '<path d="M19.14 12.94c.04-.31.06-.63.06-.94 0-.31-.02-.63-.06-.94l2.03-1.58c.18-.14.23-.41.12-.61l-1.92-3.32c-.12-.22-.37-.29-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54c-.04-.24-.24-.41-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96c-.22-.08-.47 0-.59.22L2.74 8.87c-.12.21-.08.47.12.61l2.03 1.58c-.04.31-.07.63-.07.94s.02.63.06.94l-2.03 1.58c-.18.14-.23.41-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.56 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61l-2.01-1.58zM12 15.6c-1.98 0-3.6-1.62-3.6-3.6s1.62-3.6 3.6-3.6 3.6 1.62 3.6 3.6-1.62 3.6-3.6 3.6z"/>'
    ),
    search: svg(
      '<path d="M15.5 14h-.79l-.28-.27A6.471 6.471 0 0016 9.5 6.5 6.5 0 109.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z"/>'
    ),
    minus: svg(
      '<path fill="none" stroke="currentColor" stroke-width="2.75" stroke-linecap="round" d="M5.5 12h13"/>'
    ),
    plus: svg(
      '<path fill="none" stroke="currentColor" stroke-width="2.75" stroke-linecap="round" d="M12 5.5v13M5.5 12h13"/>'
    ),
    heart: svg(
      '<path d="M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.54L12 21.35z"/>'
    ),
    mail: svg(
      '<path d="M20 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 4l-8 5-8-5V6l8 5 8-5v2z"/>'
    ),
    link: svg(
      '<path d="M3.9 12c0-1.71 1.39-3.1 3.1-3.1h4V7H7c-2.76 0-5 2.24-5 5s2.24 5 5 5h4v-1.9H7c-1.71 0-3.1-1.39-3.1-3.1zM8 13h8v-2H8v2zm9-6h-4v1.9h4c1.71 0 3.1 1.39 3.1 3.1s-1.39 3.1-3.1 3.1h-4V17h4c2.76 0 5-2.24 5-5s-2.24-5-5-5z"/>'
    ),
    map: svg(
      '<path d="M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9.5c-1.38 0-2.5-1.12-2.5-2.5s1.12-2.5 2.5-2.5 2.5 1.12 2.5 2.5-1.12 2.5-2.5 2.5z"/>'
    ),
    calendar: svg(
      '<path d="M19 4h-1V2h-2v2H8V2H6v2H5c-1.11 0-1.99.9-1.99 2L3 20c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 16H5V10h14v10zm0-12H5V6h14v2z"/>'
    ),
    phone: svg(
      '<path d="M6.62 10.79c1.44 2.83 3.76 5.14 6.59 6.59l2.2-2.2c.27-.27.67-.36 1.02-.24 1.12.37 2.33.57 3.57.57.55 0 1 .45 1 1V20c0 .55-.45 1-1 1-9.39 0-17-7.61-17-17 0-.55.45-1 1-1h3.5c.55 0 1 .45 1 1 0 1.25.2 2.45.57 3.57.11.35.03.74-.25 1.02l-2.2 2.2z"/>'
    ),
    doc: svg(
      '<path d="M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm4 18H6V4h7v5h5v11zM8 12h8v2H8v-2zm0 4h8v2H8v-2z"/>'
    ),
    star: svg(
      '<path d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/>'
    ),
    play: svg('<path d="M8 5v14l11-7z"/>'),
    share: svg(
      '<path d="M18 16.08c-.76 0-1.44.3-1.96.77L8.91 12.7c.05-.23.09-.46.09-.7s-.04-.47-.09-.7l7.05-4.11c.54.5 1.25.81 2.04.81 1.66 0 3-1.34 3-3s-1.34-3-3-3-3 1.34-3 3c0 .24.04.47.09.7L8.04 9.81C7.5 9.31 6.79 9 6 9c-1.66 0-3 1.34-3 3s1.34 3 3 3c.79 0 1.5-.31 2.04-.81l7.12 4.16c-.05.21-.08.43-.08.65 0 1.61 1.31 2.92 2.92 2.92s2.92-1.31 2.92-2.92-1.31-2.92-2.92-2.92z"/>'
    ),
  };
  var OPTIONS = [
    { id: "", labelZh: "无" },
    { id: "user", labelZh: "人物" },
    { id: "home", labelZh: "房屋" },
    { id: "book", labelZh: "书本" },
    { id: "book_open", labelZh: "展开的书" },
    { id: "info", labelZh: "信息" },
    { id: "settings", labelZh: "设置" },
    { id: "search", labelZh: "搜索" },
    { id: "minus", labelZh: "减号" },
    { id: "plus", labelZh: "加号" },
    { id: "heart", labelZh: "心形" },
    { id: "mail", labelZh: "邮件" },
    { id: "link", labelZh: "链接" },
    { id: "map", labelZh: "地图钉" },
    { id: "calendar", labelZh: "日历" },
    { id: "phone", labelZh: "电话" },
    { id: "doc", labelZh: "文档" },
    { id: "star", labelZh: "星标" },
    { id: "play", labelZh: "播放" },
    { id: "share", labelZh: "分享" },
  ];
  var ICON_ALIASES = {
    document: "doc",
    file: "doc",
    page: "doc",
    house: "home",
    person: "user",
  };
  function askBibleChromeNavIconHtml(id) {
    var k = String(id || "")
      .toLowerCase()
      .trim()
      .replace(/[^a-z0-9_]/g, "");
    if (ICON_ALIASES[k]) k = ICON_ALIASES[k];
    return BY_ID[k] || "";
  }
  g.ASKBIBLE_CHROME_NAV_ICON_OPTIONS = OPTIONS;
  g.askBibleChromeNavIconHtml = askBibleChromeNavIconHtml;
})();

(function () {
  var DEFAULT_PRODUCTION_ORIGIN = "https://askbible.me";
  var BRAND_SUBLINE_DISMISS_LS = "askbibleBrandSublineDismissed";

  function isSiteChromeAdminPage() {
    try {
      var p = window.location.pathname || "";
      return /site-chrome\.html$/i.test(p);
    } catch (e) {
      return false;
    }
  }

  function topbarDismissibleTrue(v) {
    if (v === true || v === 1) return true;
    var s = String(v ?? "")
      .trim()
      .toLowerCase();
    return s === "true" || s === "1" || s === "yes" || s === "on";
  }

  function topbarStickyViewportTrue(v) {
    if (v === true || v === 1) return true;
    var s = String(v ?? "")
      .trim()
      .toLowerCase();
    return s === "true" || s === "1" || s === "yes" || s === "on";
  }

  var __askbibleFixedTopbarRo = null;

  function syncFixedTopbarSpacer() {
    if (!document.documentElement.classList.contains("askbible-topbar-fixed")) return;
    var el = document.querySelector("header.site-topbar.site-topbar--sticky");
    if (!el) return;
    var h = Math.ceil(el.getBoundingClientRect().height);
    if (h < 28) h = 56;
    document.documentElement.style.setProperty("--askbible-fixed-topbar-h", h + "px");
  }

  function unbindFixedTopbarSpacer() {
    try {
      document.documentElement.classList.remove("askbible-topbar-fixed");
      document.documentElement.style.removeProperty("--askbible-fixed-topbar-h");
      if (__askbibleFixedTopbarRo) {
        __askbibleFixedTopbarRo.disconnect();
        __askbibleFixedTopbarRo = null;
      }
    } catch (e) {}
  }

  function bindFixedTopbarSpacer() {
    document.documentElement.classList.add("askbible-topbar-fixed");
    var el = document.querySelector("header.site-topbar.site-topbar--sticky");
    syncFixedTopbarSpacer();
    if (__askbibleFixedTopbarRo) {
      __askbibleFixedTopbarRo.disconnect();
      __askbibleFixedTopbarRo = null;
    }
    if (el && typeof ResizeObserver !== "undefined") {
      __askbibleFixedTopbarRo = new ResizeObserver(function () {
        syncFixedTopbarSpacer();
      });
      __askbibleFixedTopbarRo.observe(el);
    }
  }

  if (typeof window !== "undefined" && !window.__askbibleFixedTopbarResizeDeleg) {
    window.__askbibleFixedTopbarResizeDeleg = true;
    window.addEventListener(
      "resize",
      function () {
        syncFixedTopbarSpacer();
      },
      { passive: true }
    );
  }

  if (typeof document !== "undefined" && !window.__askbibleSublineDismissDeleg) {
    window.__askbibleSublineDismissDeleg = true;
    document.addEventListener("click", function (e) {
      var t = e.target;
      if (!t || typeof t.closest !== "function") return;
      var btn = t.closest(".askbible-chrome-brand-subline-dismiss");
      if (!btn) return;
      try {
        localStorage.setItem(BRAND_SUBLINE_DISMISS_LS, "1");
      } catch (err) {}
      var rows = document.querySelectorAll(".askbible-chrome-brand-subline-row");
      for (var i = 0; i < rows.length; i++) {
        rows[i].setAttribute("hidden", "");
      }
    });
  }

  function normalizeApiOrigin(raw) {
    var s = String(raw || "").trim();
    if (!s) return "";
    s = s.replace(/\/+$/, "");
    if (/\/api$/i.test(s)) s = s.replace(/\/api$/i, "");
    return s.replace(/\/+$/, "");
  }

  function isLikelyStaticDevServerPort(port) {
    var p = String(port || "").trim();
    if (!p) return false;
    if (p === "5173" || p === "4173" || p === "8081") return true;
    if (/^55\d{2}$/.test(p)) return true;
    return false;
  }

  function getApiBase() {
    try {
      var m = document.querySelector('meta[name="askbible-api-base"]');
      var fromMeta = m && String(m.getAttribute("content") || "").trim();
      if (fromMeta) return normalizeApiOrigin(fromMeta);
      if (typeof window !== "undefined" && window.__ASKBIBLE_API_BASE__) {
        return normalizeApiOrigin(window.__ASKBIBLE_API_BASE__);
      }
      var proto = window.location.protocol || "";
      if (proto === "capacitor:" || proto === "file:") return DEFAULT_PRODUCTION_ORIGIN;
      var h = window.location.hostname || "";
      var port = window.location.port || "";
      if (
        (h === "localhost" || h === "127.0.0.1") &&
        proto === "http:" &&
        isLikelyStaticDevServerPort(port)
      ) {
        return "http://127.0.0.1:3000";
      }
    } catch (e) {}
    return "";
  }

  function apiOriginRootFromNormalizedBase(normalized) {
    if (!normalized) return "";
    try {
      var href = normalized.indexOf("://") >= 0 ? normalized : "http://" + normalized;
      var u = new URL(href);
      return u.origin + "/";
    } catch (e) {
      return "";
    }
  }

  function apiUrl(path) {
    var p = path.indexOf("/") === 0 ? path : "/" + path;
    var base = getApiBase();
    if (!base) return p;
    var normalized = normalizeApiOrigin(base);
    var root = apiOriginRootFromNormalizedBase(normalized);
    if (root) {
      try {
        return new URL(p, root).href;
      } catch (e2) {}
    }
    try {
      var origin = normalized.charAt(normalized.length - 1) === "/" ? normalized : normalized + "/";
      return new URL(p, origin).href;
    } catch (e3) {
      return String(base).replace(/\/+$/, "") + p;
    }
  }

  function escapeHtml(str) {
    return String(str ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function resolveAssetUrl(u) {
    var s = String(u || "").trim();
    if (!s) return "";
    if (/^https?:\/\//i.test(s)) return s;
    if (s.indexOf("/") !== 0) return s;
    try {
      return new URL(s, window.location.origin).href;
    } catch (e) {
      return s;
    }
  }

  function renderBrandHtml(cfg, renderOpts) {
    renderOpts = renderOpts || {};
    var t = cfg.topbar || {};
    var home = escapeHtml(t.homeHref || "/");
    var titleAttr = escapeHtml(t.brandTitleAttr || "首页");
    var h = Math.max(20, Math.min(80, Number(t.logoHeight) || 36));
    var parts = [];
    if (String(t.logoUrl || "").trim()) {
      var imgSrc = escapeHtml(resolveAssetUrl(t.logoUrl));
      parts.push(
        '<a href="' +
          home +
          '" class="site-brand-link askbible-chrome-logo-link" title="' +
          titleAttr +
          '"><img class="askbible-chrome-logo" src="' +
          imgSrc +
          '" alt="" height="' +
          h +
          '" style="height:' +
          h +
          'px;width:auto;display:block" loading="eager" decoding="async" /></a>'
      );
    }
    var innerBrand = "";
    if (t.showSplitBrand !== false) {
      innerBrand =
        '<span class="brand-ask">' +
        escapeHtml(t.brandAsk || "Ask") +
        '</span><span class="brand-bible">' +
        escapeHtml(t.brandBible || "Bible") +
        '</span><span class="brand-me">' +
        escapeHtml(t.brandMe || ".me") +
        "</span>";
    } else if (String(t.brandPlainTitle || "").trim()) {
      innerBrand =
        '<span class="askbible-chrome-brand-plain">' +
        escapeHtml(t.brandPlainTitle) +
        "</span>";
    } else {
      innerBrand =
        '<span class="brand-ask">Ask</span><span class="brand-bible">Bible</span><span class="brand-me">.me</span>';
    }
    var showSub = t.brandSubtitleShow !== false;
    var subRaw = showSub ? String(t.brandSubtitle || "").trim() : "";
    var subtitleInline = t.brandSubtitleInline === true && subRaw.length > 0;
    var omitDismissChrome = renderOpts.omitDismissChrome === true;
    var dismissBtn =
      !omitDismissChrome &&
      subRaw.length > 0 &&
      topbarDismissibleTrue(t.brandSubtitleDismissible)
        ? '<button type="button" class="askbible-chrome-brand-subline-dismiss" aria-label="关闭副标题" title="关闭副标题（本机不再显示）">×</button>'
        : "";
    var h1Block =
      '<h1 id="brandTitle"><a href="' +
      home +
      '" class="site-brand-link" title="' +
      titleAttr +
      '">' +
      innerBrand +
      "</a></h1>";
    var titleBlock;
    if (subtitleInline) {
      var sep =
        '<span class="askbible-chrome-brand-inline-sep" aria-hidden="true">|</span>';
      var subP =
        '<p class="brand-subtitle askbible-chrome-brand-subline askbible-chrome-brand-subline--inline">' +
        escapeHtml(subRaw) +
        "</p>";
      var tailRow =
        '<div class="askbible-chrome-brand-subline-row askbible-chrome-brand-subline-row--inline-tail">' +
        sep +
        subP +
        dismissBtn +
        "</div>";
      titleBlock =
        '<div class="askbible-chrome-brand-text askbible-chrome-brand-text--inline">' +
        '<div class="askbible-chrome-brand-head-row">' +
        h1Block +
        tailRow +
        "</div></div>";
    } else {
      var subBlock =
        subRaw.length > 0
          ? '<div class="askbible-chrome-brand-subline-row">' +
            '<p class="brand-subtitle askbible-chrome-brand-subline">' +
            escapeHtml(subRaw) +
            "</p>" +
            dismissBtn +
            "</div>"
          : "";
      titleBlock =
        '<div class="askbible-chrome-brand-text">' +
        h1Block +
        subBlock +
        "</div>";
    }
    parts.push(titleBlock);
    return '<div class="askbible-chrome-brand-inner">' + parts.join("") + "</div>";
  }

  function navLinkWantsIconOnly(item) {
    if (!item || typeof item !== "object") return false;
    var v = item.iconOnly != null ? item.iconOnly : item.icon_only;
    if (v === true || v === 1) return true;
    var s = String(v ?? "")
      .trim()
      .toLowerCase();
    return s === "true" || s === "1" || s === "yes" || s === "on";
  }

  function renderNavHtml(cfg) {
    var links = (cfg.topbar && cfg.topbar.navLinks) || [];
    if (!links.length) return "";
    var inner = links
      .map(function (item) {
        var icon =
          typeof window !== "undefined" && typeof window.askBibleChromeNavIconHtml === "function"
            ? window.askBibleChromeNavIconHtml(item.icon) || ""
            : "";
        /* 与 server 一致：仅当确有 SVG 时才进入仅图标模式，避免配置勾了仅图标但 id 无效时变成「无字也无图标」 */
        var iconOnly = navLinkWantsIconOnly(item) && icon !== "";
        var labelEsc = escapeHtml(item.label || "");
        var ariaRaw =
          item.ariaLabel != null && String(item.ariaLabel).trim() !== ""
            ? String(item.ariaLabel).trim()
            : String(item.label || "").trim();
        var ariaEsc = escapeHtml(ariaRaw);
        var attrs = ' title="' + labelEsc + '"';
        if (iconOnly) attrs += ' aria-label="' + ariaEsc + '"';
        var pillClass = "site-topbar-link askbible-chrome-nav-pill";
        if (iconOnly) pillClass += " askbible-chrome-nav-pill--icon-only";
        var textHtml = iconOnly
          ? '<span class="askbible-chrome-nav-pill-text askbible-chrome-nav-pill-text--sr">' +
            ariaEsc +
            "</span>"
          : '<span class="askbible-chrome-nav-pill-text">' + labelEsc + "</span>";
        return (
          '<a href="' +
          escapeHtml(item.href) +
          '" class="' +
          pillClass +
          '"' +
          attrs +
          ">" +
          textHtml +
          icon +
          "</a>"
        );
      })
      .join("");
    return (
      '<nav class="askbible-chrome-nav" role="navigation" aria-label="站点导航">' +
      inner +
      "</nav>"
    );
  }

  function applyFooter(cfg) {
    var el = document.querySelector(".askbible-chrome-footer-slot");
    if (!el) return;
    var foot = cfg.footer || {};
    var left = String(foot.left || "").trim();
    var center = String(foot.center || "").trim();
    var right = String(foot.right || "").trim();
    var legacy = String(foot.text || "").trim();
    if (!left && !center && !right && legacy) {
      center = legacy;
    }
    if (!foot.enabled || (!left && !center && !right)) {
      el.innerHTML = "";
      el.setAttribute("hidden", "");
      return;
    }
    el.removeAttribute("hidden");
    el.className = "site-footer site-footer--cols askbible-chrome-footer-slot";
    function colHtml(align, raw) {
      var t = String(raw || "").trim();
      if (!t) {
        return '<div class="site-footer-col site-footer-col--' + align + '"></div>';
      }
      var lines = t.split(/\n/);
      var inner = lines
        .map(function (line) {
          return "<p class=\"site-footer-line\">" + escapeHtml(line) + "</p>";
        })
        .join("");
      return (
        '<div class="site-footer-col site-footer-col--' +
        align +
        '">' +
        inner +
        "</div>"
      );
    }
    el.innerHTML =
      '<div class="site-footer-inner">' +
      colHtml("left", left) +
      colHtml("center", center) +
      colHtml("right", right) +
      "</div>";
  }

  function applySublineDismissFromStorage(cfg) {
    var top = cfg && cfg.topbar ? cfg.topbar : {};
    if (!topbarDismissibleTrue(top.brandSubtitleDismissible)) return;
    var hid = false;
    try {
      hid = localStorage.getItem(BRAND_SUBLINE_DISMISS_LS) === "1";
    } catch (e) {}
    if (!hid) return;
    var rows = document.querySelectorAll(".askbible-chrome-brand-subline-row");
    for (var i = 0; i < rows.length; i++) {
      rows[i].setAttribute("hidden", "");
    }
  }

  function applyTopbars(cfg) {
    var headers = document.querySelectorAll("header.site-topbar");
    var omitDismissChrome = isSiteChromeAdminPage();
    var renderOpts = { omitDismissChrome: omitDismissChrome };
    var top = cfg && cfg.topbar ? cfg.topbar : {};
    var stickyOn = topbarStickyViewportTrue(top.topbarSticky);
    if (!stickyOn) {
      unbindFixedTopbarSpacer();
    }
    for (var i = 0; i < headers.length; i++) {
      var header = headers[i];
      if (stickyOn) {
        header.classList.add("site-topbar--sticky");
      } else {
        header.classList.remove("site-topbar--sticky");
      }
      var brandSlot = header.querySelector(".askbible-chrome-brand-slot");
      var navSlot = header.querySelector(".askbible-chrome-nav-slot");
      if (brandSlot) {
        brandSlot.innerHTML = renderBrandHtml(cfg, renderOpts);
      }
      if (navSlot) {
        navSlot.innerHTML = renderNavHtml(cfg);
      }
    }
    if (!omitDismissChrome) {
      applySublineDismissFromStorage(cfg);
    }
    if (stickyOn && headers.length) {
      requestAnimationFrame(function () {
        bindFixedTopbarSpacer();
        syncFixedTopbarSpacer();
        setTimeout(syncFixedTopbarSpacer, 80);
        setTimeout(syncFixedTopbarSpacer, 350);
      });
    } else {
      unbindFixedTopbarSpacer();
    }
  }

  function applyAll(cfg) {
    if (!cfg || typeof cfg !== "object") return;
    applyTopbars(cfg);
    applyFooter(cfg);
  }

  window.__applyAskBibleSiteChrome = applyAll;

  function getSharePageUrl() {
    try {
      return String(window.location.href || "").split("#")[0];
    } catch (e) {
      return "";
    }
  }

  function getSharePageTitle() {
    try {
      var t = document.title;
      if (t && String(t).trim()) return String(t).trim();
      return "AskBible.me";
    } catch (e) {
      return "AskBible.me";
    }
  }

  async function shareCurrentPage() {
    var url = getSharePageUrl();
    if (!url) return;
    var title = getSharePageTitle();
    try {
      if (navigator.share && typeof navigator.share === "function") {
        await navigator.share({ title: title, text: title, url: url });
        return;
      }
    } catch (e) {
      if (e && e.name === "AbortError") return;
    }
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(url);
        window.alert("链接已复制到剪贴板，可粘贴分享。");
        return;
      }
    } catch (e2) {}
    try {
      window.prompt("复制以下链接分享：", url);
    } catch (e3) {
      window.alert(url);
    }
  }

  function tryConsumeShareDeepLink() {
    try {
      if (window.location.hash !== "#openSharePage") return;
      var u = new URL(window.location.href);
      u.hash = "";
      history.replaceState({}, "", u.pathname + (u.search || ""));
      queueMicrotask(function () {
        void shareCurrentPage();
      });
    } catch (e) {}
  }

  function initSharePageNavAction() {
    tryConsumeShareDeepLink();
    window.addEventListener("hashchange", function () {
      if (window.location.hash !== "#openSharePage") return;
      try {
        history.replaceState(
          {},
          "",
          window.location.pathname + (window.location.search || "")
        );
      } catch (e) {}
      queueMicrotask(function () {
        void shareCurrentPage();
      });
    });
    document.addEventListener(
      "click",
      function (e) {
        if (e.defaultPrevented || e.button !== 0) return;
        if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
        var t = e.target;
        if (!t || typeof t.closest !== "function") return;
        var a = t.closest("a[href]");
        if (!a || !a.closest(".askbible-chrome-nav")) return;
        var abs;
        try {
          abs = new URL(a.getAttribute("href") || "", window.location.href);
        } catch (err) {
          return;
        }
        if (abs.hash !== "#openSharePage") return;
        try {
          if (String(abs.origin || "") !== String(window.location.origin || "")) return;
        } catch (eO) {
          return;
        }
        e.preventDefault();
        e.stopPropagation();
        void shareCurrentPage();
      },
      true
    );
  }

  initSharePageNavAction();

  async function run() {
    try {
      var paths = ["/api/site-chrome", "/api/sitechrome"];
      var cfg = null;
      for (var i = 0; i < paths.length; i++) {
        var url = apiUrl(paths[i]);
        var res = await fetch(url, { cache: "no-store" });
        if (res.status === 404) continue;
        if (!res.ok) return;
        cfg = await res.json();
        break;
      }
      if (cfg) applyAll(cfg);
    } catch (e) {
      /* 离线或接口不可用时保留 HTML 默认占位 */
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();
