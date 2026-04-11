/**
 * 尽早把配色变量写到 document.documentElement（依赖 /api/color-themes*）。
 * 登录后 main.js 会按账号覆盖并写回 localStorage。
 */
(function () {
  var STORAGE_KEY = "bible_color_theme_id_v1";
  var DEFAULT_PRODUCTION_ORIGIN = "https://askbible.me";

  function normalizeApiOrigin(raw) {
    var s = String(raw || "").trim();
    if (!s) return "";
    s = s.replace(/\/+$/, "");
    if (/\/api$/i.test(s)) s = s.replace(/\/api$/i, "");
    return s.replace(/\/+$/, "");
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
      if (proto === "capacitor:" || proto === "file:") {
        return DEFAULT_PRODUCTION_ORIGIN;
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
      } catch (e2) {
        /* fall through */
      }
    }
    try {
      var origin = normalized.charAt(normalized.length - 1) === "/" ? normalized : normalized + "/";
      return new URL(p, origin).href;
    } catch (e3) {
      return String(base).replace(/\/+$/, "") + p;
    }
  }

  function applyVars(variables) {
    if (!variables || typeof variables !== "object") return;
    var root = document.documentElement;
    for (var k in variables) {
      if (!Object.prototype.hasOwnProperty.call(variables, k)) continue;
      if (k.indexOf("--") !== 0) continue;
      var v = variables[k];
      if (v == null) continue;
      root.style.setProperty(k, String(v));
    }
  }

  function fetchJson(path) {
    return fetch(apiUrl(path), {
      cache: "no-store",
      headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
    }).then(function (res) {
      if (!res.ok) throw new Error(String(res.status));
      return res.json();
    });
  }

  function run() {
    var saved = "";
    try {
      saved = String(localStorage.getItem(STORAGE_KEY) || "").trim();
    } catch (e) {
      saved = "";
    }

    fetchJson("/api/color-themes")
      .then(function (meta) {
        var list = meta.themes || [];
        var def = String(meta.defaultThemeId || "classic");
        var id = saved;
        if (
          !id ||
          !list.some(function (t) {
            return t && t.id === id;
          })
        ) {
          id = def;
        }
        return fetchJson(
          "/api/color-themes/variables?themeId=" + encodeURIComponent(id)
        );
      })
      .then(function (data) {
        applyVars(data.variables);
      })
      .catch(function () {
        /* 保持 styles.css 默认 */
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();

/**
 * iOS / Android 移动浏览器 + PWA：用 visualViewport 写 --askbible-inner-*，横竖屏与工具栏伸缩后尺寸更稳。
 * Android 不跑 WebKit 的 scrollTo 抖一下重排（避免多余跳动）；iOS 仍保留。
 * 含微信内置浏览器等 UA 时也会启用（桌面微信打开链接时 inner/VV 也常滞后）。
 */
(function () {
  /** 内置浏览器 / 定制壳：VV、innerHeight、键盘与首屏晚于布局，需额外 load、focus 补同步 */
  function uaNeedsAggressiveViewportGlue() {
    try {
      var ua = navigator.userAgent || "";
      if (/SamsungBrowser/i.test(ua)) return true;
      if (
        /Android/i.test(ua) &&
        (/\bwv\)/.test(ua) || /; wv\)/.test(ua) || /WebView\//i.test(ua))
      ) {
        return true;
      }
      if (/MicroMessenger/i.test(ua)) return true;
      if (/FB_IAB|FBAN|FBAV|FBIOS/i.test(ua)) return true;
      if (/\bInstagram\b/i.test(ua)) return true;
      if (/\bLine\/[\d.]+/i.test(ua)) return true;
      if (/UCBrowser|UCWEB/i.test(ua)) return true;
      if (/\bMiuiBrowser\//i.test(ua)) return true;
      if (/HuaweiBrowser|HUAWEI.*Browser/i.test(ua)) return true;
      if (/\bHeyTapBrowser\//i.test(ua)) return true;
      if (/\bVivoBrowser\//i.test(ua)) return true;
      if (/MQQBrowser/i.test(ua)) return true;
      if (/\bFxiOS\b/i.test(ua)) return true;
      if (/Android/i.test(ua) && /Firefox\//i.test(ua) && /Mobile/i.test(ua)) return true;
      return false;
    } catch (e) {
      return false;
    }
  }
  try {
    window.__askBibleUaNeedsAggressiveViewportGlue = uaNeedsAggressiveViewportGlue;
  } catch (eReg) {}
  function isIosLike() {
    try {
      var ua = navigator.userAgent || "";
      if (/iPhone|iPad|iPod/.test(ua)) return true;
      if (navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1) return true;
    } catch (e) {}
    return false;
  }
  function isAndroid() {
    try {
      return /Android/i.test(navigator.userAgent || "");
    } catch (e2) {
      return false;
    }
  }
  function isStandaloneDisplay() {
    try {
      if (window.matchMedia("(display-mode: standalone)").matches) return true;
      if (window.matchMedia("(display-mode: fullscreen)").matches) return true;
    } catch (e) {}
    try {
      if (window.navigator.standalone === true) return true;
    } catch (e2) {}
    return false;
  }
  if (!isIosLike() && !isStandaloneDisplay() && !isAndroid() && !uaNeedsAggressiveViewportGlue()) {
    return;
  }

  function setInnerSizeVars() {
    try {
      var h = window.innerHeight;
      var w = window.innerWidth;
      if (window.visualViewport) {
        h = window.visualViewport.height;
        w = window.visualViewport.width;
      }
      document.documentElement.style.setProperty("--askbible-inner-h", h + "px");
      document.documentElement.style.setProperty("--askbible-inner-w", w + "px");
    } catch (e) {}
  }

  function nudgeReflow() {
    setInnerSizeVars();
    try {
      window.dispatchEvent(new Event("resize"));
    } catch (e) {}
    if (!isAndroid()) {
      try {
        var y = window.scrollY || 0;
        window.scrollTo(0, y + 1);
        window.requestAnimationFrame(function () {
          window.scrollTo(0, y);
        });
      } catch (e2) {}
    }
  }

  window.addEventListener("resize", setInnerSizeVars, { passive: true });
  window.addEventListener("orientationchange", function () {
    setTimeout(nudgeReflow, 0);
    setTimeout(nudgeReflow, 120);
    setTimeout(nudgeReflow, 350);
  });
  document.addEventListener("visibilitychange", function () {
    if (!document.hidden) setTimeout(setInnerSizeVars, 50);
  });
  if (window.visualViewport) {
    window.visualViewport.addEventListener("resize", setInnerSizeVars, { passive: true });
    window.visualViewport.addEventListener("scroll", setInnerSizeVars, { passive: true });
  }
  setInnerSizeVars();

  if (uaNeedsAggressiveViewportGlue()) {
    function samWvInnerBump() {
      setInnerSizeVars();
      try {
        window.dispatchEvent(new Event("resize"));
      } catch (e5) {}
    }
    window.addEventListener(
      "load",
      function () {
        setTimeout(samWvInnerBump, 0);
        setTimeout(samWvInnerBump, 200);
        setTimeout(samWvInnerBump, 500);
      },
      { once: true }
    );
    document.addEventListener(
      "focusin",
      function () {
        setTimeout(setInnerSizeVars, 0);
        setTimeout(setInnerSizeVars, 120);
        setTimeout(setInnerSizeVars, 320);
      },
      true
    );
    document.addEventListener(
      "focusout",
      function () {
        setTimeout(setInnerSizeVars, 0);
        setTimeout(setInnerSizeVars, 200);
      },
      true
    );
  }
})();

/**
 * 底栏 position:fixed 锚在布局视口底；iOS Safari / Android Chrome 等地址栏显隐时与可视底错位。
 * 用 visualViewport 与 innerHeight 的差值作 bottom / padding 补偿；滚动用 rAF 节流（安卓常随滚动收栏）。
 * 微信 / FB / LINE / UC / 三星 / WebView 等再叠加 load、focus 补算（与上一段 UA 列表一致）。
 */
(function () {
  var vvInsetRaf = 0;
  function syncVisualViewportBottomInset() {
    try {
      var vv = window.visualViewport;
      if (!vv) {
        document.documentElement.style.setProperty("--askbible-vv-layout-bottom-inset", "0px");
        return;
      }
      var gap = window.innerHeight - vv.offsetTop - vv.height;
      if (gap < 0 || !isFinite(gap)) gap = 0;
      document.documentElement.style.setProperty(
        "--askbible-vv-layout-bottom-inset",
        Math.round(gap) + "px"
      );
    } catch (e) {}
  }
  function scheduleSyncVisualViewportBottomInset() {
    if (vvInsetRaf) return;
    vvInsetRaf = requestAnimationFrame(function () {
      vvInsetRaf = 0;
      syncVisualViewportBottomInset();
    });
  }
  if (typeof window === "undefined" || !window.visualViewport) {
    try {
      document.documentElement.style.setProperty("--askbible-vv-layout-bottom-inset", "0px");
    } catch (e2) {}
    return;
  }
  syncVisualViewportBottomInset();
  window.visualViewport.addEventListener("resize", syncVisualViewportBottomInset, {
    passive: true,
  });
  window.visualViewport.addEventListener("scroll", scheduleSyncVisualViewportBottomInset, {
    passive: true,
  });
  window.addEventListener("resize", syncVisualViewportBottomInset, { passive: true });
  window.addEventListener("scroll", scheduleSyncVisualViewportBottomInset, { passive: true });
  window.addEventListener("orientationchange", function () {
    setTimeout(syncVisualViewportBottomInset, 0);
    setTimeout(syncVisualViewportBottomInset, 120);
    setTimeout(syncVisualViewportBottomInset, 350);
  });
  document.addEventListener("visibilitychange", function () {
    if (!document.hidden) setTimeout(syncVisualViewportBottomInset, 50);
  });
  window.addEventListener("pageshow", function (ev) {
    if (ev && ev.persisted) setTimeout(syncVisualViewportBottomInset, 0);
  });

  function uaGlueForVvInset() {
    try {
      if (typeof window.__askBibleUaNeedsAggressiveViewportGlue === "function") {
        return window.__askBibleUaNeedsAggressiveViewportGlue();
      }
    } catch (e6) {}
    return false;
  }
  if (uaGlueForVvInset()) {
    function samWvVvBump() {
      syncVisualViewportBottomInset();
      setTimeout(syncVisualViewportBottomInset, 0);
      setTimeout(syncVisualViewportBottomInset, 90);
      setTimeout(syncVisualViewportBottomInset, 260);
    }
    window.addEventListener("load", samWvVvBump, { once: true });
    document.addEventListener("focusin", samWvVvBump, true);
    document.addEventListener("focusout", samWvVvBump, true);
  }
})();
