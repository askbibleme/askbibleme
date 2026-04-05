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
