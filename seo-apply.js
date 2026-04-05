/**
 * 从 /api/site-seo 拉取配置，覆盖当前页的 <title>、meta 与 JSON-LD 描述字段。
 * 页面须在 <html> 上标注 data-askbible-seo-page="index" | "promo"，且 JSON-LD 脚本带 id="askbibleSeoJsonLd"。
 */
(function () {
  var DEFAULT_PRODUCTION_ORIGIN = "https://askbible.me";

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
    if (p === "5173" || p === "4173" || p === "8081" || p === "8080" || p === "3001") return true;
    if (/^55\d{2}$/.test(p)) return true;
    return false;
  }

  /**
   * 常见静态开发端口（Vite / Live Server 等）把 API 指到 127.0.0.1:3000；
   * 页面与 Node 同端口（如整站 8080）时须保持空串走同源，勿写死 3000。
   */
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
      var h = window.location.hostname || "";
      var port = String(window.location.port || "").trim();
      var isLoop = h === "localhost" || h === "127.0.0.1";
      if (isLoop && proto === "http:") {
        if (!port || port === "80" || port === "3000") return "";
        if (isLikelyStaticDevServerPort(port)) return "http://127.0.0.1:3000";
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

  function getPageKey() {
    try {
      var html = document.documentElement;
      var v = html && html.getAttribute("data-askbible-seo-page");
      if (v === "promo" || v === "index") return v;
      var path = (window.location && window.location.pathname) || "";
      if (/promo\.html$/i.test(path) || /\/promo\/?$/i.test(path)) return "promo";
    } catch (e) {}
    return "index";
  }

  function setMetaName(name, content) {
    if (content == null) return;
    var s = String(content).trim();
    if (!s) return;
    var el = document.querySelector('meta[name="' + name.replace(/"/g, "") + '"]');
    if (!el) {
      el = document.createElement("meta");
      el.setAttribute("name", name);
      document.head.appendChild(el);
    }
    el.setAttribute("content", s);
  }

  function setMetaProperty(prop, content) {
    if (content == null) return;
    var s = String(content).trim();
    if (!s) return;
    var el = document.querySelector('meta[property="' + prop.replace(/"/g, "") + '"]');
    if (!el) {
      el = document.createElement("meta");
      el.setAttribute("property", prop);
      document.head.appendChild(el);
    }
    el.setAttribute("content", s);
  }

  function applyJsonLd(scriptEl, page) {
    if (!scriptEl || !scriptEl.textContent) return;
    var web = page.jsonLdWebsiteDescription;
    var soft = page.jsonLdSoftwareDescription;
    var webp = page.jsonLdWebPageDescription;
    try {
      var obj = JSON.parse(scriptEl.textContent);
      var graph = obj && obj["@graph"];
      if (!Array.isArray(graph)) return;
      for (var i = 0; i < graph.length; i++) {
        var node = graph[i];
        if (!node || typeof node !== "object") continue;
        var t = node["@type"];
        if (t === "WebSite" && web != null && String(web).trim()) {
          node.description = String(web).trim();
        }
        if (t === "SoftwareApplication" && soft != null && String(soft).trim()) {
          node.description = String(soft).trim();
        }
        if (t === "WebPage" && webp != null && String(webp).trim()) {
          node.description = String(webp).trim();
        }
      }
      scriptEl.textContent = JSON.stringify(obj);
    } catch (e) {}
  }

  /** 仅请求扁平路径，减少控制台重复 404；server.js 已注册 GET /api/siteseo */
  var SITE_SEO_PUBLIC_PATH = "/api/siteseo";

  function safeJson(res) {
    if (!res || !res.ok) return Promise.resolve(null);
    return res.json().catch(function () {
      return null;
    });
  }

  function fetchSiteSeoJson() {
    var opts = {
      cache: "no-store",
      headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
    };
    return fetch(apiUrl(SITE_SEO_PUBLIC_PATH), opts)
      .then(function (res) {
        if (res.ok) return safeJson(res);
        if (res.status !== 404) return null;
        return fetch(apiUrl("/api/site-seo"), opts).then(function (res2) {
          return safeJson(res2);
        });
      })
      .catch(function () {
        return null;
      });
  }

  function run() {
    var key = getPageKey();
    fetchSiteSeoJson()
      .then(function (data) {
        try {
          if (!data || typeof data !== "object") return;
          var page = data[key];
          if (!page || typeof page !== "object") return;
          if (page.documentTitle) {
            document.title = String(page.documentTitle);
          }
          setMetaName("description", page.metaDescription);
          setMetaName("keywords", page.metaKeywords);
          setMetaProperty("og:site_name", page.ogSiteName);
          setMetaProperty("og:title", page.ogTitle);
          setMetaProperty("og:description", page.ogDescription);
          setMetaName("twitter:title", page.twitterTitle);
          setMetaName("twitter:description", page.twitterDescription);
          setMetaName("apple-mobile-web-app-title", page.appleMobileWebAppTitle);
          var ld = document.getElementById("askbibleSeoJsonLd");
          if (ld) applyJsonLd(ld, page);
        } catch (e) {}
      })
      .catch(function () {});
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();
