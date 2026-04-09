/**
 * 更新本常量即丢弃旧 Cache Storage，用户下次激活 SW 后生效。
 * 改版频繁时也可只靠下方「网络优先」资源自动拉新； bump 仍用于强制换 SW 脚本本体。
 */
const CACHE_NAME = "askbible-static-v53";

const STATIC_ASSETS = [
  "/manifest.webmanifest",
  "/assets/icons/icon.svg",
  "/assets/icons/icon-maskable.svg",
];

/**
 * 网络优先：同源下除 /assets/ 外，凡 .html / .js / .css 均先请求网络，成功则更新缓存，失败再用缓存。
 * manifest、图标等仍走下方 cache-first，减少无谓请求。
 */
function isNetworkFirstPath(pathname) {
  if (pathname.startsWith("/assets/")) return false;
  if (pathname === "/" || pathname === "/index.html") return true;
  if (/\.html$/i.test(pathname)) return true;
  if (/\.(js|css)$/i.test(pathname)) return true;
  return false;
}

function networkFirstWithCacheFallback(req) {
  return fetch(new Request(req, { cache: "no-store" }))
    .then((res) => {
      if (res && res.ok) {
        const cloned = res.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(req, cloned)).catch(() => {});
      }
      return res;
    })
    .catch(() => caches.match(req));
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(STATIC_ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(
          keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
        )
      )
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith("/api/")) return;
  if (url.pathname.startsWith("/downloads/")) {
    event.respondWith(fetch(req));
    return;
  }

  /* 管理后台与配置工具 HTML：始终直连网络，不经过 SW 缓存策略 */
  if (
    /\/(?:admin-hub|site-chrome|promo-edit|color-themes|admin-analytics|seo-settings|home-layout-map|video-center|bible-character-designer|illustration-admin|chapter-illustration-library|generated-png-thumbs)\.html$/i.test(
      url.pathname
    )
  ) {
    event.respondWith(fetch(new Request(req, { cache: "no-store" })));
    return;
  }

  if (isNetworkFirstPath(url.pathname)) {
    event.respondWith(networkFirstWithCacheFallback(req));
    return;
  }

  event.respondWith(
    caches.match(req).then((cached) => {
      if (cached) return cached;
      return fetch(req).then((res) => {
        const cloned = res.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(req, cloned)).catch(() => {});
        return res;
      });
    })
  );
});
