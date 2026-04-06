const CACHE_NAME = "askbible-static-v41";
const STATIC_ASSETS = [
  "/",
  "/index.html",
  "/download.html",
  "/promo.html",
  "/promo-edit.html",
  "/vision.html",
  "/vision.css",
  "/styles.css",
  "/promo.css",
  "/main.js",
  "/manifest.webmanifest",
  "/assets/icons/icon.svg",
  "/assets/icons/icon-maskable.svg",
];

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

  /* 管理后台与配置工具 HTML：勿 cache-first，否则内联脚本长期停留在旧版（如 site-chrome 图标 UI） */
  if (
    /\/(?:admin-hub|site-chrome|promo-edit|color-themes|chapter-illustration|article-studio|admin-analytics|seo-settings)\.html$/i.test(
      url.pathname
    )
  ) {
    event.respondWith(fetch(req));
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
