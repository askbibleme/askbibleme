const CACHE_NAME = "askbible-static-v22";
const STATIC_ASSETS = [
  "/",
  "/index.html",
  "/download.html",
  "/styles.css",
  "/main.js",
  "/pwa-install-hint.js",
  "/manifest.webmanifest",
  "/assets/icons/icon-16.png",
  "/assets/icons/icon-32.png",
  "/assets/icons/icon-180.png",
  "/assets/icons/icon-192.png",
  "/assets/icons/icon-512.png",
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
