(function () {
  const VID_KEY = "ab_analytics_vid_v1";
  const TOKEN_KEY = "bible_user_auth_token_v1";

  function getVisitorId() {
    try {
      let v = localStorage.getItem(VID_KEY);
      if (!v || v.length < 8) {
        v =
          (typeof crypto !== "undefined" &&
            crypto.randomUUID &&
            crypto.randomUUID()) ||
          "v" + Date.now() + Math.random().toString(36).slice(2, 12);
        localStorage.setItem(VID_KEY, v);
      }
      return v;
    } catch {
      return "anon_" + Date.now();
    }
  }

  function apiOrigin() {
    try {
      const proto = window.location.protocol || "";
      if (proto === "capacitor:" || proto === "file:") {
        return "https://askbible.me";
      }
    } catch (_) {}
    return "";
  }

  function apiUrl(path) {
    const p = path.startsWith("/") ? path : "/" + path;
    const base = apiOrigin();
    if (!base) return p;
    try {
      return new URL(p, base.endsWith("/") ? base : base + "/").href;
    } catch {
      return base.replace(/\/$/, "") + p;
    }
  }

  function collect(kind) {
    var token = "";
    try {
      token = localStorage.getItem(TOKEN_KEY) || "";
    } catch (_) {}
    var headers = { "Content-Type": "application/json" };
    if (token) headers.Authorization = "Bearer " + token;
    fetch(apiUrl("/api/analytics/collect"), {
      method: "POST",
      headers: headers,
      body: JSON.stringify({ visitorId: getVisitorId(), kind: kind }),
      keepalive: true,
    }).catch(function () {});
  }

  collect("pv");

  if (typeof document === "undefined" || !document.visibilityState) return;

  var hbTimer = null;
  function scheduleHb() {
    if (hbTimer) clearInterval(hbTimer);
    hbTimer = setInterval(function () {
      if (document.visibilityState === "visible") collect("hb");
    }, 45000);
  }

  if (document.visibilityState === "visible") scheduleHb();

  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "visible") {
      collect("hb");
      scheduleHb();
    } else if (hbTimer) {
      clearInterval(hbTimer);
      hbTimer = null;
    }
  });
})();
