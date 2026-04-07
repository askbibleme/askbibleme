/**
 * 将 body 下原有节点包进统一管理布局；依赖 body[data-admin-current] 与 admin-shell.css。
 */
(function () {
  if (document.body.dataset.adminShellMounted === "1") return;
  document.body.dataset.adminShellMounted = "1";

  const current = String(document.body.getAttribute("data-admin-current") || "").trim();

  const NAV_GROUPS = [
    {
      sub: "总览",
      items: [{ href: "/admin-hub.html", label: "管理首页" }],
    },
    {
      sub: "站点与展示",
      items: [
        { href: "/site-chrome.html", label: "顶栏与底栏" },
        { href: "/seo-settings.html", label: "SEO 设置" },
        { href: "/color-themes.html", label: "全局配色" },
        { href: "/promo-edit.html", label: "宣传页正文" },
        { href: "/home-layout-map.html", label: "主页版面结构" },
      ],
    },
    {
      sub: "数据",
      items: [{ href: "/admin-analytics.html", label: "访问统计" }],
    },
    {
      sub: "读经页内",
      items: [
        { href: "/video-center.html", label: "章节视频上传" },
        {
          href: "/chapter-illustration-prompt.html",
          label: "章节插画 Prompt",
        },
        {
          href: "/bible-character-designer.html",
          label: "圣经人物设计器",
        },
        {
          href: "/#openAdmin",
          label: "规则 · 任务 · 发布",
          note: "在读经首页打开大面板",
        },
      ],
    },
  ];

  function pathMatchesCurrent(href) {
    if (!current || !href || href.startsWith("/#")) return false;
    try {
      const u = new URL(href, window.location.origin);
      return u.pathname === current;
    } catch {
      return href === current;
    }
  }

  /** 侧栏浅色底：主页字标 askbible-wordmark.svg 为浅色字，换用同构图深色字 askbible-wordmark-ink.svg；自定义 https 图保持原 URL */
  function adminShellBrandImageUrl(logoUrl) {
    const u = String(logoUrl || "").trim();
    if (!u) return "/assets/brand/askbible-wordmark-ink.svg";
    const needle = "askbible-wordmark.svg";
    const i = u.indexOf(needle);
    if (i === -1) return u;
    const after = u.slice(i + needle.length);
    if (after.startsWith("?") || after === "") {
      return u.slice(0, i) + "askbible-wordmark-ink.svg" + after;
    }
    return u;
  }

  function isSafeBrandImgSrc(u) {
    const s = String(u || "").trim();
    if (!s) return false;
    if (s.startsWith("/") && !s.startsWith("//")) return true;
    return /^https?:\/\//i.test(s);
  }

  function applyAdminShellBrand(top) {
    const t = top && typeof top === "object" ? top : {};
    const rawLogo = String(t.logoUrl || "").trim();
    brand.title =
      String(t.brandTitleAttr || "AskBible.me 首页").trim() || "AskBible.me 首页";

    brand.replaceChildren();

    if (!rawLogo) {
      const mk = (cls, text) => {
        const s = document.createElement("span");
        s.className = cls;
        s.textContent = text;
        return s;
      };
      brand.appendChild(mk("brand-ask", String(t.brandAsk || "Ask")));
      brand.appendChild(mk("brand-bible", String(t.brandBible || "Bible")));
      brand.appendChild(mk("brand-me", String(t.brandMe || ".me")));
      return;
    }

    const imgUrl = adminShellBrandImageUrl(rawLogo);
    if (!isSafeBrandImgSrc(imgUrl)) return;

    const h = Math.max(20, Math.min(80, Number(t.logoHeight) || 36));
    const sideH = Math.min(32, h);
    const alt =
      String(t.brandAsk || "Ask") +
      String(t.brandBible || "Bible") +
      String(t.brandMe || ".me");

    const img = document.createElement("img");
    img.className = "admin-shell-brand-wordmark";
    img.src = imgUrl;
    img.alt = alt;
    img.height = sideH;
    img.style.height = sideH + "px";
    img.style.width = "auto";
    img.style.display = "block";
    img.loading = "eager";
    img.decoding = "async";
    brand.appendChild(img);
  }

  const root = document.createElement("div");
  root.className = "admin-shell-root";

  const aside = document.createElement("aside");
  aside.className = "admin-shell-aside";
  aside.id = "adminShellAside";
  aside.setAttribute("aria-label", "管理后台导航");

  const brandTitle = document.createElement("h1");
  brandTitle.className = "admin-shell-brand-title";

  const brand = document.createElement("a");
  brand.className = "site-brand-link admin-shell-brand";
  brand.href = "/";
  brand.title = "AskBible.me 首页";
  {
    const preload = document.createElement("img");
    preload.className = "admin-shell-brand-wordmark";
    preload.src = "/assets/brand/askbible-wordmark-ink.svg";
    preload.alt = "AskBible.me";
    preload.height = 28;
    preload.style.height = "28px";
    preload.style.width = "auto";
    preload.style.display = "block";
    preload.loading = "eager";
    preload.decoding = "async";
    brand.appendChild(preload);
  }

  brandTitle.appendChild(brand);
  aside.appendChild(brandTitle);

  void (async function syncAdminShellBrandWithSiteChrome() {
    const paths = ["/api/site-chrome", "/api/sitechrome"];
    for (let i = 0; i < paths.length; i++) {
      try {
        const res = await fetch(paths[i], { cache: "no-store" });
        if (res.status === 404) continue;
        if (!res.ok) continue;
        const cfg = await res.json();
        if (cfg && cfg.topbar) applyAdminShellBrand(cfg.topbar);
        return;
      } catch (_) {}
    }
  })();

  for (let g = 0; g < NAV_GROUPS.length; g++) {
    const group = NAV_GROUPS[g];
    const sub = document.createElement("div");
    sub.className = "admin-shell-nav-sub";
    sub.textContent = group.sub;
    aside.appendChild(sub);
    for (let i = 0; i < group.items.length; i++) {
      const it = group.items[i];
      const a = document.createElement("a");
      a.className = "admin-shell-nav-link";
      a.href = it.href;
      a.textContent = it.label;
      if (pathMatchesCurrent(it.href)) {
        a.classList.add("is-current");
        a.setAttribute("aria-current", "page");
      }
      aside.appendChild(a);
    }
  }

  const spacer = document.createElement("div");
  spacer.className = "admin-shell-spacer";
  aside.appendChild(spacer);

  const back = document.createElement("a");
  back.className = "admin-shell-back";
  back.href = "/";
  back.textContent = "返回读经首页";
  aside.appendChild(back);

  const backdrop = document.createElement("button");
  backdrop.type = "button";
  backdrop.className = "admin-shell-backdrop";
  backdrop.setAttribute("aria-label", "关闭菜单");
  backdrop.tabIndex = -1;

  const main = document.createElement("div");
  main.className = "admin-shell-main";

  const mobileBar = document.createElement("div");
  mobileBar.className = "admin-shell-mobile-bar";

  const menuBtn = document.createElement("button");
  menuBtn.type = "button";
  menuBtn.className = "admin-shell-menu-btn";
  menuBtn.setAttribute("aria-expanded", "false");
  menuBtn.setAttribute("aria-controls", "adminShellAside");
  menuBtn.textContent = "目录";

  const mobileTitle = document.createElement("span");
  mobileTitle.className = "admin-shell-mobile-title";
  mobileTitle.textContent = "管理后台";

  mobileBar.append(menuBtn, mobileTitle);

  const inner = document.createElement("div");
  inner.className = "admin-shell-main-inner";

  const self = document.currentScript;
  while (document.body.firstChild && document.body.firstChild !== self) {
    inner.appendChild(document.body.firstChild);
  }

  function closeDrawer() {
    aside.classList.remove("is-open");
    backdrop.classList.remove("is-visible");
    menuBtn.setAttribute("aria-expanded", "false");
  }

  function openDrawer() {
    aside.classList.add("is-open");
    backdrop.classList.add("is-visible");
    menuBtn.setAttribute("aria-expanded", "true");
  }

  menuBtn.addEventListener("click", function () {
    if (aside.classList.contains("is-open")) closeDrawer();
    else openDrawer();
  });

  backdrop.addEventListener("click", closeDrawer);

  aside.addEventListener("click", function (e) {
    const t = e.target;
    if (t && t.closest && t.closest("a.admin-shell-nav-link")) {
      closeDrawer();
    }
  });

  window.addEventListener(
    "resize",
    function () {
      if (window.matchMedia("(min-width: 901px)").matches) closeDrawer();
    },
    { passive: true }
  );

  main.append(mobileBar, inner);
  root.append(aside, backdrop, main);
  document.body.appendChild(root);
})();
