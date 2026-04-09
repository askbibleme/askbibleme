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
      items: [
        { href: "/admin-hub.html", label: "管理首页" },
        {
          href: "/#openMemberHub",
          label: "会员登录",
          note: "进入读经首页并打开会员/登录",
        },
        {
          href: "/admin-user-login.html",
          label: "用户登录页",
          note: "在此用邮箱密码登录读经账号（写入本机 token，与首页会员登录相同）",
        },
      ],
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
      sub: "插画",
      items: [
        {
          href: "/illustration-admin.html",
          label: "插画管理",
        },
        {
          href: "/chapter-illustration-library.html",
          label: "章插图总表",
          note: "按章节查看、跳转生成、删除页面插图/文件",
        },
        {
          href: "/generated-png-thumbs.html",
          label: "PNG 与缩略图",
          note: "public/generated 与 thumbs 批量处理",
        },
        { href: "/bible-character-designer.html", label: "圣经人物设计器" },
        {
          href: "/bible-character-designer.html#repair-images",
          label: "人物图片预检修复",
          note: "失效引用统计、预检报告、一键修复",
        },
        {
          href: "/chapter-key-people.html",
          label: "章末人物表（全局）",
          note: "全版本语言共用的 chapter_key_people.json",
        },
      ],
    },
    {
      sub: "读经页内",
      items: [
        { href: "/video-center.html", label: "章节视频上传" },
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

  /** 侧栏深底（同顶栏）：用浅字 askbible-wordmark.svg；站点若配 ink 版则改为 .svg；其它 URL 原样 */
  function adminShellBrandImageUrl(logoUrl) {
    const u = String(logoUrl || "").trim();
    if (!u) return "/assets/brand/askbible-wordmark.svg";
    if (u.includes("askbible-wordmark-ink.svg")) {
      return u.replace(/askbible-wordmark-ink\.svg/g, "askbible-wordmark.svg");
    }
    return u;
  }

  function isSafeBrandImgSrc(u) {
    const s = String(u || "").trim();
    if (!s) return false;
    if (s.startsWith("/") && !s.startsWith("//")) return true;
    return /^https?:\/\//i.test(s);
  }

  const ADMIN_SHELL_BRAND_TEXT_DEFAULT = {
    brandAsk: "Ask",
    brandBible: "Bible",
    brandMe: ".me",
  };

  /**
   * 横版字标 SVG 加载失败时：ink 路径会改试浅字 .svg；浅字版失败则不再换 ink（深底不可读），回退文字品牌。
   * 不使用应用方形图标（icon-180），避免侧栏误显示「四方大 LOGO」。
   */
  function wordmarkAlternateSvgPath(fromSrc) {
    try {
      const u = new URL(String(fromSrc || ""), window.location.origin);
      let p = u.pathname;
      if (p.includes("askbible-wordmark-ink.svg")) {
        return (
          p.replace("askbible-wordmark-ink.svg", "askbible-wordmark.svg") +
          u.search +
          u.hash
        );
      }
      /* 深侧栏上浅字版失败时不再换 ink（墨色在深色底不可读），由逻辑回退到文字品牌 */
      if (
        p.includes("askbible-wordmark.svg") &&
        !p.includes("askbible-wordmark-ink")
      ) {
        return "";
      }
    } catch (_) {}
    return "";
  }

  function mergeBrandParts(t) {
    return Object.assign({}, ADMIN_SHELL_BRAND_TEXT_DEFAULT, t && typeof t === "object" ? t : {});
  }

  function appendTextBrand(t) {
    const tp = mergeBrandParts(t);
    const mk = (cls, text) => {
      const s = document.createElement("span");
      s.className = cls;
      s.textContent = text;
      return s;
    };
    brand.appendChild(mk("brand-ask", String(tp.brandAsk || "Ask")));
    brand.appendChild(mk("brand-bible", String(tp.brandBible || "Bible")));
    brand.appendChild(mk("brand-me", String(tp.brandMe || ".me")));
  }

  function wireAdminShellWordmarkFallback(img, t) {
    const tp = mergeBrandParts(t);
    img.addEventListener(
      "error",
      function () {
        const step = img.dataset.abShellFb || "";
        if (step === "") {
          const alt = wordmarkAlternateSvgPath(img.src);
          if (alt) {
            img.dataset.abShellFb = "altSvg";
            img.src = alt;
            return;
          }
          img.remove();
          appendTextBrand(tp);
          return;
        }
        if (step === "altSvg") {
          img.remove();
          appendTextBrand(tp);
        }
      },
      { once: false }
    );
  }

  function appendWordmarkImg(t, imgUrl, sideH) {
    const tp = mergeBrandParts(t);
    const alt =
      String(tp.brandAsk || "Ask") +
      String(tp.brandBible || "Bible") +
      String(tp.brandMe || ".me");
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
    wireAdminShellWordmarkFallback(img, t);
    brand.appendChild(img);
  }

  function applyAdminShellBrand(top) {
    const t = top && typeof top === "object" ? top : {};
    const rawLogo = String(t.logoUrl || "").trim();
    brand.title =
      String(t.brandTitleAttr || "AskBible.me 首页").trim() || "AskBible.me 首页";

    brand.replaceChildren();

    if (!rawLogo) {
      appendTextBrand(t);
      return;
    }

    const imgUrl = adminShellBrandImageUrl(rawLogo);
    if (!isSafeBrandImgSrc(imgUrl)) {
      appendTextBrand(t);
      return;
    }

    const h = Math.max(20, Math.min(80, Number(t.logoHeight) || 36));
    const sideH = Math.min(32, h);
    appendWordmarkImg(t, imgUrl, sideH);
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
    const boot = mergeBrandParts({});
    const preload = document.createElement("img");
    preload.className = "admin-shell-brand-wordmark";
    preload.src = "/assets/brand/askbible-wordmark.svg";
    preload.alt =
      String(boot.brandAsk) + String(boot.brandBible) + String(boot.brandMe);
    preload.height = 28;
    preload.style.height = "28px";
    preload.style.width = "auto";
    preload.style.display = "block";
    preload.loading = "eager";
    preload.decoding = "async";
    wireAdminShellWordmarkFallback(preload, boot);
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
      if (it.note) a.title = String(it.note);
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
