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
      ],
    },
    {
      sub: "数据",
      items: [{ href: "/admin-analytics.html", label: "访问统计" }],
    },
    {
      sub: "读经页内",
      items: [
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
  brand.innerHTML =
    '<span class="brand-ask">Ask</span><span class="brand-bible">Bible</span><span class="brand-me">.me</span>';

  brandTitle.appendChild(brand);
  aside.appendChild(brandTitle);

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
