/**
 * 添加到主屏幕 / PWA 安装引导（index.html 等页面共用）
 */
(function () {
  const STORAGE_SNOOZE = "askbible_pwa_hint_snooze_until";
  const SNOOZE_MS = 7 * 24 * 60 * 60 * 1000;

  function isStandalone() {
    return (
      window.matchMedia("(display-mode: standalone)").matches ||
      window.matchMedia("(display-mode: fullscreen)").matches ||
      window.navigator.standalone === true
    );
  }

  function isCapacitorShell() {
    return /Capacitor/i.test(navigator.userAgent || "");
  }

  function isIOS() {
    const ua = navigator.userAgent || "";
    if (/iPhone|iPad|iPod/i.test(ua)) return true;
    return navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1;
  }

  function isMobileViewport() {
    return window.matchMedia("(max-width: 720px)").matches;
  }

  const bar = document.getElementById("addToHomeHint");
  if (!bar || isStandalone() || isCapacitorShell()) return;

  const until = parseInt(localStorage.getItem(STORAGE_SNOOZE) || "0", 10);
  if (until > Date.now()) return;

  const installBtn = document.getElementById("addToHomeInstallBtn");
  const howBtn = document.getElementById("addToHomeHowBtn");
  const laterBtn = document.getElementById("addToHomeLaterBtn");
  const modal = document.getElementById("addToHomeHintModal");
  const stepsEl = document.getElementById("addToHomeHintSteps");
  const modalClose = document.getElementById("addToHomeHintModalClose");

  let deferredPrompt = null;

  function showBar() {
    bar.hidden = false;
  }

  function hideBar() {
    bar.hidden = true;
  }

  function openHow() {
    if (!modal || !stepsEl) return;
    if (isIOS()) {
      stepsEl.innerHTML =
        "<ol class='add-to-home-hint-ol'>" +
        "<li>点击浏览器底部的<strong>分享</strong>按钮 <span class='add-to-home-hint-sym'>□↑</span></li>" +
        "<li>在菜单中找到并点击<strong>添加到主屏幕</strong></li>" +
        "<li>点右上角<strong>添加</strong>，主屏幕会出现 AskBible 图标</li>" +
        "</ol>" +
        "<p class='add-to-home-hint-note'>请使用 Safari 打开本站，第三方内置浏览器可能无此选项。</p>";
    } else {
      stepsEl.innerHTML =
        "<ol class='add-to-home-hint-ol'>" +
        "<li>使用 <strong>Chrome</strong> 打开本站（推荐）</li>" +
        "<li>点浏览器右上角 <strong>⋮</strong> 菜单</li>" +
        "<li>选择<strong>添加到主屏幕</strong>或<strong>安装应用</strong></li>" +
        "<li>确认后，桌面会出现图标，打开后像 App 一样全屏使用</li>" +
        "</ol>" +
        "<p class='add-to-home-hint-note'>若菜单里没有安装项，可先点击底栏的<strong>安装</strong>按钮（部分机型需等待几秒后出现）。</p>";
    }
    modal.hidden = false;
    modal.setAttribute("aria-hidden", "false");
  }

  function closeHow() {
    if (!modal) return;
    modal.hidden = true;
    modal.setAttribute("aria-hidden", "true");
  }

  if (isIOS() && installBtn) installBtn.hidden = true;

  if (isMobileViewport()) showBar();

  window.addEventListener("beforeinstallprompt", (e) => {
    e.preventDefault();
    deferredPrompt = e;
    if (installBtn) {
      installBtn.hidden = false;
      installBtn.textContent = "安装到设备";
    }
    showBar();
  });

  installBtn?.addEventListener("click", async () => {
    if (!deferredPrompt) {
      openHow();
      return;
    }
    deferredPrompt.prompt();
    try {
      await deferredPrompt.userChoice;
    } catch (_) {}
    deferredPrompt = null;
    if (installBtn) installBtn.hidden = true;
  });

  howBtn?.addEventListener("click", () => openHow());
  modalClose?.addEventListener("click", () => closeHow());
  modal?.addEventListener("click", (ev) => {
    if (ev.target === modal) closeHow();
  });

  laterBtn?.addEventListener("click", () => {
    localStorage.setItem(STORAGE_SNOOZE, String(Date.now() + SNOOZE_MS));
    hideBar();
    closeHow();
  });

  document.querySelectorAll(".open-add-to-home-help").forEach((el) => {
    el.addEventListener("click", (ev) => {
      ev.preventDefault();
      openHow();
    });
  });
})();
