#!/usr/bin/env node
/**
 * 自检：本仓库 server.js 是否已注册插画管理 gpt-copy（GET probe → 200；POST → 401 请先登录，而非 404）。
 */
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const port = String(38000 + Math.floor(Math.random() * 2500));
const env = {
  ...process.env,
  PORT: port,
  LISTEN_HOST: "127.0.0.1",
};

const child = spawn(process.execPath, ["server.js"], {
  cwd: root,
  env,
  stdio: ["ignore", "pipe", "pipe"],
});

let out = "";
child.stdout.on("data", (c) => {
  out += String(c);
});
child.stderr.on("data", (c) => {
  out += String(c);
});

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  for (let i = 0; i < 40; i++) {
    if (/localhost:\d+/.test(out) || /\[routes\]/.test(out)) break;
    await sleep(100);
  }
  await sleep(400);

  const urlBase = `http://127.0.0.1:${port}`;
  try {
    const probe = await fetch(`${urlBase}/api/chapter-illustration/gpt-copy-probe`, {
      method: "GET",
      cache: "no-store",
    });
    const pt = await probe.text();
    if (probe.status !== 200) {
      console.error("FAIL GET /api/chapter-illustration/gpt-copy-probe → HTTP", probe.status, pt.slice(0, 120));
      child.kill("SIGTERM");
      await sleep(200);
      try {
        child.kill("SIGKILL");
      } catch (_) {}
      process.exit(1);
    }
    console.log("OK  GET /api/chapter-illustration/gpt-copy-probe → HTTP 200");
  } catch (e) {
    console.error("FAIL GET probe", e.message || e);
    child.kill("SIGTERM");
    await sleep(200);
    try {
      child.kill("SIGKILL");
    } catch (_) {}
    process.exit(1);
  }

  const paths = [
    "/api/chapter-illustration/gpt-copy",
    "/api/admin/illustration-admin/gpt-copy",
    "/api/admin/ill-adm-gptcopy",
  ];
  let ok = false;
  for (const p of paths) {
    try {
      const r = await fetch(`${urlBase}${p}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
      const t = await r.text();
      if (r.status === 404) {
        console.error("FAIL", p, "→ HTTP 404");
        continue;
      }
      if (r.status === 401 && /请先登录|login/i.test(t)) {
        console.log("OK  ", p, "→ HTTP 401（路由已注册，需登录后调用）");
        ok = true;
        break;
      }
      console.log("INFO ", p, "→ HTTP", r.status, t.slice(0, 120));
      if (r.status !== 404) ok = true;
    } catch (e) {
      console.error("FAIL", p, e.message || e);
    }
  }

  child.kill("SIGTERM");
  await sleep(200);
  try {
    child.kill("SIGKILL");
  } catch (_) {}

  if (!ok) {
    console.error(
      "\n未检测到可用 gpt-copy 路由。请确认已保存最新 server.js，且未在函数定义之前注册 app.post（见仓库内 server.js 中 handleIllustrationAdminGptCopy）。"
    );
    process.exit(1);
  }
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  try {
    child.kill("SIGKILL");
  } catch (_) {}
  process.exit(1);
});
