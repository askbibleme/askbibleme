/**
 * Regen: extract promo-feed from promo.html → markdown bootstrap.
 * Run: node scripts/build-promo-bootstrap.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const htmlPath = path.join(root, "promo.html");
const outPath = path.join(root, "admin_data", "promo_page.bootstrap.md");

function decodeEntities(s) {
  return s
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)));
}

function stripTags(html) {
  const withBr = html.replace(/<br\s*\/?>/gi, "\n");
  return decodeEntities(
    withBr.replace(/<[^>]+>/g, "").replace(/\n{3,}/g, "\n\n").trim()
  );
}

function takeMatch(s, re) {
  const m = s.match(re);
  if (!m) return { head: "", rest: s };
  return { head: m[0], rest: s.slice(m[0].length) };
}

function parseArticleInner(inner) {
  let s = inner.replace(/^\s+/, "");
  const chunks = [];
  while (s.length) {
    s = s.replace(/^\s+/, "");
    if (!s.length) break;

    let m = s.match(
      /^<h2[^>]*class="promo-h2"[^>]*>([\s\S]*?)<\/h2>/i
    );
    if (m) {
      chunks.push("\n\n## " + stripTags(m[1]) + "\n\n");
      s = s.slice(m[0].length);
      continue;
    }

    m = s.match(/^<p class="promo-p promo-p--emph"[^>]*>([\s\S]*?)<\/p>/i);
    if (m) {
      chunks.push("\n**" + stripTags(m[1]) + "**\n");
      s = s.slice(m[0].length);
      continue;
    }

    m = s.match(/^<p class="promo-p promo-p--en"[^>]*>([\s\S]*?)<\/p>/i);
    if (m) {
      chunks.push("\n" + stripTags(m[1]) + "\n");
      s = s.slice(m[0].length);
      continue;
    }

    m = s.match(/^<p class="promo-p"[^>]*>([\s\S]*?)<\/p>/i);
    if (m) {
      chunks.push(stripTags(m[1]) + "\n\n");
      s = s.slice(m[0].length);
      continue;
    }

    m = s.match(/^<ul class="promo-list">([\s\S]*?)<\/ul>/i);
    if (m) {
      const items = [...m[1].matchAll(/<li>([\s\S]*?)<\/li>/gi)].map(
        (x) => "- " + stripTags(x[1])
      );
      chunks.push("\n" + items.join("\n") + "\n\n");
      s = s.slice(m[0].length);
      continue;
    }

    s = s.slice(1);
  }
  return chunks.join("").trim();
}

const html = fs.readFileSync(htmlPath, "utf8");
const startMark = '<div class="promo-feed">';
const endMark = '<div class="promo-closing">';
const i0 = html.indexOf(startMark);
const i1 = html.indexOf(endMark);
if (i0 < 0 || i1 < 0 || i1 <= i0) {
  console.error("Could not find promo-feed boundaries");
  process.exit(1);
}
let rest = html.slice(i0 + startMark.length, i1);

const parts = [];
while (rest.length) {
  rest = rest.replace(/^\s+/, "");
  if (!rest.length) break;

  const kick = rest.match(
    /^<p class="promo-kicker">\s*<span class="promo-kicker-text">([^<]*)<\/span>\s*<\/p>/i
  );
  if (kick) {
    parts.push(
      `<p class="promo-kicker"><span class="promo-kicker-text">${kick[1]}</span></p>`
    );
    rest = rest.slice(kick[0].length);
    continue;
  }

  const art = rest.match(
    /^<article class="promo-block"[^>]*>([\s\S]*?)<\/article>/i
  );
  if (art) {
    parts.push(parseArticleInner(art[1]));
    rest = rest.slice(art[0].length);
    continue;
  }

  rest = rest.slice(1);
}

const md = parts.join("\n\n").replace(/\n{4,}/g, "\n\n\n").trim() + "\n";
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, md, "utf8");
console.log("Wrote", outPath, md.length, "chars");
