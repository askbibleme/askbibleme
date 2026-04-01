import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, "../..");

const cnXml = fs.readFileSync(
  path.join(PROJECT_ROOT, "data", "chi-cuv-simp.usfx.xml"),
  "utf8"
);

const enXml = fs.readFileSync(
  path.join(PROJECT_ROOT, "data", "eng-web.usfx.xml"),
  "utf8"
);

/**
 * 第一版先只支持创世记
 */
const BOOK_MAP = {
  genesis: {
    bookCn: "创世记",
    usfx: "GEN",
    chapters: 50,
    outputDir: "genesis",
  },
};

export function getBookConfig(bookKey) {
  const book = BOOK_MAP[String(bookKey || "").toLowerCase()];
  if (!book) {
    throw new Error(`暂不支持该卷书：${bookKey}`);
  }
  return book;
}

function stripXml(text) {
  return String(text || "")
    .replace(/<f\b[^>]*>[\s\S]*?<\/f>/g, " ")
    .replace(/<x\b[^>]*>[\s\S]*?<\/x>/g, " ")
    .replace(/<fig\b[^>]*>[\s\S]*?<\/fig>/g, " ")
    .replace(/<table\b[^>]*>[\s\S]*?<\/table>/g, " ")
    .replace(/<ref\b[^>]*>[\s\S]*?<\/ref>/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractChapter(xml, bookCode, chapter) {
  const bookRe = new RegExp(
    `<book\\b[^>]*id="${bookCode}"[^>]*>([\\s\\S]*?)<\\/book>`,
    "i"
  );
  const bookMatch = xml.match(bookRe);
  if (!bookMatch) return [];

  const bookBody = bookMatch[1];
  const chapterRe = new RegExp(
    `<c\\b[^>]*id="${chapter}"[^>]*\\/>[\\s\\S]*?(?=<c\\b[^>]*id="\\d+"[^>]*\\/>|$)`,
    "i"
  );
  const chapterMatch = bookBody.match(chapterRe);
  if (!chapterMatch) return [];

  const chunk = chapterMatch[0];
  const verseRe1 = /<v\b[^>]*id="(\d+)"[^>]*\/>([\s\S]*?)<ve\/>/g;
  const verseRe2 =
    /<v\b[^>]*id="(\d+)"[^>]*\/>([\s\S]*?)(?=<v\b[^>]*id="\d+"[^>]*\/>|$)/g;

  const verses = [];
  let m;

  while ((m = verseRe1.exec(chunk)) !== null) {
    const verseNo = Number(m[1]);
    const verseText = stripXml(m[2]);
    if (verseText) {
      verses.push({ verse: verseNo, text: verseText });
    }
  }

  if (!verses.length) {
    while ((m = verseRe2.exec(chunk)) !== null) {
      const verseNo = Number(m[1]);
      const verseText = stripXml(m[2]);
      if (verseText) {
        verses.push({ verse: verseNo, text: verseText });
      }
    }
  }

  return verses;
}

function mergeVerses(cnVerses, enVerses) {
  const map = new Map();

  cnVerses.forEach((v) => {
    map.set(v.verse, {
      verse: v.verse,
      cn: v.text,
      en: "",
    });
  });

  enVerses.forEach((v) => {
    const row = map.get(v.verse) || {
      verse: v.verse,
      cn: "",
      en: "",
    };
    row.en = v.text;
    map.set(v.verse, row);
  });

  return Array.from(map.values()).sort((a, b) => a.verse - b.verse);
}

export function getMergedScripture(bookKey, chapter) {
  const book = getBookConfig(bookKey);
  const chapterNum = Number(chapter);

  if (!Number.isInteger(chapterNum) || chapterNum < 1 || chapterNum > book.chapters) {
    throw new Error(`章节范围不正确：${chapter}`);
  }

  const cnVerses = extractChapter(cnXml, book.usfx, chapterNum);
  const enVerses = extractChapter(enXml, book.usfx, chapterNum);

  if (!cnVerses.length) {
    throw new Error(`没有读到经文：${book.bookCn} ${chapterNum}`);
  }

  return {
    bookKey,
    bookCn: book.bookCn,
    chapter: chapterNum,
    verses: mergeVerses(cnVerses, enVerses),
    outputDir: book.outputDir,
  };
}