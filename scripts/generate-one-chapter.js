import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import OpenAI from "openai";
import { getMergedScripture } from "./utils/bible.js";
import {
  buildSystemPrompt,
  buildUserPrompt,
  normalizeAiResult,
} from "./utils/prompt.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, "..");

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJson(filepath, data) {
  fs.writeFileSync(filepath, JSON.stringify(data, null, 2), "utf8");
}

function parseArgs() {
  const [, , bookKeyArg, chapterArg] = process.argv;

  if (!bookKeyArg || !chapterArg) {
    console.error("用法：node scripts/generate-one-chapter.js genesis 28");
    process.exit(1);
  }

  const bookKey = String(bookKeyArg).toLowerCase();
  const chapter = Number(chapterArg);

  if (!Number.isInteger(chapter) || chapter < 1) {
    console.error("章节必须是正整数");
    process.exit(1);
  }

  return { bookKey, chapter };
}

async function main() {
  if (!process.env.OPENAI_API_KEY) {
    throw new Error("缺少 OPENAI_API_KEY，请检查 .env");
  }

  const { bookKey, chapter } = parseArgs();

  const scripture = getMergedScripture(bookKey, chapter);

  const client = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
  });

  const systemPrompt = buildSystemPrompt({
    scene: "小组",
    template: "讨论版",
    styleTags: ["简洁", "生活化", "适合讨论"],
    customPrompt: "问题要贴着经文推进，适合带领者使用，也适合个人或小组学习。",
  });

  const userPrompt = buildUserPrompt({
    bookCn: scripture.bookCn,
    bookKey: scripture.bookKey,
    chapter: scripture.chapter,
    verses: scripture.verses,
  });

  console.log(`开始生成：${scripture.bookCn} ${scripture.chapter}章`);

  const response = await client.responses.create({
    model: "gpt-5.4",
    input: [
      {
        role: "system",
        content: [{ type: "input_text", text: systemPrompt }],
      },
      {
        role: "user",
        content: [{ type: "input_text", text: userPrompt }],
      },
    ],
  });

  const rawText = response.output_text;

  let parsed;
  try {
    parsed = JSON.parse(rawText);
  } catch (error) {
    console.error("模型原始输出如下：\n");
    console.error(rawText);
    throw new Error("模型返回的不是合法 JSON");
  }

  const normalized = normalizeAiResult(parsed, {
    bookCn: scripture.bookCn,
    bookKey: scripture.bookKey,
    chapter: scripture.chapter,
  });

  const outputDir = path.join(PROJECT_ROOT, "content", scripture.outputDir);
  ensureDir(outputDir);

  const outputPath = path.join(outputDir, `${scripture.chapter}.json`);
  writeJson(outputPath, normalized);

  console.log(`生成完成：${outputPath}`);
}

main().catch((error) => {
  console.error("生成失败：", error.message);
  process.exit(1);
});
