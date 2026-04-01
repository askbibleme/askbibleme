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

const START_CHAPTER = 1;
const END_CHAPTER = 5;
const DELAY_MS = 1200;

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJson(filepath, data) {
  fs.writeFileSync(filepath, JSON.stringify(data, null, 2), "utf8");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function generateOneChapter(client, chapter) {
  const scripture = getMergedScripture("genesis", chapter);

  const systemPrompt = buildSystemPrompt({
    scene: "小组",
    template: "讨论版",
    styleTags: ["简洁", "生活化", "适合讨论"],
    customPrompt:
      "问题要贴着经文推进，适合带领者使用，也适合个人或小组学习。避免空泛、重复、过度神学化。",
  });

  const userPrompt = buildUserPrompt({
    bookCn: scripture.bookCn,
    bookKey: scripture.bookKey,
    chapter: scripture.chapter,
    verses: scripture.verses,
  });

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
    console.error(`第 ${chapter} 章模型原始输出：\n`);
    console.error(rawText);
    throw new Error(`第 ${chapter} 章返回的不是合法 JSON`);
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

  return outputPath;
}

async function main() {
  if (!process.env.OPENAI_API_KEY) {
    throw new Error("缺少 OPENAI_API_KEY，请检查 .env");
  }

  const client = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY,
  });

  const success = [];
  const failed = [];

  console.log(`开始批量生成：《创世记》${START_CHAPTER}–${END_CHAPTER}章`);
  console.log("");

  for (let chapter = START_CHAPTER; chapter <= END_CHAPTER; chapter++) {
    try {
      console.log(`正在生成：创世记 ${chapter} 章`);
      const outputPath = await generateOneChapter(client, chapter);
      success.push({ chapter, outputPath });
      console.log(`已保存：${outputPath}`);
    } catch (error) {
      failed.push({
        chapter,
        error: error.message || "未知错误",
      });
      console.error(`生成失败：创世记 ${chapter} 章 -> ${error.message}`);
    }

    if (chapter < END_CHAPTER) {
      await sleep(DELAY_MS);
    }

    console.log("");
  }

  console.log("========== 批量生成结束 ==========");
  console.log(`成功：${success.length} 章`);
  console.log(`失败：${failed.length} 章`);

  const outputDir = path.join(PROJECT_ROOT, "content", "genesis");
  ensureDir(outputDir);

  if (failed.length) {
    const logPath = path.join(outputDir, "_failed.json");
    writeJson(logPath, {
      generatedAt: new Date().toISOString(),
      failed,
    });
    console.log(`失败记录已保存：${logPath}`);
  }

  const summaryPath = path.join(outputDir, "_summary.json");
  writeJson(summaryPath, {
    generatedAt: new Date().toISOString(),
    successCount: success.length,
    failedCount: failed.length,
    success,
    failed,
    version: "genesis-offline-v1",
  });

  console.log(`汇总已保存：${summaryPath}`);
}

main().catch((error) => {
  console.error("批量生成中断：", error.message);
  process.exit(1);
});
