export function buildSystemPrompt({
  scene = "小组",
  template = "讨论版",
  styleTags = ["简洁", "生活化", "适合讨论"],
  customPrompt = "",
}) {
  return `
你是一个中文圣经查经助手。

你的任务：
根据给定经文，生成适合“${scene}”场景使用的读经辅助内容。

固定要求：
1. 按经文自然分段，不要平均机械切段。
2. 不要把每段拆成“观察、解释、应用”三栏。
3. 每一段只给 2 到 4 个问题。
4. 问题必须顺着经文推进，贴近该段内容，不要泛泛而谈。
5. 全章总问题数尽量控制在 15 到 20 个左右。
6. 语气与结构要适合模板：“${template}”。
7. 风格标签：${styleTags.join("、") || "简洁、生活化"}。
8. 请提炼本章关键重复词，控制在 3 到 6 个之间。
9. 重复词不要抓“的、了、就、并且”等无意义虚词，要优先抓有观察价值的词或短语。
10. 重复词只返回“词”和“出现次数”，不要写解释说明。
11. 每个分段必须给出明确的经文范围：rangeStart 和 rangeEnd。
12. 分段标题里也可以自然写出范围，但 JSON 字段里的 rangeStart 和 rangeEnd 必须准确。
13. 尽量避免不同段落使用重复句式。
14. 不要输出祷告，不要输出神学难点。
15. 输出必须是合法 JSON。
16. 不要输出 markdown 代码块，不要在 JSON 外说任何话。

用户补充要求：
${customPrompt || "无"}
  `.trim();
}

export function buildUserPrompt({ bookCn, bookKey, chapter, verses }) {
  const scriptureText = verses.map((v) => `${v.verse}. ${v.cn}`).join("\n");

  return `
请根据以下经文生成读经辅助内容：

书卷：${bookCn}
书卷标识：${bookKey}
章节：第 ${chapter} 章

经文：
${scriptureText}

请严格返回 JSON，格式如下：
{
  "bookCn": "${bookCn}",
  "bookKey": "${bookKey}",
  "chapter": ${chapter},
  "title": "${bookCn}${chapter}章",
  "theme": "本章主题",
  "repeatedWords": [
    {
      "word": "重复词或短语",
      "count": 5
    }
  ],
  "segments": [
    {
      "title": "1. 段落标题",
      "rangeStart": 1,
      "rangeEnd": 5,
      "questions": [
        "问题1",
        "问题2",
        "问题3"
      ]
    }
  ],
  "closing": "一句总结"
}
  `.trim();
}

export function normalizeAiResult(raw, { bookCn, bookKey, chapter }) {
  return {
    bookCn,
    bookKey,
    chapter,
    title: raw.title || `${bookCn}${chapter}章`,
    theme: raw.theme || "",
    repeatedWords: Array.isArray(raw.repeatedWords) ? raw.repeatedWords : [],
    segments: Array.isArray(raw.segments)
      ? raw.segments.map((seg) => ({
          title: seg.title || "",
          rangeStart: Number(seg.rangeStart) || null,
          rangeEnd: Number(seg.rangeEnd) || null,
          questions: Array.isArray(seg.questions) ? seg.questions : [],
        }))
      : [],
    closing: raw.closing || "",
    generatedAt: new Date().toISOString(),
    version: "genesis-offline-v5",
  };
}
