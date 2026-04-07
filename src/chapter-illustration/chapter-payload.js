/**
 * Normalize published chapter JSON into a stable chapter payload for the pipeline.
 */
export function themeToFlatString(themeInput) {
  if (themeInput == null) return "";
  if (typeof themeInput === "object" && !Array.isArray(themeInput)) {
    const core = String(themeInput.core ?? themeInput.summary ?? "").trim();
    const resolution = String(
      themeInput.resolution ?? themeInput.resolve ?? themeInput.turn ?? ""
    ).trim();
    return [resolution, core].filter(Boolean).join("；");
  }
  return String(themeInput || "").trim();
}

export function buildChapterPayloadFromPublished(data, meta) {
  const versionId = String(meta?.versionId ?? data?.version ?? "");
  const lang = String(meta?.lang ?? data?.contentLang ?? "");
  const bookId = String(meta?.bookId ?? data?.bookId ?? "");
  const chapterNumber = Number(meta?.chapter ?? data?.chapter ?? 0);
  const bookName = String(data?.bookLabel || bookId || "").trim();
  const theme = data?.theme ?? "";
  const themeFlat = themeToFlatString(theme);

  const segments = Array.isArray(data?.segments) ? data.segments : [];
  const storyUnits = segments.map((s, i) => ({
    id: `seg_${i}`,
    title: String(s?.title || "").trim(),
    rangeStart: s?.rangeStart,
    rangeEnd: s?.rangeEnd,
  }));

  const segmentTitles = storyUnits.map((u) => u.title).filter(Boolean);
  const summary =
    segmentTitles[0] ||
    (themeFlat ? themeFlat.slice(0, 120) : "") ||
    `${bookName} ${chapterNumber}`;

  const scriptureText = segmentTitles.join("\n");

  const combinedForKeys = [themeFlat, summary, scriptureText].join(" ");

  return {
    bookName,
    bookId,
    chapterNumber,
    versionId,
    lang,
    scriptureText,
    theme,
    themeFlat,
    summary,
    keyPeople: extractKeyPeople(combinedForKeys),
    keyLocations: extractKeyLocations(combinedForKeys),
    storyUnits,
    raw: data,
  };
}

function extractKeyPeople(text) {
  const names = [];
  /* 长名在前，避免短前缀误切；含创世记系列常用称谓 */
  const patterns =
    /以实玛利|亚伯拉罕|麦基洗德|波提非拉|波提乏|便雅悯|利百加|撒母耳|彼得|保罗|马利亚|耶稣|基督|参孙|喇合|路得|波阿斯|所罗门|以利亚|以利沙|但以理|约伯|亚伯兰|撒拉|撒莱|夏甲|挪亚|亚当|夏娃|以扫|以撒|雅各|约瑟|犹大|流便|罗得|拉班|拉结|利亚|辟拉|悉帕|他拉|哈兰|法老|摩西|大卫|扫罗|闪|含|雅弗/g;
  let m;
  const t = String(text || "");
  while ((m = patterns.exec(t)) && names.length < 12) {
    if (!names.includes(m[0])) names.push(m[0]);
  }
  return names;
}

function extractKeyLocations(text) {
  const out = [];
  const patterns =
    /旷野|埃及|迦南|伯特利|示剑|耶路撒冷|巴比伦|西奈|红海|约旦河|井|坛|会幕|圣殿|橄榄山/g;
  let m;
  const t = String(text || "");
  while ((m = patterns.exec(t)) && out.length < 8) {
    if (!out.includes(m[0])) out.push(m[0]);
  }
  return out;
}
