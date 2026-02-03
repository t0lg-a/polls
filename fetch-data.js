#!/usr/bin/env node
/**
 * RTWH "hunt the sheet/json" + normalize into polls.json
 *
 * Key changes vs your current hunter:
 * - Excludes analytics/ad/tracking URLs (gtag/gtm/ga/doubleclick/etc.)
 * - CSV detection requires tabular structure (consistent columns), not "has commas"
 * - Captures candidate URLs from:
 *    (a) requests, (b) frames, (c) DOM src/href, (d) non-HTML responses
 * - Fails loudly if no dataset is found (workflow should fail, not silently write empty polls.json)
 */

const fs = require("fs");
const path = require("path");

const OUT_POLLS = path.join(__dirname, "polls.json");
const OUT_SOURCES = path.join(__dirname, "rtwh_sources.json");

const ENTRY_URL = process.env.RTWH_ENTRY_URL || "https://www.racetothewh.com/allpolls";
const WAIT_MS = Number(process.env.RTWH_WAIT_MS || "12000");
const MIN_ROWS = Number(process.env.RTWH_MIN_ROWS || "30");
const MIN_SCORE = Number(process.env.RTWH_MIN_SCORE || "14"); // raise/lower if needed

function nowISO() { return new Date().toISOString(); }
function uniq(arr) { return Array.from(new Set(arr)); }
function normKey(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^\w %/.-]/g, "");
}
function safeNum(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  const s = String(v).trim();
  if (!s) return null;
  const cleaned = s.replace(/%/g, "").replace(/,/g, "");
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}
function safeInt(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && Number.isFinite(v)) return Math.trunc(v);
  const s = String(v).trim();
  if (!s) return null;
  const m = s.match(/(\d[\d,]*)/);
  if (!m) return null;
  const n = Number(m[1].replace(/,/g, ""));
  return Number.isFinite(n) ? Math.trunc(n) : null;
}
function parseDateToISO(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s) return null;

  const d0 = new Date(s);
  if (!isNaN(d0.getTime())) return d0.toISOString().slice(0, 10);

  const mdy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mdy) {
    const mm = Number(mdy[1]), dd = Number(mdy[2]), yy = Number(mdy[3]);
    const d = new Date(Date.UTC(yy, mm - 1, dd));
    return !isNaN(d.getTime()) ? d.toISOString().slice(0, 10) : null;
  }

  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (iso) return s;

  return null;
}
function looksLikeURL(s) { return /^https?:\/\//i.test(String(s ?? "").trim()); }

function isExcludedUrl(u) {
  const s = String(u || "").toLowerCase();
  if (!s.startsWith("http")) return true;
  return (
    s.includes("googletagmanager.com") ||
    s.includes("google-analytics.com") ||
    s.includes("/gtag/js") ||
    s.includes("doubleclick.net") ||
    s.includes("facebook.net") ||
    s.includes("connect.facebook.net") ||
    s.includes("hotjar") ||
    s.includes("segment.com") ||
    s.includes("datadoghq") ||
    s.includes("sentry.io") ||
    s.includes("cloudflareinsights") ||
    s.includes("adsystem") ||
    s.includes("adservice") ||
    s.includes("amazon-adsystem")
  );
}

function isCandidateUrl(u) {
  if (isExcludedUrl(u)) return false;
  const s = String(u || "").toLowerCase();

  // strong signals
  if (s.includes("docs.google.com/spreadsheets")) return true;
  if (s.includes("spreadsheets.google.com")) return true;
  if (s.includes("/gviz/tq")) return true;
  if (s.includes("tqx=out:json")) return true;
  if (s.includes("output=csv")) return true;
  if (s.endsWith(".csv") || s.includes(".csv?")) return true;
  if (s.endsWith(".json") || s.includes(".json?")) return true;

  // medium signals
  if (s.includes("airtable")) return true;
  if (s.includes("poll") && (s.includes("data") || s.includes("api"))) return true;

  return false;
}

/** ---- parsers ---- **/

function parseGviz(text) {
  const m = text.match(/setResponse\(([\s\S]+)\)\s*;?\s*$/);
  if (!m) return null;
  let payload;
  try { payload = JSON.parse(m[1]); } catch { return null; }
  if (!payload?.table?.cols || !payload?.table?.rows) return null;

  const cols = payload.table.cols.map((c, i) => (c && (c.label || c.id)) ? (c.label || c.id) : `col_${i}`);
  const rows = payload.table.rows.map(r => (r.c || []).map(cell => {
    if (!cell) return null;
    return (cell.v !== undefined) ? cell.v : (cell.f !== undefined ? cell.f : null);
  }));
  return { format: "gviz", cols, rows };
}

function isProbablyJavascript(text) {
  const t = text.slice(0, 4000);
  return (
    /(^|\n)\s*(function|var|let|const)\s+/i.test(t) ||
    /window\.|document\.|dataLayer|gtag\(/i.test(t) ||
    /sourceMappingURL=/i.test(t)
  );
}

function parseCSVStrict(text) {
  // Require at least ~5 lines with consistent column count.
  const lines = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
  const nonEmpty = lines.filter(l => l.trim().length > 0);
  if (nonEmpty.length < 6) return null;

  // Reject obvious JS even if it has commas.
  if (isProbablyJavascript(text)) return null;

  function splitCSVLine(line) {
    const out = [];
    let cur = "";
    let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQ) {
        if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; continue; }
        if (ch === '"') { inQ = false; continue; }
        cur += ch;
      } else {
        if (ch === '"') { inQ = true; continue; }
        if (ch === ",") { out.push(cur); cur = ""; continue; }
        cur += ch;
      }
    }
    out.push(cur);
    return out.map(s => s.trim());
  }

  const header = splitCSVLine(nonEmpty[0]);
  if (header.length < 5) return null;

  // Check consistency on next 5 lines
  const testRows = nonEmpty.slice(1, 6).map(splitCSVLine);
  const ok = testRows.every(r => r.length === header.length || r.length >= header.length - 1);
  if (!ok) return null;

  const rows = nonEmpty.slice(1).map(splitCSVLine);
  return { format: "csv", cols: header, rows };
}

function parseJSONTable(text) {
  if (isProbablyJavascript(text)) return null;
  let obj;
  try { obj = JSON.parse(text); } catch { return null; }

  if (Array.isArray(obj) && obj.length && typeof obj[0] === "object" && obj[0] !== null && !Array.isArray(obj[0])) {
    const cols = uniq(obj.flatMap(o => Object.keys(o)));
    const rows = obj.map(o => cols.map(c => o[c]));
    return { format: "json_objects", cols, rows };
  }

  if (obj && Array.isArray(obj.cols) && Array.isArray(obj.rows)) {
    return { format: "json_table", cols: obj.cols, rows: obj.rows };
  }

  return null;
}

function tryParseDataset(body, contentType) {
  const ct = (contentType || "").toLowerCase();
  const b = String(body || "");

  // gviz can come back with JS-ish content-type
  if (b.includes("google.visualization.Query.setResponse")) {
    const g = parseGviz(b);
    if (g) return g;
  }

  if (ct.includes("application/json") || b.trim().startsWith("{") || b.trim().startsWith("[")) {
    const j = parseJSONTable(b);
    if (j) return j;
  }

  if (ct.includes("text/csv") || ct.includes("application/csv") || /output=csv/i.test(ct)) {
    const c = parseCSVStrict(b);
    if (c) return c;
  }

  // last resort CSV (only if it REALLY looks like CSV)
  const c2 = parseCSVStrict(b);
  if (c2) return c2;

  return null;
}

/** ---- scoring ---- **/

function scoreDataset(ds) {
  if (!ds?.cols?.length || !ds?.rows?.length) return -1;

  const colsN = ds.cols.map(normKey);

  // Must look poll-ish
  const mustHave = [
    /pollster|firm|polling|organization/,
    /start|field|begin|from|end|finish|to/,
    /sample|respond|n\b|sample size/,
    /approve|disapprove|dem|rep|gop|trump|biden|harris|margin|spread|race|state|district|senate|house|governor|president/
  ];
  const hit = mustHave.reduce((acc, re) => acc + (colsN.some(c => re.test(c)) ? 1 : 0), 0);

  // heavy penalty if it looks like scripts
  const joined = colsN.join(" ");
  if (/gtag|datalayer|analytics|tag manager/.test(joined)) return -10;

  let score = 0;
  score += hit * 6;
  score += Math.min(ds.rows.length, 400) / 12;
  score += Math.min(ds.cols.length, 80) / 8;

  // If too small, penalize hard
  if (ds.rows.length < MIN_ROWS) score -= 20;

  return score;
}

function pickDataset(datasets) {
  const scored = datasets
    .map(d => ({ ...d, _score: scoreDataset(d) }))
    .sort((a, b) => b._score - a._score);
  const best = scored[0] || null;
  if (!best || best._score < MIN_SCORE) return null;
  return best;
}

/** ---- normalize into polls.json schema your poll.html already uses ---- **/

function buildColLookup(cols) {
  const normed = cols.map(c => normKey(c));
  return {
    cols,
    normed,
    findIdx(patterns) {
      for (const p of patterns) {
        const re = (p instanceof RegExp) ? p : new RegExp(p, "i");
        const idx = normed.findIndex(c => re.test(c));
        if (idx !== -1) return idx;
      }
      return -1;
    }
  };
}

function normalizePollRow(row, metaCols, answerCols) {
  const get = (idx) => (idx >= 0 && idx < row.length) ? row[idx] : null;

  const pollster = get(metaCols.pollsterIdx);
  const sponsor = get(metaCols.sponsorIdx);
  const start = parseDateToISO(get(metaCols.startIdx));
  const end = parseDateToISO(get(metaCols.endIdx));
  const sample = safeInt(get(metaCols.sampleIdx));
  const pop = get(metaCols.populationIdx);
  const url = get(metaCols.urlIdx);
  const race = get(metaCols.raceIdx);
  const state = get(metaCols.stateIdx);
  const district = get(metaCols.districtIdx);

  const answers = [];
  for (const a of answerCols) {
    const val = safeNum(get(a.idx));
    if (val === null) continue;
    answers.push({ choice: a.name, pct: val });
  }

  if (!pollster && answers.length === 0) return null;

  const out = {
    start_date: start,
    end_date: end,
    pollster: pollster ?? null,
    sample_size: sample,
    population: pop ?? null,
    sponsors: sponsor ?? null,
    url: looksLikeURL(url) ? url : null,
    answers
  };

  if (race) out.race = String(race).trim();
  if (state) out.state = String(state).trim();
  if (district) out.district = String(district).trim();

  return out;
}

function extractPollBuckets(ds) {
  const L = buildColLookup(ds.cols);

  const metaCols = {
    pollsterIdx: L.findIdx([/pollster|firm|polling|organization/]),
    sponsorIdx: L.findIdx([/sponsor|client|commission/]),
    startIdx: L.findIdx([/start|field start|begin|from/]),
    endIdx: L.findIdx([/end|field end|finish|to/]),
    sampleIdx: L.findIdx([/sample|respondents?|sample size|n\b/]),
    populationIdx: L.findIdx([/population|lv|rv|adults?|a\b/]),
    urlIdx: L.findIdx([/url|link|source/]),
    raceIdx: L.findIdx([/race|contest|office|seat|matchup/]),
    stateIdx: L.findIdx([/^state$/, /state\s*abbr/]),
    districtIdx: L.findIdx([/district|cd\b|sd\b|hd\b/])
  };

  const metaIdxSet = new Set(Object.values(metaCols).filter(i => i >= 0));

  // numeric columns become "answers"
  const answerCols = [];
  for (let i = 0; i < ds.cols.length; i++) {
    if (metaIdxSet.has(i)) continue;
    const name = String(ds.cols[i] ?? "").trim();
    if (!name) continue;

    let numericCount = 0, sampleN = 0;
    for (let r = 0; r < Math.min(ds.rows.length, 60); r++) {
      const v = ds.rows[r][i];
      if (v === null || v === undefined || v === "") continue;
      sampleN++;
      if (safeNum(v) !== null) numericCount++;
    }
    if (sampleN > 0 && (numericCount / sampleN) >= 0.7) {
      answerCols.push({ idx: i, name });
    }
  }

  const normalized = ds.rows
    .map(r => normalizePollRow(r, metaCols, answerCols))
    .filter(Boolean);

  const genericBallot = [];
  const approval = [];
  const races = {};

  function bucketKeyFor(p) {
    const race = String(p.race ?? "").toLowerCase();
    if (/generic/.test(race) && /(ballot|dem|gop|democrat|republican)/.test(race)) return "generic";
    if (/approval/.test(race) && /(trump|president)/.test(race)) return "approval";

    const choices = (p.answers || []).map(a => String(a.choice).toLowerCase());
    const hasApprove = choices.some(c => /approve/.test(c));
    const hasDisapprove = choices.some(c => /disapprove/.test(c));
    if (hasApprove && hasDisapprove) return "approval";

    const hasDem = choices.some(c => /\bdem\b|democrat|\bd\b/.test(c));
    const hasGop = choices.some(c => /\bgop\b|rep\b|republican|\br\b/.test(c));
    if (hasDem && hasGop) return "generic";

    return "other";
  }

  function standardize(p, kind) {
    const out = { ...p };
    out.answers = (p.answers || []).map(a => {
      const name = String(a.choice ?? "");
      if (kind === "generic") {
        if (/dem|democrat|\bd\b/i.test(name)) return { ...a, choice: "Dem" };
        if (/gop|rep|republican|\br\b/i.test(name)) return { ...a, choice: "GOP" };
      }
      if (kind === "approval") {
        if (/approve/i.test(name)) return { ...a, choice: "Approve" };
        if (/disapprove/i.test(name)) return { ...a, choice: "Disapprove" };
      }
      return a;
    });
    return out;
  }

  for (const p of normalized) {
    const key = bucketKeyFor(p);
    if (key === "generic") genericBallot.push(standardize(p, "generic"));
    else if (key === "approval") approval.push(standardize(p, "approval"));
    else {
      const label = (p.race || "Unknown race").trim() || "Unknown race";
      if (!races[label]) races[label] = [];
      races[label].push(p);
    }
  }

  return { genericBallot, approval, races };
}

/** ---- main: hunt ---- **/

async function main() {
  let playwright;
  try { playwright = require("playwright"); }
  catch {
    console.error("Playwright missing. In Actions: npm i -D playwright && npx playwright install --with-deps chromium");
    process.exit(2);
  }

  const ua = process.env.RTWH_UA ||
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";

  const browser = await playwright.chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"]
  });

  const context = await browser.newContext({
    userAgent: ua,
    viewport: { width: 1400, height: 900 },
    locale: "en-US"
  });

  const page = await context.newPage();

  const seenRequests = new Map(); // url -> {type, method}
  const responseSniff = [];       // [{url,status,ct,bytes,parsed?}]
  const datasets = [];            // parsed datasets discovered

  page.on("request", (req) => {
    const u = req.url();
    if (!seenRequests.has(u)) {
      seenRequests.set(u, { type: req.resourceType(), method: req.method() });
    }
  });

  page.on("response", async (res) => {
    try {
      const u = res.url();
      if (isExcludedUrl(u)) return;

      const ct = (res.headers()["content-type"] || "").toLowerCase();
      // only sniff non-HTML
      if (ct.includes("text/html")) return;

      // only consider candidate-ish responses
      if (!isCandidateUrl(u) && !u.toLowerCase().includes("gviz")) return;

      const body = await res.text();
      const parsed = tryParseDataset(body, ct);
      responseSniff.push({ url: u, status: res.status(), content_type: ct, bytes: body.length, parsed: !!parsed });

      if (parsed && parsed.rows?.length >= MIN_ROWS) {
        datasets.push({ url: u, content_type: ct, ...parsed });
      }
    } catch {
      // ignore per-response failures
    }
  });

  console.log("Opening", ENTRY_URL);
  await page.goto(ENTRY_URL, { waitUntil: "domcontentloaded", timeout: 90_000 });
  await page.waitForTimeout(WAIT_MS);

  const frameUrls = page.frames().map(f => f.url()).filter(u => u && u.startsWith("http"));

  const domUrls = await page.evaluate(() => {
    const out = [];
    const push = (v) => { if (v && typeof v === "string") out.push(v); };
    document.querySelectorAll("iframe[src]").forEach(n => push(n.src));
    document.querySelectorAll("script[src]").forEach(n => push(n.src));
    document.querySelectorAll("link[href]").forEach(n => push(n.href));
    document.querySelectorAll("a[href]").forEach(n => push(n.href));
    return out;
  });

  const allUrls = uniq([...seenRequests.keys(), ...frameUrls, ...domUrls]).filter(u => u.startsWith("http"));
  const candidates = allUrls.filter(isCandidateUrl);

  // Actively probe candidates (some aren’t fetched by the page due to lazy loads)
  const probeResults = [];
  for (const u of candidates.slice(0, 500)) {
    try {
      const r = await context.request.get(u, {
        headers: { "user-agent": ua, "accept": "application/json,text/csv,*/*" },
        timeout: 45_000
      });
      const ct = (r.headers()["content-type"] || "").toLowerCase();
      if (ct.includes("text/html")) continue;

      const body = await r.text();
      const parsed = tryParseDataset(body, ct);
      probeResults.push({ url: u, ok: r.ok(), status: r.status(), content_type: ct, bytes: body.length, parsed: !!parsed });

      if (parsed && parsed.rows?.length >= MIN_ROWS) {
        datasets.push({ url: u, content_type: ct, ...parsed });
      }
    } catch (e) {
      probeResults.push({ url: u, ok: false, error: String(e?.message || e) });
    }
  }

  // Deduplicate datasets by url
  const byUrl = new Map();
  for (const d of datasets) {
    const prev = byUrl.get(d.url);
    if (!prev || d.rows.length > prev.rows.length) byUrl.set(d.url, d);
  }
  const uniqueDatasets = Array.from(byUrl.values());

  const chosen = pickDataset(uniqueDatasets);

  const sourcesOut = {
    fetched_at: nowISO(),
    entry_url: ENTRY_URL,
    wait_ms: WAIT_MS,
    frames: frameUrls,
    candidates_count: candidates.length,
    candidates: candidates.slice(0, 200),
    response_sniff: responseSniff.slice(0, 200),
    probe_results: probeResults.slice(0, 300),
    datasets: uniqueDatasets
      .map(d => ({
        url: d.url,
        format: d.format,
        rows: d.rows.length,
        cols: d.cols.length,
        content_type: d.content_type,
        score: scoreDataset(d)
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 30),
    chosen: chosen ? {
      url: chosen.url,
      format: chosen.format,
      rows: chosen.rows.length,
      cols: chosen.cols.length,
      content_type: chosen.content_type,
      score: chosen._score
    } : null
  };

  fs.writeFileSync(OUT_SOURCES, JSON.stringify(sourcesOut, null, 2));

  if (!chosen) {
    console.error("No plausible sheet/json dataset found. See rtwh_sources.json -> candidates/datasets/probe_results.");
    await browser.close();
    process.exit(3);
  }

  const { genericBallot, approval, races } = extractPollBuckets(chosen);

  const out = {
    meta: {
      fetched_at: nowISO(),
      source: "racetothewh",
      entry_url: ENTRY_URL,
      dataset_url: chosen.url,
      dataset_format: chosen.format
    },
    genericBallot,
    approval,
    races
  };

  fs.writeFileSync(OUT_POLLS, JSON.stringify(out, null, 2));
  console.log(`Wrote polls.json (genericBallot=${genericBallot.length}, approval=${approval.length}, races=${Object.keys(races).length})`);
  console.log(`Wrote rtwh_sources.json (chosen=${chosen.url})`);

  await browser.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
