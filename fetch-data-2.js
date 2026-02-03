#!/usr/bin/env node
/**
 * Race to the WH ingestion → polls.json
 *
 * Goal:
 * - Stop using VoteHub.
 * - Use https://www.racetothewh.com/allpolls as the entrypoint.
 * - "Hunt" the underlying data feed(s) (Google Sheets / JSON / CSV / Gviz, etc.)
 *   by capturing network requests from a real browser session and probing
 *   non-HTML responses.
 *
 * Outputs:
 * - polls.json          (consumed by poll.html)
 * - rtwh_sources.json   (debug: candidate URLs + dataset selection)
 *
 * Notes:
 * - This script prefers Playwright. The workflow should install Playwright + Chromium.
 * - Once rtwh_sources.json shows a stable sheet/json URL, you can delete Playwright
 *   and switch to pure fetch against that URL.
 */

const fs = require("fs");
const path = require("path");

const OUT_POLLS = path.join(__dirname, "polls.json");
const OUT_SOURCES = path.join(__dirname, "rtwh_sources.json");

const ENTRY_URL = process.env.RTWH_ENTRY_URL || "https://www.racetothewh.com/allpolls";
const WAIT_MS = Number(process.env.RTWH_WAIT_MS || "8000");

function nowISO() {
  return new Date().toISOString();
}

function normKey(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^\w %/.-]/g, "");
}

function uniq(arr) {
  return Array.from(new Set(arr));
}

function safeNum(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  const s = String(v).trim();
  if (!s) return null;
  // strip %, commas
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

  // Gviz can give JS Date strings or numbers; Sheets can give ISO or M/D/YYYY
  if (typeof v === "number" && Number.isFinite(v)) {
    // heuristics: ms since epoch
    const ms = v > 10_000_000_000 ? v : v * 1000;
    const d = new Date(ms);
    if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
    return null;
  }

  const s = String(v).trim();
  if (!s) return null;

  // Try native parse first
  const d0 = new Date(s);
  if (!isNaN(d0.getTime())) return d0.toISOString().slice(0, 10);

  // M/D/YYYY or MM/DD/YYYY
  const mdy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mdy) {
    const mm = Number(mdy[1]), dd = Number(mdy[2]), yy = Number(mdy[3]);
    const d = new Date(Date.UTC(yy, mm - 1, dd));
    return !isNaN(d.getTime()) ? d.toISOString().slice(0, 10) : null;
  }

  // YYYY-MM-DD
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (iso) return s;

  return null;
}

function looksLikeURL(s) {
  if (!s) return false;
  const t = String(s).trim();
  return /^https?:\/\//i.test(t);
}

/** --- dataset parsers --- **/

function parseGviz(text) {
  // Common prefixes: "/*O_o*/\ngoogle.visualization.Query.setResponse({...});"
  const m = text.match(/setResponse\(([\s\S]+)\)\s*;?\s*$/);
  if (!m) return null;

  let payload;
  try {
    payload = JSON.parse(m[1]);
  } catch {
    return null;
  }
  if (!payload || !payload.table || !Array.isArray(payload.table.cols) || !Array.isArray(payload.table.rows)) return null;

  const cols = payload.table.cols.map((c, i) => (c && (c.label || c.id)) ? (c.label || c.id) : `col_${i}`);
  const rows = payload.table.rows.map((r) => (r.c || []).map((cell) => {
    if (!cell) return null;
    // prefer v; fallback to f (formatted)
    return (cell.v !== undefined) ? cell.v : (cell.f !== undefined ? cell.f : null);
  }));
  return { format: "gviz", cols, rows };
}

// Minimal CSV parser with quotes
function parseCSV(text) {
  const lines = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n").filter(l => l.trim().length > 0);
  if (lines.length < 2) return null;

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

  const header = splitCSVLine(lines[0]);
  const rows = lines.slice(1).map(splitCSVLine);
  return { format: "csv", cols: header, rows };
}

function parseJSON(text) {
  let obj;
  try { obj = JSON.parse(text); } catch { return null; }

  // Array of objects
  if (Array.isArray(obj) && obj.length && typeof obj[0] === "object" && obj[0] !== null && !Array.isArray(obj[0])) {
    const cols = uniq(obj.flatMap(o => Object.keys(o)));
    const rows = obj.map(o => cols.map(c => o[c]));
    return { format: "json_objects", cols, rows };
  }

  // {cols:[...], rows:[...]} or similar
  if (obj && Array.isArray(obj.cols) && Array.isArray(obj.rows)) {
    return { format: "json_table", cols: obj.cols, rows: obj.rows };
  }

  // Unknown JSON shape; keep raw
  return { format: "json_raw", raw: obj };
}

function tryParseDataset(body, contentType) {
  const ct = (contentType || "").toLowerCase();

  if (body.includes("google.visualization.Query.setResponse")) {
    const g = parseGviz(body);
    if (g) return g;
  }

  if (ct.includes("text/csv") || ct.includes("application/csv") || (body.includes(",") && body.includes("\n") && !body.trim().startsWith("<"))) {
    const c = parseCSV(body);
    if (c) return c;
  }

  if (ct.includes("application/json") || body.trim().startsWith("{") || body.trim().startsWith("[")) {
    const j = parseJSON(body);
    if (j && j.format !== "json_raw") return j;
  }

  return null;
}

/** --- heuristics: find the "best" dataset --- **/
function scoreDataset(ds) {
  if (!ds || !ds.cols || !ds.rows) return -1;
  const colsN = ds.cols.map(normKey);

  const want = [
    /pollster|firm|polling/,
    /start|field|begin/,
    /end|finish/,
    /sample|n\b|respond/,
    /approve|disapprove|dem|rep|gop|ind|other/,
    /race|state|district|seat|office|contest/
  ];

  let score = 0;
  for (const w of want) {
    if (colsN.some(c => w.test(c))) score += 2;
  }
  score += Math.min(ds.rows.length, 200) / 10; // prefer bigger
  score += Math.min(ds.cols.length, 60) / 10;
  return score;
}

function pickDataset(datasets) {
  const scored = datasets
    .map(d => ({ ...d, _score: scoreDataset(d) }))
    .sort((a, b) => b._score - a._score);
  return scored[0] || null;
}

/** --- normalize into polls.json schema expected by poll.html --- **/

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
    },
    idxByExact(key) {
      const k = normKey(key);
      const idx = normed.indexOf(k);
      return idx === -1 ? -1 : idx;
    }
  };
}

function normalizePollRow(row, L, metaCols, answerCols) {
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

  // discard empty rows
  if (!pollster && !answers.length) return null;

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

  // keep extra metadata for future pages
  if (race) out.race = race;
  if (state) out.state = state;
  if (district) out.district = district;

  return out;
}

function extractPollBuckets(ds) {
  if (!ds || !ds.cols || !ds.rows) {
    return { genericBallot: [], approval: [], races: {} };
  }

  const L = buildColLookup(ds.cols);

  // meta columns (flex)
  const metaCols = {
    pollsterIdx: L.findIdx([/pollster|firm|polling/, /pollster\/firm/, /organization/]),
    sponsorIdx: L.findIdx([/sponsor|sponsors|client/, /commission/]),
    startIdx: L.findIdx([/start|field start|begin/, /fielding start/, /from/]),
    endIdx: L.findIdx([/end|field end|finish|to/, /fielding end/]),
    sampleIdx: L.findIdx([/sample|n\b|respondents?/, /sample size/]),
    populationIdx: L.findIdx([/population|pop\b|lv|rv|a\b/, /likely voters|registered voters|adults/]),
    urlIdx: L.findIdx([/url|link|source/]),
    raceIdx: L.findIdx([/race|contest|office|seat|matchup/]),
    stateIdx: L.findIdx([/^state$/, /state\s*abbr/, /st\b/]),
    districtIdx: L.findIdx([/district|cd\b|sd\b|hd\b/])
  };

  // Identify answer columns:
  // - anything numeric-ish that isn't a meta column
  const metaIdxSet = new Set(Object.values(metaCols).filter(i => i >= 0));
  const answerCols = [];
  for (let i = 0; i < ds.cols.length; i++) {
    if (metaIdxSet.has(i)) continue;
    const name = String(ds.cols[i] ?? "").trim();
    if (!name) continue;

    // check if this column is mostly numeric
    let numericCount = 0, sampleN = 0;
    for (let r = 0; r < Math.min(ds.rows.length, 50); r++) {
      const v = ds.rows[r][i];
      if (v === null || v === undefined || v === "") continue;
      sampleN++;
      if (safeNum(v) !== null) numericCount++;
    }
    if (sampleN > 0 && (numericCount / sampleN) >= 0.6) {
      answerCols.push({ idx: i, name });
    }
  }

  // Normalize all rows
  const normalized = ds.rows
    .map(r => normalizePollRow(r, L, metaCols, answerCols))
    .filter(Boolean);

  // Bucket: Generic ballot & Trump approval (poll.html uses these)
  const genericBallot = [];
  const approval = [];
  const races = {}; // everything else

  function bucketKeyFor(p) {
    const race = String(p.race ?? "").toLowerCase();
    if (/generic/.test(race) && /(ballot|dems|gop|democrat|republican)/.test(race)) return "generic";
    if (/approval/.test(race) && /(trump|president)/.test(race)) return "approval";
    // Some sheets may not have race labels, but answer columns could.
    const choices = (p.answers || []).map(a => String(a.choice).toLowerCase());
    const hasApprove = choices.some(c => /approve/.test(c));
    const hasDisapprove = choices.some(c => /disapprove/.test(c));
    if (hasApprove && hasDisapprove) return "approval";
    const hasDem = choices.some(c => /\bdem\b|democrat/.test(c));
    const hasGop = choices.some(c => /\bgop\b|rep\b|republican/.test(c));
    if (hasDem && hasGop) return "generic";
    return "other";
  }


  function standardize(p, kind) {
    if (!p || !Array.isArray(p.answers)) return p;
    const out = { ...p };
    out.answers = p.answers.map(a => {
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
      const raceLabel = (p.race || "Unknown race").trim();
      const rk = raceLabel || "Unknown race";
      if (!races[rk]) races[rk] = [];
      races[rk].push(p);
    }
  }

  return { genericBallot, approval, races };
}

/** --- main --- **/

async function main() {
  let playwright;
  try {
    playwright = require("playwright");
  } catch (e) {
    console.error("Playwright is required. Install it in your workflow: npm i -D playwright && npx playwright install --with-deps chromium");
    process.exit(2);
  }

  const ua =
    process.env.RTWH_UA ||
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";

  const browser = await playwright.chromium.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
      "--disable-features=IsolateOrigins,site-per-process"
    ]
  });

  const context = await browser.newContext({
    userAgent: ua,
    viewport: { width: 1400, height: 900 },
    locale: "en-US"
  });

  const page = await context.newPage();

  const captured = new Map(); // url -> {type, method}
  page.on("request", (req) => {
    const u = req.url();
    if (u.startsWith("http")) {
      if (!captured.has(u)) captured.set(u, { type: req.resourceType(), method: req.method() });
    }
  });

  console.log("Opening", ENTRY_URL);
  await page.goto(ENTRY_URL, { waitUntil: "domcontentloaded", timeout: 90_000 });
  await page.waitForTimeout(WAIT_MS);

  // Collect frame URLs too (many embeds load inside iframes).
  const frameUrls = page.frames().map((f) => f.url()).filter((u) => u && u.startsWith("http"));

  // Candidate URL filters: likely data endpoints
  const allUrls = uniq([...captured.keys(), ...frameUrls]);

  const candidates = allUrls.filter((u) => {
    const s = u.toLowerCase();
    return (
      s.includes("docs.google.com") ||
      s.includes("spreadsheets.google.com") ||
      s.includes("googleusercontent.com") ||
      s.includes("gviz") ||
      s.endsWith(".csv") || s.includes(".csv?") ||
      s.endsWith(".json") || s.includes(".json?") ||
      s.includes("airtable") ||
      s.includes("data") ||
      s.includes("poll")
    );
  });

  console.log("Captured URLs:", allUrls.length, "Candidate data URLs:", candidates.length, "Frames:", frameUrls.length);

  const datasets = [];
  const probeResults = [];

  for (const u of candidates.slice(0, 400)) { // cap to avoid rate limits
    try {
      const res = await context.request.get(u, {
        headers: {
          "user-agent": ua,
          "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        },
        timeout: 45_000
      });

      const ct = (res.headers()["content-type"] || "").toLowerCase();
      // Skip HTML; we want the underlying tabular data.
      if (ct.includes("text/html")) continue;

      const body = await res.text();
      const parsed = tryParseDataset(body, ct);
      if (parsed && parsed.cols && parsed.rows && parsed.rows.length >= 10) {
        datasets.push({ url: u, content_type: ct, ...parsed });
      }
      probeResults.push({ url: u, ok: res.ok(), status: res.status(), content_type: ct, bytes: body.length });
    } catch (e) {
      probeResults.push({ url: u, ok: false, error: String(e && e.message ? e.message : e) });
    }
  }

  const chosen = pickDataset(datasets);

  const sourcesOut = {
    fetched_at: nowISO(),
    entry_url: ENTRY_URL,
    wait_ms: WAIT_MS,
    frames: frameUrls,
    candidates,
    probe_results: probeResults,
    datasets: datasets.map(d => ({ url: d.url, format: d.format, rows: d.rows.length, cols: d.cols.length, content_type: d.content_type, score: scoreDataset(d) })),
    chosen: chosen ? { url: chosen.url, format: chosen.format, rows: chosen.rows.length, cols: chosen.cols.length, content_type: chosen.content_type, score: chosen._score } : null
  };

  fs.writeFileSync(OUT_SOURCES, JSON.stringify(sourcesOut, null, 2));

  if (!chosen) {
    console.error("No non-HTML dataset found. See rtwh_sources.json for captured URLs and probe results.");
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
  console.log(`Wrote ${path.basename(OUT_POLLS)} (genericBallot=${genericBallot.length}, approval=${approval.length}, races=${Object.keys(races).length})`);
  console.log(`Wrote ${path.basename(OUT_SOURCES)} (chosen=${chosen.url})`);

  await browser.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
