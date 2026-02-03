#!/usr/bin/env node
/**
 * RTWH poll ingestion (hunt sheet/json) -> polls.json
 *
 * What this does:
 * - Opens RTWH /allpolls via Playwright (headless).
 * - Collects candidate URLs from: requests + DOM + frames.
 * - Special-cases Google Sheets embeds:
 *    - Scrape pubhtml table directly from iframe DOM
 *    - Derive stable CSV + GVIZ JSON endpoints from the iframe URL and probe them
 * - Scores datasets by poll-like columns; refuses to pick analytics/tracking junk.
 * - On failure: writes rtwh_sources.json + rtwh_debug.png and exits 3.
 */

const fs = require("fs");
const path = require("path");

const OUT_POLLS = path.join(__dirname, "polls.json");
const OUT_SOURCES = path.join(__dirname, "rtwh_sources.json");
const OUT_SCREEN = path.join(__dirname, "rtwh_debug.png");

const ENTRY_URL = process.env.RTWH_ENTRY_URL || "https://www.racetothewh.com/allpolls";
const WAIT_MS = Number(process.env.RTWH_WAIT_MS || "15000");
const MIN_ROWS = Number(process.env.RTWH_MIN_ROWS || "30");
const MIN_SCORE = Number(process.env.RTWH_MIN_SCORE || "14");

function nowISO() { return new Date().toISOString(); }
function uniq(a) { return Array.from(new Set(a)); }
function normKey(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^\w %/.-]/g, "");
}
function looksLikeURL(s) { return /^https?:\/\//i.test(String(s ?? "").trim()); }

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

/** ---- hard-exclude analytics/tracking ---- **/
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
    s.includes("segment.") ||
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
  if (s.includes("docs.google.com/spreadsheets")) return true;
  if (s.includes("spreadsheets.google.com")) return true;
  if (s.includes("/gviz/tq")) return true;
  if (s.includes("tqx=out:json")) return true;
  if (s.includes("output=csv")) return true;
  if (s.endsWith(".csv") || s.includes(".csv?")) return true;
  if (s.endsWith(".json") || s.includes(".json?")) return true;
  if (s.includes("airtable")) return true;
  return false;
}

/** ---- parse GVIZ/CSV/JSON tables ---- **/
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
  const t = String(text || "").slice(0, 4000);
  return (
    /(^|\n)\s*(function|var|let|const)\s+/i.test(t) ||
    /window\.|document\.|dataLayer|gtag\(/i.test(t) ||
    /sourceMappingURL=/i.test(t)
  );
}

function parseCSVStrict(text) {
  const raw = String(text || "");
  if (isProbablyJavascript(raw)) return null;

  const lines = raw.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");
  const nonEmpty = lines.filter(l => l.trim().length > 0);
  if (nonEmpty.length < 6) return null;

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

  const testRows = nonEmpty.slice(1, 6).map(splitCSVLine);
  const ok = testRows.every(r => r.length === header.length || r.length >= header.length - 1);
  if (!ok) return null;

  const rows = nonEmpty.slice(1).map(splitCSVLine);
  return { format: "csv", cols: header, rows };
}

function parseJSONTable(text) {
  const raw = String(text || "");
  if (isProbablyJavascript(raw)) return null;
  let obj;
  try { obj = JSON.parse(raw); } catch { return null; }

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

  if (b.includes("google.visualization.Query.setResponse")) {
    const g = parseGviz(b);
    if (g) return g;
  }
  if (ct.includes("application/json") || b.trim().startsWith("{") || b.trim().startsWith("[")) {
    const j = parseJSONTable(b);
    if (j) return j;
  }
  if (ct.includes("text/csv") || ct.includes("application/csv")) {
    const c = parseCSVStrict(b);
    if (c) return c;
  }
  const c2 = parseCSVStrict(b);
  if (c2) return c2;

  return null;
}

/** ---- score datasets: must look poll-ish ---- **/
function scoreDataset(ds) {
  if (!ds?.cols?.length || !ds?.rows?.length) return -1;

  const colsN = ds.cols.map(normKey);
  const joined = colsN.join(" ");

  if (/gtag|datalayer|analytics|tag manager/.test(joined)) return -10;

  const signals = [
    /pollster|firm|polling|organization/,
    /start|field|begin|from|end|finish|to/,
    /sample|respond|sample size|n\b/,
    /race|contest|office|seat|matchup|state|district/,
    /approve|disapprove|dem|rep|gop|margin|spread|trump|biden|harris/
  ];

  let hit = 0;
  for (const re of signals) if (colsN.some(c => re.test(c))) hit++;

  let score = 0;
  score += hit * 6;
  score += Math.min(ds.rows.length, 400) / 12;
  score += Math.min(ds.cols.length, 80) / 8;

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

/** ---- google sheets url -> export endpoints ---- **/
function deriveGoogleSheetsExports(u) {
  const out = [];
  try {
    const url = new URL(u);
    const host = url.hostname.toLowerCase();
    if (!host.includes("docs.google.com")) return [];

    const gid = url.searchParams.get("gid") || "0";
    const p = url.pathname;

    // Published sheet: /spreadsheets/d/e/<PUBID>/pubhtml...
    const mPub = p.match(/\/spreadsheets\/d\/e\/([^/]+)\//);
    if (mPub) {
      const pubid = mPub[1];
      // Common stable endpoints:
      out.push(`https://docs.google.com/spreadsheets/d/e/${pubid}/pub?output=csv&gid=${gid}`);
      out.push(`https://docs.google.com/spreadsheets/d/e/${pubid}/pub?output=tsv&gid=${gid}`);
      // Sometimes gid is ignored, but keep it.
      return uniq(out);
    }

    // Normal sheet: /spreadsheets/d/<ID>/...
    const m = p.match(/\/spreadsheets\/d\/([^/]+)/);
    if (m) {
      const id = m[1];
      out.push(`https://docs.google.com/spreadsheets/d/${id}/export?format=csv&gid=${gid}`);
      out.push(`https://docs.google.com/spreadsheets/d/${id}/gviz/tq?tqx=out:json&gid=${gid}`);
      return uniq(out);
    }
  } catch { /* ignore */ }

  return uniq(out);
}

/** ---- scrape HTML tables from iframes (pubhtml) ---- **/
async function scrapeTablesFromFrames(page) {
  const frames = page.frames().filter(f => {
    const u = f.url();
    return u && u.startsWith("http") && !isExcludedUrl(u);
  });

  const datasets = [];
  for (const frame of frames) {
    const u = frame.url().toLowerCase();
    const isSheets = u.includes("docs.google.com/spreadsheets");
    if (!isSheets) continue;

    try {
      const t = await frame.evaluate((minRows) => {
        const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();

        const tables = Array.from(document.querySelectorAll("table"));
        let best = null;

        for (const table of tables) {
          const trs = Array.from(table.querySelectorAll("tr"));
          if (trs.length < minRows) continue;

          const rows = trs.map(tr => Array.from(tr.querySelectorAll("th,td")).map(td => norm(td.innerText)));
          const lens = rows.map(r => r.length).filter(n => n > 0);
          if (!lens.length) continue;

          const maxCols = Math.max(...lens);
          if (maxCols < 5) continue;

          // pick the table with most rows; tie-breaker: more columns
          if (!best || rows.length > best.rows.length || (rows.length === best.rows.length && maxCols > best.maxCols)) {
            best = { rows, maxCols };
          }
        }

        if (!best) return null;

        let cols = best.rows[0];
        let data = best.rows.slice(1);

        // If header row looks empty or numeric, synthesize headers
        const headerAlpha = cols.some(c => /[A-Za-z]/.test(c));
        if (!headerAlpha) {
          cols = Array.from({ length: best.maxCols }, (_, i) => `col_${i+1}`);
          data = best.rows;
        } else {
          // Normalize row widths
          data = data.map(r => {
            const rr = r.slice();
            while (rr.length < cols.length) rr.push("");
            return rr;
          });
        }

        // Trim trailing empty rows
        data = data.filter(r => r.some(x => String(x || "").trim().length > 0));

        return { cols, rows: data };
      }, MIN_ROWS);

      if (t && t.rows && t.rows.length >= MIN_ROWS) {
        datasets.push({
          url: frame.url(),
          content_type: "text/html (frame)",
          format: "frame_table",
          cols: t.cols,
          rows: t.rows
        });
      }
    } catch {
      // ignore frame evaluation failures
    }
  }
  return datasets;
}

/** ---- normalize into your existing polls.json structure (genericBallot + approval + races) ---- **/
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
    raceIdx: L.findIdx([/race|contest|office|seat|matchup/])
  };

  const metaIdxSet = new Set(Object.values(metaCols).filter(i => i >= 0));

  // numeric columns -> answers
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

  const normalized = ds.rows.map(r => normalizePollRow(r, metaCols, answerCols)).filter(Boolean);

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
      (races[label] ||= []).push(p);
    }
  }

  return { genericBallot, approval, races };
}

/** ---- main ---- **/
async function main() {
  let playwright;
  try { playwright = require("playwright"); }
  catch {
    console.error("Playwright missing. Install: npm i -D playwright && npx playwright install --with-deps chromium");
    process.exit(2);
  }

  const ua = process.env.RTWH_UA ||
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36";

  const browser = await playwright.chromium.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled"
    ]
  });

  const context = await browser.newContext({
    userAgent: ua,
    viewport: { width: 1400, height: 900 },
    locale: "en-US"
  });

  // basic anti-bot fingerprint reduction
  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
  });

  const page = await context.newPage();

  const seenRequests = new Map();
  const sniff = [];
  const datasets = [];
  const derivedCandidates = [];

  page.on("request", (req) => {
    const u = req.url();
    if (!seenRequests.has(u)) seenRequests.set(u, { type: req.resourceType(), method: req.method() });
  });

  page.on("response", async (res) => {
    try {
      const u = res.url();
      if (isExcludedUrl(u)) return;

      const ct = (res.headers()["content-type"] || "").toLowerCase();
      if (ct.includes("text/html")) return;

      if (!isCandidateUrl(u) && !u.toLowerCase().includes("/gviz/tq")) return;

      const body = await res.text();
      const parsed = tryParseDataset(body, ct);
      sniff.push({ url: u, status: res.status(), content_type: ct, bytes: body.length, parsed: !!parsed });

      if (parsed && parsed.rows?.length >= MIN_ROWS) {
        datasets.push({ url: u, content_type: ct, ...parsed });
      }
    } catch {
      // ignore
    }
  });

  console.log("Opening", ENTRY_URL);
  await page.goto(ENTRY_URL, { waitUntil: "domcontentloaded", timeout: 90_000 });

  // trigger lazy-loads
  for (let i = 0; i < 3; i++) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(1200);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(800);
  }
  await page.waitForTimeout(WAIT_MS);

  const frameUrls = page.frames().map(f => f.url()).filter(u => u && u.startsWith("http") && !isExcludedUrl(u));
  for (const fu of frameUrls) derivedCandidates.push(...deriveGoogleSheetsExports(fu));

  const domUrls = await page.evaluate(() => {
    const out = [];
    const push = (v) => { if (v && typeof v === "string") out.push(v); };
    document.querySelectorAll("iframe[src]").forEach(n => push(n.src));
    document.querySelectorAll("script[src]").forEach(n => push(n.src));
    document.querySelectorAll("link[href]").forEach(n => push(n.href));
    document.querySelectorAll("a[href]").forEach(n => push(n.href));
    return out;
  });
  for (const du of domUrls) derivedCandidates.push(...deriveGoogleSheetsExports(du));

  // scrape tables from Sheets frames (pubhtml)
  const frameTableDatasets = await scrapeTablesFromFrames(page);
  datasets.push(...frameTableDatasets);

  const allUrls = uniq([...seenRequests.keys(), ...frameUrls, ...domUrls])
    .filter(u => u && u.startsWith("http") && !isExcludedUrl(u));

  const candidates = uniq(allUrls.filter(isCandidateUrl).concat(uniq(derivedCandidates)));

  // actively probe candidates (including derived sheet exports)
  const probeResults = [];
  for (const u of candidates.slice(0, 600)) {
    try {
      const r = await context.request.get(u, {
        headers: { "user-agent": ua, "accept": "application/json,text/csv,*/*" },
        timeout: 45_000
      });
      const ct = (r.headers()["content-type"] || "").toLowerCase();
      if (ct.includes("text/html")) continue; // we already handled HTML via frame scraping
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

  // dedupe datasets by url
  const byUrl = new Map();
  for (const d of datasets) {
    const key = d.url || `frame://${d.format}`;
    const prev = byUrl.get(key);
    if (!prev || (d.rows?.length || 0) > (prev.rows?.length || 0)) byUrl.set(key, d);
  }
  const uniqDatasets = Array.from(byUrl.values());

  const chosen = pickDataset(uniqDatasets);

  const sourcesOut = {
    fetched_at: nowISO(),
    entry_url: ENTRY_URL,
    wait_ms: WAIT_MS,
    frames: frameUrls.slice(0, 80),
    derived_candidates: uniq(derivedCandidates).slice(0, 80),
    candidates_count: candidates.length,
    candidates: candidates.slice(0, 120),
    sniff: sniff.slice(0, 120),
    probe_results: probeResults.slice(0, 200),
    datasets: uniqDatasets
      .map(d => ({
        url: d.url,
        format: d.format,
        rows: d.rows?.length || 0,
        cols: d.cols?.length || 0,
        content_type: d.content_type,
        score: scoreDataset(d)
      }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 40),
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
    try { await page.screenshot({ path: OUT_SCREEN, fullPage: true }); } catch {}
    console.error("No plausible sheet/json dataset found. See rtwh_sources.json (+ rtwh_debug.png).");
    console.error("Top frames:");
    for (const u of frameUrls.slice(0, 12)) console.error("  -", u);
    console.error("Top derived sheet exports:");
    for (const u of uniq(derivedCandidates).slice(0, 12)) console.error("  -", u);
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

  await browser.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
