"use strict";

/**
 * RTWH poll ingestion → polls.json
 *
 * Outputs (always):
 *  - rtwh_status.json  { ok: boolean, fetched_at, reason?, dataset_url?, dataset_format? }
 *  - rtwh_sources.json debug (candidates, chosen, sample keys/row, numeric cols, probe logs)
 *  - rtwh_debug/*      HTML + PNG snapshots for seed pages
 *
 * Output (only when ok=true):
 *  - polls.json        { meta, genericBallot, approval, races }
 *
 * This script is designed to "hunt the sheet/json" behind RTWH's client-rendered pages.
 * It uses Playwright to load pages, captures network responses, extracts embedded URLs
 * (including Google Sheets), derives CSV/GVIZ endpoints, probes them, and then normalizes
 * into your existing poll.html-compatible schema.
 */

const fs = require("fs");
const path = require("path");

const OUT_POLLS = path.join(__dirname, "polls.json");
const OUT_STATUS = path.join(__dirname, "rtwh_status.json");
const OUT_SOURCES = path.join(__dirname, "rtwh_sources.json");
const OUT_DEBUG_DIR = path.join(__dirname, "rtwh_debug");

const ENTRY_PAGES = [
  "https://www.racetothewh.com/allpolls",
  "https://www.racetothewh.com/trump",
  "https://www.racetothewh.com/polls/genericballot",
  "https://www.racetothewh.com/senate/26polls",
  "https://www.racetothewh.com/governor/26polls",
  "https://www.racetothewh.com/house/26polls",
  "https://www.racetothewh.com/president/2028/polls",
  "https://www.racetothewh.com/president/2028/dem",
  "https://www.racetothewh.com/president/2028/gop",
];

const USER_AGENT =
  process.env.RTWH_UA ||
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const WAIT_MS = Number(process.env.RTWH_WAIT_MS || "14000");
const PAGE_TIMEOUT_MS = Number(process.env.RTWH_PAGE_TIMEOUT_MS || "90000");

const MIN_ROWS = Number(process.env.RTWH_MIN_ROWS || "30");         // dataset rows min
const MIN_SCORE = Number(process.env.RTWH_MIN_SCORE || "14");       // dataset score min
const MIN_BODY_CHARS = Number(process.env.RTWH_MIN_BODY_CHARS || "12000");
const MAX_BODY_CHARS = Number(process.env.RTWH_MAX_BODY_CHARS || "2500000");

const MAX_PROBES = Number(process.env.RTWH_MAX_PROBES || "700");
const MAX_CAPTURED_RESPONSES = Number(process.env.RTWH_MAX_CAPTURED || "900");

const BLOCK_URL_RE =
  /(googletagmanager|google-analytics|doubleclick|adsystem|adservice|amazon-adsystem|facebook|connect\.facebook|hotjar|segment\.|datadoghq|sentry|cloudflareinsights)/i;

function nowISO() {
  return new Date().toISOString();
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeJSON(fp, obj) {
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2));
}

function writeStatus(ok, extra = {}) {
  writeJSON(OUT_STATUS, { ok: !!ok, fetched_at: nowISO(), ...extra });
}

function normKey(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^\w_]/g, "");
}

function uniq(arr) {
  return Array.from(new Set(arr));
}

function looksLikeURL(s) {
  return /^https?:\/\//i.test(String(s ?? "").trim());
}

function firstNumber(x) {
  if (x === null || x === undefined) return null;
  const s = String(x)
    .trim()
    .replace(/\u2212/g, "-") // unicode minus
    .replace(/,/g, "");     // thousands sep
  if (!s) return null;

  const m = s.match(/-?\d+(\.\d+)?/);
  if (!m) return null;

  const n = Number(m[0]);
  return Number.isFinite(n) ? n : null;
}

function isNumeric(x) {
  return firstNumber(x) !== null;
}

function toNum(x) {
  return firstNumber(x);
}

function toISODateMaybe(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    const mm = String(m[1]).padStart(2, "0");
    const dd = String(m[2]).padStart(2, "0");
    return `${m[3]}-${mm}-${dd}`;
  }

  const d = new Date(s);
  if (!Number.isNaN(d.valueOf())) {
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }

  return null;
}

function safeInt(v) {
  const n = firstNumber(v);
  if (n === null) return null;
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function isExcludedUrl(u) {
  const s = String(u || "").toLowerCase();
  if (!s.startsWith("http")) return true;
  if (BLOCK_URL_RE.test(s)) return true;
  return false;
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

  // occasionally RTWH uses opaque endpoints; keep mild signal
  if (s.includes("poll") && (s.includes("data") || s.includes("api"))) return true;

  return false;
}

function extractUrlsFromText(t) {
  if (!t) return [];
  const urls = new Set();
  const re = /(https?:\/\/[^\s"'<>]+)|((?:https?:)?\/\/[^\s"'<>]+)/g;

  for (const m of String(t).matchAll(re)) {
    const u = (m[1] || m[2] || "").trim();
    if (!u) continue;
    const fixed = u.startsWith("//") ? "https:" + u : u;
    urls.add(fixed.replace(/[)\].,]+$/, ""));
  }

  // Also catch raw spreadsheet IDs
  const sheetRe = /spreadsheets\/d\/([a-zA-Z0-9-_]+)/g;
  for (const m of String(t).matchAll(sheetRe)) {
    const id = m[1];
    urls.add(`https://docs.google.com/spreadsheets/d/${id}/export?format=csv`);
    urls.add(`https://docs.google.com/spreadsheets/d/${id}/gviz/tq?tqx=out:json`);
  }

  return Array.from(urls);
}

function deriveGoogleSheetsExports(u) {
  const out = [];
  try {
    const url = new URL(u);
    const host = url.hostname.toLowerCase();
    if (!host.includes("docs.google.com")) return [];

    const gid = url.searchParams.get("gid") || "0";
    const p = url.pathname;

    // Published: /spreadsheets/d/e/<pubid>/pubhtml...
    const mPub = p.match(/\/spreadsheets\/d\/e\/([^/]+)\//);
    if (mPub) {
      const pubid = mPub[1];
      out.push(`https://docs.google.com/spreadsheets/d/e/${pubid}/pub?output=csv&gid=${gid}`);
      out.push(`https://docs.google.com/spreadsheets/d/e/${pubid}/pub?output=tsv&gid=${gid}`);
      return uniq(out);
    }

    // Normal: /spreadsheets/d/<id>/...
    const m = p.match(/\/spreadsheets\/d\/([^/]+)/);
    if (m) {
      const id = m[1];
      out.push(`https://docs.google.com/spreadsheets/d/${id}/export?format=csv&gid=${gid}`);
      out.push(`https://docs.google.com/spreadsheets/d/${id}/gviz/tq?tqx=out:json&gid=${gid}`);
      return uniq(out);
    }
  } catch {
    return out;
  }
  return uniq(out);
}

function isProbablyJavascript(text) {
  const t = String(text || "").slice(0, 4000);
  return (
    /(^|\n)\s*(function|var|let|const)\s+/i.test(t) ||
    /window\.|document\.|dataLayer|gtag\(/i.test(t) ||
    /sourceMappingURL=/i.test(t)
  );
}

// Simple RFC4180-ish CSV parser
function parseCSV(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  function endField() {
    row.push(field);
    field = "";
  }
  function endRow() {
    endField();
    rows.push(row);
    row = [];
  }

  const s = String(text || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (inQuotes) {
      if (c === '"') {
        if (s[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += c;
      }
    } else {
      if (c === '"') {
        inQuotes = true;
      } else if (c === ",") {
        endField();
      } else if (c === "\n") {
        endRow();
      } else {
        field += c;
      }
    }
  }
  endRow();

  const header = (rows[0] || []).map((h) => String(h || "").trim());
  const records = rows
    .slice(1)
    .filter((r) => r.some((x) => String(x || "").trim() !== ""))
    .map((r) => {
      const o = {};
      for (let j = 0; j < header.length; j++) o[header[j]] = (r[j] ?? "").trim();
      return o;
    });

  return { header, records };
}

function parseGviz(text) {
  const m = String(text || "").match(/setResponse\(([\s\S]+)\)\s*;?\s*$/);
  if (!m) return null;
  try {
    return JSON.parse(m[1]);
  } catch {
    return null;
  }
}

function tryParseDataset(body, contentType = "") {
  const ct = String(contentType || "").toLowerCase();
  const b = String(body || "");
  if (!b || b.length < MIN_BODY_CHARS) return null;

  if (isProbablyJavascript(b)) return null;

  // GVIZ wrapper (often content-type text/javascript)
  if (b.includes("google.visualization.Query.setResponse")) {
    const g = parseGviz(b);
    if (g && g.table && Array.isArray(g.table.cols) && Array.isArray(g.table.rows)) {
      const cols = g.table.cols.map((c, i) => c?.label || c?.id || `col_${i}`);
      const rows = g.table.rows.map((r) => (r.c || []).map((cell) => (cell ? (cell.f ?? cell.v ?? null) : null)));
      return { format: "gviz", cols, rows };
    }
  }

  // JSON
  const trimmed = b.trim();
  if (ct.includes("application/json") || trimmed.startsWith("{") || trimmed.startsWith("[")) {
    try {
      const j = JSON.parse(trimmed);
      if (Array.isArray(j) && j.length && typeof j[0] === "object" && j[0] !== null && !Array.isArray(j[0])) {
        const cols = uniq(j.flatMap((o) => Object.keys(o)));
        const rows = j.map((o) => cols.map((c) => o[c]));
        return { format: "json_objects", cols, rows };
      }
      // common table-ish shapes
      if (j && Array.isArray(j.cols) && Array.isArray(j.rows)) return { format: "json_table", cols: j.cols, rows: j.rows };
      if (j && Array.isArray(j.data) && typeof j.data[0] === "object") {
        const cols = uniq(j.data.flatMap((o) => Object.keys(o)));
        const rows = j.data.map((o) => cols.map((c) => o[c]));
        return { format: "json_data", cols, rows };
      }
    } catch {
      // ignore
    }
  }

  // CSV
  if (ct.includes("text/csv") || ct.includes("application/csv") || (trimmed.includes(",") && trimmed.includes("\n"))) {
    const parsed = parseCSV(b);
    if (parsed.header.length >= 4 && parsed.records.length >= 5) {
      // reject “CSV-like JS” (gtag etc.)
      const headerJoin = parsed.header.join(" ").toLowerCase();
      if (/gtag|datalayer|analytics|tag manager/.test(headerJoin)) return null;

      const cols = parsed.header;
      const rows = parsed.records.map((o) => cols.map((c) => o[c]));
      return { format: "csv", cols, rows };
    }
  }

  return null;
}

function scoreDataset(ds, url = "") {
  if (!ds?.cols?.length || !ds?.rows?.length) return -1;

  const colsN = ds.cols.map((c) => normKey(c));
  const joined = colsN.join(" ");
  if (/gtag|datalayer|analytics|tag_manager/.test(joined)) return -10;

  const signals = [
    /pollster|firm|polling|organization/,
    /start|field|begin|from|end|finish|to/,
    /sample|respond|sample_size|n\b/,
    /race|contest|office|seat|matchup|state|district/,
    /approve|disapprove|dem|rep|gop|margin|spread|trump|biden|harris/
  ];

  let hit = 0;
  for (const re of signals) if (colsN.some((c) => re.test(c))) hit++;

  let score = 0;
  score += hit * 6;
  score += Math.min(ds.rows.length, 600) / 12;
  score += Math.min(ds.cols.length, 100) / 10;

  if (/docs\.google\.com\/spreadsheets/.test(url)) score += 10;
  if (/gviz\/tq/.test(url)) score += 8;
  if (/export\?format=csv/.test(url) || /output=csv/.test(url)) score += 8;

  if (ds.rows.length < MIN_ROWS) score -= 20;
  return score;
}

function countNumericResultCols(rowObjects) {
  if (!rowObjects || !rowObjects.length) return { numericCols: [], count: 0 };

  const META_KEYS = new Set([
    "pollster","pollster_name",
    "start_date","end_date","start","end","date","released","release_date",
    "sample_size","sample","samplesize","n",
    "url","link","source","poll",
    "race","contest","office","cycle",
    "notes","population","pop","method","mode",
    "margin","spread","moe","error","net","lead"
  ]);

  const keys = Object.keys(rowObjects[0] || {});
  const numericCols = [];

  const N = Math.min(rowObjects.length, 70);
  for (const k of keys) {
    const nk = normKey(k);
    if (META_KEYS.has(nk)) continue;
    if (/(margin|spread|moe|error|net|lead)/i.test(k)) continue;

    let seen = 0, numeric = 0;
    for (let i = 0; i < N; i++) {
      const v = rowObjects[i]?.[k];
      if (v === null || v === undefined || String(v).trim() === "") continue;
      seen++;
      if (isNumeric(v)) numeric++;
    }
    if (seen >= 10 && numeric / seen >= 0.65) numericCols.push(k);
  }

  return { numericCols, count: numericCols.length };
}

function rowsToObjects(ds) {
  const cols = ds.cols;
  const out = ds.rows.map((r) => {
    const o = {};
    for (let i = 0; i < cols.length; i++) o[cols[i]] = r[i];
    return o;
  });
  return out;
}

function pickFirst(o, keys) {
  if (!o) return null;
  for (const k of keys) {
    if (o[k] !== undefined && String(o[k]).trim() !== "") return o[k];
    const hit = Object.keys(o).find((x) => normKey(x) === normKey(k));
    if (hit && String(o[hit]).trim() !== "") return o[hit];
  }
  return null;
}

function normalizeRowToPoll(row, raceHint = null) {
  const pollster =
    pickFirst(row, ["pollster", "Pollster", "Pollster(s)", "pollster_name", "firm", "organization"]) ?? null;

  const start_date =
    toISODateMaybe(pickFirst(row, ["start_date", "Start Date", "start", "field_start", "date_start"])) ||
    toISODateMaybe(pickFirst(row, ["start", "Start"])) ||
    null;

  const end_date =
    toISODateMaybe(pickFirst(row, ["end_date", "End Date", "end", "field_end", "date_end"])) ||
    toISODateMaybe(pickFirst(row, ["end", "End"])) ||
    start_date ||
    null;

  const sampleRaw = pickFirst(row, ["sample_size", "Sample", "Sample Size", "n", "N", "sample"]);
  const sample_size = sampleRaw !== null ? safeInt(sampleRaw) : null;

  const url = pickFirst(row, ["url", "link", "source", "poll_url"]) ?? null;

  const race =
    pickFirst(row, ["race", "Race", "contest", "Contest", "office", "Office", "seat", "Seat", "matchup"]) ||
    raceHint ||
    null;

  const META = new Set([
    "pollster", "pollster_name", "firm", "organization",
    "start_date", "end_date", "start", "end", "date", "released", "release_date",
    "sample_size", "sample", "samplesize", "n",
    "url", "link", "source", "poll", "poll_url",
    "race", "contest", "office", "cycle", "seat", "matchup",
    "notes", "population", "pop", "method", "mode",
    "margin", "spread", "moe", "error", "net", "lead"
  ]);

  const answers = [];
  for (const [k, v] of Object.entries(row)) {
    const nk = normKey(k);
    if (META.has(nk)) continue;

    const kl = String(k).toLowerCase();
    if (/(margin|spread|moe|error|net|lead)/i.test(kl)) continue;

    const pct = toNum(v);
    if (pct === null) continue;

    // normalize common labels
    if (kl.includes("approve") && !kl.includes("dis")) answers.push({ choice: "Approve", pct });
    else if (kl.includes("disapprove") || (kl.includes("dis") && kl.includes("approve"))) answers.push({ choice: "Disapprove", pct });
    else if (nk === "dem" || kl.includes("democrat") || nk === "dems") answers.push({ choice: "Dem", pct });
    else if (nk === "gop" || nk === "rep" || kl.includes("republican") || nk === "reps") answers.push({ choice: "Rep", pct });
    else answers.push({ choice: String(k).trim(), pct });
  }

  if (!answers.length) return null;

  // stable ordering (approval first; then Dem/Rep)
  const order = (a) => {
    const c = a.choice.toLowerCase();
    if (c === "approve") return 0;
    if (c === "disapprove") return 1;
    if (c === "dem") return 2;
    if (c === "rep" || c === "gop") return 3;
    return 10;
  };
  answers.sort((a, b) => order(a) - order(b));

  return {
    pollster,
    start_date,
    end_date,
    sample_size,
    url: looksLikeURL(url) ? url : null,
    race,
    answers
  };
}

function classifyPoll(p) {
  const choices = (p.answers || []).map((a) => String(a.choice).toLowerCase());
  const hasApprove = choices.includes("approve");
  const hasDis = choices.includes("disapprove");
  const hasDem = choices.includes("dem");
  const hasRep = choices.includes("rep") || choices.includes("gop");

  if (hasApprove && hasDis && !hasDem && !hasRep) return "approval";
  if (hasDem && hasRep && !hasApprove && !hasDis) return "genericBallot";
  return "race";
}

function dedup(list) {
  const seen = new Set();
  const out = [];
  for (const p of list) {
    const k = `${p.pollster || ""}|${p.end_date || ""}|${p.race || ""}|${(p.answers || []).map(a => a.choice).join(",")}`.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(p);
  }
  return out;
}

async function scrapeTablesFromSheetsFrames(page) {
  const frames = page.frames().filter((f) => {
    const u = f.url();
    return u && u.startsWith("http") && !isExcludedUrl(u) && u.toLowerCase().includes("docs.google.com/spreadsheets");
  });

  const datasets = [];
  for (const frame of frames) {
    try {
      const table = await frame.evaluate((minRows) => {
        const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
        const tables = Array.from(document.querySelectorAll("table"));
        let best = null;

        for (const t of tables) {
          const trs = Array.from(t.querySelectorAll("tr"));
          if (trs.length < minRows) continue;

          const rows = trs.map((tr) =>
            Array.from(tr.querySelectorAll("th,td")).map((td) => norm(td.innerText))
          );

          const lens = rows.map((r) => r.length).filter((n) => n > 0);
          if (!lens.length) continue;

          const maxCols = Math.max(...lens);
          if (maxCols < 5) continue;

          if (!best || rows.length > best.rows.length || (rows.length === best.rows.length && maxCols > best.maxCols)) {
            best = { rows, maxCols };
          }
        }

        if (!best) return null;

        // pick header if it has letters; otherwise synthesize
        let cols = best.rows[0];
        let data = best.rows.slice(1);

        const headerAlpha = cols.some((c) => /[A-Za-z]/.test(c));
        if (!headerAlpha) {
          cols = Array.from({ length: best.maxCols }, (_, i) => `col_${i + 1}`);
          data = best.rows;
        } else {
          data = data.map((r) => {
            const rr = r.slice();
            while (rr.length < cols.length) rr.push("");
            return rr;
          });
        }

        data = data.filter((r) => r.some((x) => String(x || "").trim().length > 0));
        return { cols, rows: data };
      }, MIN_ROWS);

      if (table && table.rows && table.rows.length >= MIN_ROWS) {
        datasets.push({
          kind: "frame_table",
          url: frame.url(),
          content_type: "text/html(frame)",
          format: "frame_table",
          cols: table.cols,
          rows: table.rows
        });
      }
    } catch {
      // ignore frame access failures
    }
  }
  return datasets;
}

async function main() {
  ensureDir(OUT_DEBUG_DIR);

  const startedAt = nowISO();
  writeStatus(false, { reason: "starting" });

  let playwright;
  try {
    playwright = require("playwright");
  } catch {
    writeStatus(false, { reason: "missing playwright dependency" });
    process.exit(2);
  }

  const browser = await playwright.chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
  });

  const context = await browser.newContext({
    userAgent: USER_AGENT,
    viewport: { width: 1400, height: 900 },
    locale: "en-US",
    timezoneId: "America/Chicago"
  });

  await context.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
  });

  // Route: block images/fonts/media + obvious tracking to speed, but keep scripts/xhr
  await context.route("**/*", (route) => {
    const req = route.request();
    const url = req.url();
    const rt = req.resourceType();
    if (BLOCK_URL_RE.test(url)) return route.abort();
    if (rt === "image" || rt === "font" || rt === "media") return route.abort();
    return route.continue();
  });

  const page = await context.newPage();

  const discoveredUrls = new Set();
  const networkSniff = [];         // response logs
  const candidateDatasets = [];    // parsed datasets found directly
  let capturedCount = 0;

  page.on("response", async (res) => {
    if (capturedCount >= MAX_CAPTURED_RESPONSES) return;
    const url = res.url();
    if (isExcludedUrl(url)) return;

    const req = res.request();
    const rt = req.resourceType();
    const status = res.status();
    const ct = (res.headers()["content-type"] || "").toLowerCase();

    // log basic
    networkSniff.push({ url, status, content_type: ct, resource_type: rt });
    capturedCount++;

    // Only consider non-HTML payloads for direct parsing
    if (ct.includes("text/html")) return;

    // Only read body for likely data-ish resources
    if (!(rt === "xhr" || rt === "fetch" || url.toLowerCase().includes("/gviz/tq") || ct.includes("json") || ct.includes("csv"))) return;

    let body = "";
    try {
      body = await res.text();
    } catch {
      return;
    }
    if (!body || body.length < MIN_BODY_CHARS) return;

    const snippet = body.slice(0, Math.min(body.length, MAX_BODY_CHARS));

    for (const u of extractUrlsFromText(snippet)) discoveredUrls.add(u);

    if (!isCandidateUrl(url) && !snippet.includes("google.visualization.Query.setResponse")) return;

    const ds = tryParseDataset(snippet, ct);
    if (!ds || !ds.rows || ds.rows.length < MIN_ROWS) return;

    // require numeric result columns (avoid picking config-like JSON)
    const rowObjs = rowsToObjects(ds);
    const { count } = countNumericResultCols(rowObjs);
    if (count < 2) return;

    candidateDatasets.push({
      kind: "network_parsed",
      url,
      content_type: ct,
      format: ds.format,
      cols: ds.cols,
      rows: ds.rows,
      score: scoreDataset(ds, url),
      numeric_cols_count: count
    });
  });

  async function snapshot(tag) {
    const safe = tag.replace(/^https?:\/\//, "").replace(/[^\w]+/g, "_").slice(0, 90);
    try {
      const html = await page.content();
      fs.writeFileSync(path.join(OUT_DEBUG_DIR, `${safe}.html`), html, "utf8");
      await page.screenshot({ path: path.join(OUT_DEBUG_DIR, `${safe}.png`), fullPage: true });
      for (const u of extractUrlsFromText(html)) discoveredUrls.add(u);

      const domUrls = await page.evaluate(() => {
        const out = [];
        const push = (v) => { if (v && typeof v === "string") out.push(v); };
        document.querySelectorAll("iframe[src]").forEach(n => push(n.src));
        document.querySelectorAll("script[src]").forEach(n => push(n.src));
        document.querySelectorAll("link[href]").forEach(n => push(n.href));
        document.querySelectorAll("a[href]").forEach(n => push(n.href));
        return out;
      });
      for (const u of domUrls) discoveredUrls.add(u);
      for (const u of domUrls) {
        if (u.toLowerCase().includes("docs.google.com/spreadsheets")) {
          for (const du of deriveGoogleSheetsExports(u)) discoveredUrls.add(du);
        }
      }

      const frameUrls = page.frames().map(f => f.url()).filter(u => u && u.startsWith("http"));
      for (const fu of frameUrls) {
        discoveredUrls.add(fu);
        if (fu.toLowerCase().includes("docs.google.com/spreadsheets")) {
          for (const du of deriveGoogleSheetsExports(fu)) discoveredUrls.add(du);
        }
      }
    } catch {
      // ignore snapshot failures
    }
  }

  async function visit(u) {
    try {
      console.log("Opening", u);
      await page.goto(u, { waitUntil: "domcontentloaded", timeout: PAGE_TIMEOUT_MS });

      // encourage lazy loads
      for (let i = 0; i < 2; i++) {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await page.waitForTimeout(1200);
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.waitForTimeout(700);
      }

      await page.waitForTimeout(WAIT_MS);
      await snapshot(u);
    } catch {
      // still snapshot whatever loaded
      await snapshot(u + "_failed");
    }
  }

  // Visit seed pages
  for (const u of ENTRY_PAGES) await visit(u);

  // Scrape tables from Sheets iframes (pubhtml case)
  const frameTableDatasets = await scrapeTablesFromSheetsFrames(page);
  for (const ds of frameTableDatasets) {
    const rowObjs = rowsToObjects(ds);
    const { count } = countNumericResultCols(rowObjs);
    if (count >= 2 && rowObjs.length >= MIN_ROWS) {
      candidateDatasets.push({
        ...ds,
        score: scoreDataset(ds, ds.url),
        numeric_cols_count: count
      });
    }
  }

  // Probe discovered URLs (sheet exports / gviz / csv/json)
  const discoveredList = uniq(Array.from(discoveredUrls))
    .filter((u) => u && u.startsWith("http"))
    .filter((u) => !isExcludedUrl(u));

  // derive exports from any discovered sheet links
  for (const u of discoveredList) {
    if (u.toLowerCase().includes("docs.google.com/spreadsheets")) {
      for (const du of deriveGoogleSheetsExports(u)) discoveredUrls.add(du);
    }
  }

  const probeQueue = uniq(Array.from(discoveredUrls))
    .filter((u) => u && u.startsWith("http"))
    .filter((u) => !isExcludedUrl(u))
    .filter((u) => isCandidateUrl(u) || u.toLowerCase().includes("/gviz/tq"))
    .slice(0, MAX_PROBES);

  const probeResults = [];

  // Prefer Sheets/GVIZ/CSV
  probeQueue.sort((a, b) => {
    const pa =
      a.includes("docs.google.com/spreadsheets") ? 0 :
      a.includes("/gviz/tq") ? 1 :
      a.includes("format=csv") || a.includes("output=csv") ? 2 :
      a.endsWith(".json") ? 3 :
      9;
    const pb =
      b.includes("docs.google.com/spreadsheets") ? 0 :
      b.includes("/gviz/tq") ? 1 :
      b.includes("format=csv") || b.includes("output=csv") ? 2 :
      b.endsWith(".json") ? 3 :
      9;
    return pa - pb;
  });

  for (const u of probeQueue) {
    try {
      const r = await context.request.get(u, {
        headers: { "user-agent": USER_AGENT, "accept": "application/json,text/csv,*/*" },
        timeout: 45000
      });

      const ct = (r.headers()["content-type"] || "").toLowerCase();
      const status = r.status();
      if (!r.ok()) {
        probeResults.push({ url: u, ok: false, status, content_type: ct });
        continue;
      }
      if (ct.includes("text/html")) {
        // do not treat HTML as dataset here (frames are handled separately)
        probeResults.push({ url: u, ok: true, status, content_type: ct, note: "html ignored" });
        continue;
      }

      const body = await r.text();
      probeResults.push({ url: u, ok: true, status, content_type: ct, bytes: body.length });

      if (!body || body.length < MIN_BODY_CHARS) continue;
      if (isProbablyJavascript(body)) continue;

      const snippet = body.slice(0, Math.min(body.length, MAX_BODY_CHARS));
      const ds = tryParseDataset(snippet, ct);
      if (!ds || !ds.rows || ds.rows.length < MIN_ROWS) continue;

      const rowObjs = rowsToObjects(ds);
      const { count } = countNumericResultCols(rowObjs);
      if (count < 2) continue;

      candidateDatasets.push({
        kind: "probe_parsed",
        url: u,
        content_type: ct,
        format: ds.format,
        cols: ds.cols,
        rows: ds.rows,
        score: scoreDataset(ds, u),
        numeric_cols_count: count
      });
    } catch (e) {
      probeResults.push({ url: u, ok: false, error: String(e?.message || e) });
    }
  }

  // Choose best dataset candidate
  const scored = candidateDatasets
    .map((d) => {
      const base = d.score ?? scoreDataset(d, d.url);
      const bonus = (d.numeric_cols_count || 0) * 20 + Math.min(d.rows?.length || 0, 800) / 6;
      return { ...d, final_score: base + bonus };
    })
    .sort((a, b) => b.final_score - a.final_score);

  const chosen = scored[0] || null;

  // Write sources debug now (even if we fail)
  const chosenRowObjects = chosen ? rowsToObjects(chosen) : [];
  const chosenNumeric = chosen ? countNumericResultCols(chosenRowObjects) : { numericCols: [], count: 0 };

  writeJSON(OUT_SOURCES, {
    fetched_at: startedAt,
    entry_pages: ENTRY_PAGES,
    wait_ms: WAIT_MS,
    captured_responses: Math.min(networkSniff.length, 5000),
    network_sniff_sample: networkSniff.slice(0, 200),
    discovered_url_count: discoveredUrls.size,
    probe_results_sample: probeResults.slice(0, 250),
    candidates_top20: scored.slice(0, 20).map((d) => ({
      kind: d.kind,
      url: d.url,
      format: d.format,
      rows: d.rows?.length || 0,
      cols: d.cols?.length || 0,
      numeric_cols_count: d.numeric_cols_count || 0,
      score: d.score,
      final_score: d.final_score,
      content_type: d.content_type
    })),
    chosen_debug: chosen ? {
      kind: chosen.kind,
      url: chosen.url,
      format: chosen.format,
      content_type: chosen.content_type,
      rows: chosen.rows?.length || 0,
      cols: chosen.cols?.length || 0,
      numeric_result_cols: chosenNumeric.numericCols.slice(0, 40),
      sample_keys: Object.keys(chosenRowObjects[0] || {}).slice(0, 80),
      sample_row: chosenRowObjects[0] || null
    } : null
  });

  if (!chosen || chosen.final_score < MIN_SCORE) {
    writeStatus(false, { reason: "no plausible dataset candidate", note: "see rtwh_sources.json and rtwh_debug/" });
    await browser.close();
    process.exit(3);
  }

  // Normalize into polls.json
  const rowObjects = rowsToObjects(chosen);
  const { count: numericColsCount } = countNumericResultCols(rowObjects);
  if (numericColsCount < 2) {
    writeStatus(false, { reason: "chosen dataset has insufficient numeric result columns", dataset_url: chosen.url, dataset_format: chosen.format });
    await browser.close();
    process.exit(3);
  }

  const out = {
    meta: {
      fetched_at: startedAt,
      source: "racetothewh",
      entry_url: ENTRY_PAGES[0],
      dataset_url: chosen.url,
      dataset_format: chosen.format
    },
    genericBallot: [],
    approval: [],
    races: {}
  };

  let kept = 0;
  let dropped = 0;

  for (const r of rowObjects) {
    const poll = normalizeRowToPoll(r, null);
    if (!poll || !poll.answers || poll.answers.length === 0) {
      dropped++;
      continue;
    }
    const bucket = classifyPoll(poll);
    if (bucket === "approval") out.approval.push(poll);
    else if (bucket === "genericBallot") out.genericBallot.push(poll);
    else {
      const key = String(poll.race || "Other").trim() || "Other";
      (out.races[key] ||= []).push(poll);
    }
    kept++;
  }

  out.approval = dedup(out.approval);
  out.genericBallot = dedup(out.genericBallot);
  for (const k of Object.keys(out.races)) out.races[k] = dedup(out.races[k]);

  const hasAny =
    (out.genericBallot?.length || 0) > 0 ||
    (out.approval?.length || 0) > 0 ||
    Object.keys(out.races || {}).length > 0;

  if (!hasAny) {
    // Write an empty polls.json so the workflow can fail deterministically,
    // but keep all debug artifacts committed.
    writeJSON(OUT_POLLS, out);
    writeStatus(false, {
      reason: "dataset parsed but produced 0 polls after normalization",
      dataset_url: chosen.url,
      dataset_format: chosen.format,
      kept_rows: kept,
      dropped_rows: dropped
    });
    await browser.close();
    process.exit(4);
  }

  writeJSON(OUT_POLLS, out);
  writeStatus(true, {
    dataset_url: chosen.url,
    dataset_format: chosen.format,
    genericBallot: out.genericBallot.length,
    approval: out.approval.length,
    races: Object.keys(out.races).length,
    kept_rows: kept,
    dropped_rows: dropped
  });

  console.log("OK: wrote polls.json");
  console.log("  dataset_url:", chosen.url);
  console.log("  genericBallot:", out.genericBallot.length);
  console.log("  approval:", out.approval.length);
  console.log("  races:", Object.keys(out.races).length);

  await browser.close();
}

main().catch((e) => {
  try {
    writeStatus(false, { reason: "unhandled exception", error: String(e?.stack || e) });
  } catch {}
  console.error(e);
  process.exit(1);
});
