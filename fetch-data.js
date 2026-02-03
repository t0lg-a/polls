// fetch-data.js  (replace your current file with this)
#!/usr/bin/env node
/**
 * RacetotheWH poll scraper → polls.json
 *
 * Output schema (backward compatible with poll.html):
 * {
 *   updatedAt: ISO string,
 *   meta: { fetched_at, source, entry_url, dataset_url, dataset_format, notes? },
 *   genericBallot: [ { pollster, start_date, end_date, sample_size, answers:[{choice,pct}], url?, race? } ],
 *   approval:      [ { ... } ],
 *   races: { [race_key]: [poll objects...] },
 * }
 *
 * Strategy:
 *  1) Headless Playwright visits multiple RTWH polling pages.
 *  2) Collects *all* XHR/fetch responses + JS/HTML snippets, then hunts for:
 *     - direct CSV/JSON payloads
 *     - Google Sheets URLs (export CSV / gviz)
 *  3) Parses the best dataset candidate into rows, then normalizes into buckets.
 *
 * Notes:
 *  - This is intentionally heuristic; RTWH is Squarespace + client-rendered.
 *  - If no dataset is found, the script writes rtwh_sources.json + snapshots for debugging and exits non-zero.
 */

const fs = require("fs");
const path = require("path");

const OUT_JSON = "polls.json";
const OUT_SOURCES = "rtwh_sources.json";
const OUT_SNAP_DIR = "rtwh_debug";

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
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const MAX_BODY_CHARS = 2_000_000;   // per response
const MAX_CAPTURED = 600;           // total responses saved
const MIN_DATASET_CHARS = 15_000;   // ignore tiny payloads
const PAGE_TIMEOUT_MS = 60_000;

const BLOCK_URL_RE = /(googletagmanager|google-analytics|doubleclick|adsystem|facebook|instagram|twitter|fonts\.googleapis|gstatic\.com\/fonts)/i;

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function safeWrite(fp, content) {
  fs.writeFileSync(fp, content, "utf8");
}

function normKey(k) {
  return String(k || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^\w_]/g, "");
}

function isNumeric(x) {
  if (x === null || x === undefined) return false;
  const s = String(x).trim().replace(/[%+,]/g, "");
  if (!s) return false;
  return /^-?\d+(\.\d+)?$/.test(s);
}

function toNum(x) {
  if (!isNumeric(x)) return null;
  return parseFloat(String(x).trim().replace(/[%+,]/g, ""));
}

function toISODateMaybe(s) {
  if (!s) return null;
  const t = String(s).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;

  const m1 = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m1) {
    const mm = String(m1[1]).padStart(2, "0");
    const dd = String(m1[2]).padStart(2, "0");
    return `${m1[3]}-${mm}-${dd}`;
  }

  const d = new Date(t);
  if (!Number.isNaN(d.valueOf())) {
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }
  return null;
}

// RFC4180-ish CSV parser (no deps). Handles quoted fields + embedded commas/newlines.
function parseCSV(text) {
  const rows = [];
  let row = [];
  let i = 0;
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

  while (i < text.length) {
    const c = text[i];

    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        } else {
          inQuotes = false;
          i += 1;
          continue;
        }
      } else {
        field += c;
        i += 1;
        continue;
      }
    } else {
      if (c === '"') {
        inQuotes = true;
        i += 1;
        continue;
      }
      if (c === ",") {
        endField();
        i += 1;
        continue;
      }
      if (c === "\r") {
        i += 1;
        continue;
      }
      if (c === "\n") {
        endRow();
        i += 1;
        continue;
      }
      field += c;
      i += 1;
    }
  }
  endRow();

  if (!rows.length) return { header: [], records: [] };
  const header = rows[0].map((h) => String(h || "").trim());
  const records = rows.slice(1).filter((r) => r.some((x) => String(x || "").trim() !== ""));

  const out = records.map((r) => {
    const o = {};
    for (let j = 0; j < header.length; j++) o[header[j]] = r[j] ?? "";
    return o;
  });
  return { header, records: out };
}

// Google Visualization response wrapper → JSON object
function parseGviz(text) {
  const m = text.match(/setResponse\(([\s\S]+)\);\s*$/);
  if (!m) return null;
  try {
    return JSON.parse(m[1]);
  } catch {
    return null;
  }
}

function extractUrlsFromText(t) {
  if (!t) return [];
  const urls = new Set();

  const re = /(https?:\/\/[^\s"'<>]+)|((?:https?:)?\/\/[^\s"'<>]+)/g;
  for (const m of t.matchAll(re)) {
    const u = (m[1] || m[2] || "").trim();
    if (!u) continue;
    const fixed = u.startsWith("//") ? "https:" + u : u;
    urls.add(fixed.replace(/[)\].,]+$/, ""));
  }

  const sheetRe = /spreadsheets\/d\/([a-zA-Z0-9-_]+)/g;
  for (const m of t.matchAll(sheetRe)) {
    const id = m[1];
    urls.add(`https://docs.google.com/spreadsheets/d/${id}/export?format=csv`);
    urls.add(`https://docs.google.com/spreadsheets/d/${id}/gviz/tq?tqx=out:json`);
  }

  return Array.from(urls);
}

function looksLikeCSV(text) {
  if (!text) return false;
  const s = text.slice(0, 4000);
  return s.includes(",") && (s.match(/\n/g) || []).length >= 5;
}

function looksLikeJSON(text) {
  if (!text) return false;
  const s = text.trim();
  return (s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"));
}

function looksLikeGviz(text) {
  if (!text) return false;
  return /google\.visualization\.Query\.setResponse\(/.test(text);
}

function scoreDatasetCandidate(c) {
  let s = 0;
  const url = c.url || "";
  const ct = (c.content_type || "").toLowerCase();
  const n = c.body_len || 0;
  const snip = (c.body_snippet || "").toLowerCase();

  if (ct.includes("csv")) s += 60;
  if (ct.includes("json")) s += 55;
  if (looksLikeGviz(c.body_snippet)) s += 40;
  if (looksLikeCSV(c.body_snippet)) s += 30;
  if (looksLikeJSON(c.body_snippet)) s += 25;

  if (/docs\.google\.com\/spreadsheets/.test(url)) s += 35;
  if (/gviz\/tq/.test(url)) s += 20;
  if (/export\?format=csv/.test(url)) s += 20;

  const kw = ["pollster", "sample", "start", "end", "approve", "disapprove", "dem", "gop", "republican", "democrat", "senate", "governor", "house", "district", "state"];
  for (const k of kw) if (snip.includes(k)) s += 5;

  if (n >= 200_000) s += 25;
  else if (n >= 80_000) s += 15;
  else if (n >= MIN_DATASET_CHARS) s += 8;
  else s -= 40;

  if (BLOCK_URL_RE.test(url)) s -= 200;
  if (/\bgtag\/js\b/.test(url)) s -= 200;

  return s;
}

function pickFirst(o, keys) {
  for (const k of keys) {
    if (o[k] !== undefined && String(o[k]).trim() !== "") return o[k];
    const hit = Object.keys(o).find((x) => normKey(x) === normKey(k));
    if (hit && String(o[hit]).trim() !== "") return o[hit];
  }
  return null;
}

function normalizeRowToPoll(row, raceHint) {
  const pollster = pickFirst(row, ["pollster", "Pollster", "Pollster(s)", "pollster_name"]) || null;

  const start_date = toISODateMaybe(pickFirst(row, ["start_date", "Start Date", "start", "field_start", "date_start"])) ||
                     toISODateMaybe(pickFirst(row, ["start", "Start"])) ||
                     null;

  const end_date = toISODateMaybe(pickFirst(row, ["end_date", "End Date", "end", "field_end", "date_end"])) ||
                   toISODateMaybe(pickFirst(row, ["end", "End"])) ||
                   start_date ||
                   null;

  const sampleRaw = pickFirst(row, ["sample_size", "Sample", "Sample Size", "n", "N", "sample"]);
  const sample_size = sampleRaw && isNumeric(sampleRaw) ? parseInt(String(sampleRaw).replace(/[,]/g, ""), 10) : null;

  const url = pickFirst(row, ["url", "link", "source", "Poll", "poll_url"]) || null;

  const race = pickFirst(row, ["race", "Race", "contest", "Contest", "state", "State", "district", "District"]) || raceHint || null;

  const META = new Set([
    "pollster", "pollster_name",
    "start_date", "end_date", "date", "released", "release_date",
    "sample_size", "sample", "samplesize", "n",
    "url", "link", "source", "poll",
    "race", "contest", "office", "cycle",
    "notes", "population", "pop", "method", "mode"
  ]);

  const answers = [];
  for (const [k, v] of Object.entries(row)) {
    const nk = normKey(k);
    if (META.has(nk)) continue;
    if (!isNumeric(v)) continue;
    const pct = toNum(v);
    if (pct === null) continue;

    const kl = String(k).toLowerCase();

    if (kl.includes("approve") && !kl.includes("dis")) {
      answers.push({ choice: "Approve", pct });
    } else if (kl.includes("disapprove") || (kl.includes("dis") && kl.includes("approve"))) {
      answers.push({ choice: "Disapprove", pct });
    } else if (nk === "dem" || kl.includes("democrat") || nk === "dems") {
      answers.push({ choice: "Dem", pct });
    } else if (nk === "gop" || nk === "rep" || kl.includes("republican") || nk === "reps") {
      answers.push({ choice: "Rep", pct });
    } else {
      answers.push({ choice: String(k).trim(), pct });
    }
  }

  if (!answers.length) return null;

  const orderKey = (a) => {
    const c = a.choice.toLowerCase();
    if (c === "approve") return 0;
    if (c === "disapprove") return 1;
    if (c === "dem") return 2;
    if (c === "rep") return 3;
    return 10;
  };
  answers.sort((a, b) => orderKey(a) - orderKey(b));

  return {
    pollster,
    start_date,
    end_date,
    sample_size,
    answers,
    url,
    race,
  };
}

function classifyPoll(p) {
  const choices = p.answers.map((a) => a.choice.toLowerCase());
  const hasApprove = choices.some((c) => c.includes("approve"));
  const hasDis = choices.some((c) => c.includes("disapprove") || (c.includes("dis") && c.includes("approve")));
  const hasDem = choices.some((c) => c === "dem" || c.includes("democrat"));
  const hasRep = choices.some((c) => c === "rep" || c === "gop" || c.includes("republican"));
  if (hasApprove && hasDis && !hasDem && !hasRep) return "approval";
  if (hasDem && hasRep && !hasApprove && !hasDis) return "genericBallot";
  return "race";
}

async function main() {
  const startedAt = new Date().toISOString();

  ensureDir(OUT_SNAP_DIR);

  let playwright;
  try {
    playwright = require("playwright");
  } catch (e) {
    console.error("Missing dependency: playwright. In GitHub Actions, add `npm i playwright` and `npx playwright install --with-deps chromium`.");
    process.exit(2);
  }

  const { chromium } = playwright;

  const captured = [];
  const seen = new Set();
  const discoveredUrls = new Set();

  function considerCapture(url, status, contentType, resourceType) {
    if (!url) return false;
    if (BLOCK_URL_RE.test(url)) return false;
    if (seen.has(url)) return false;
    if (captured.length >= MAX_CAPTURED) return false;

    const rt = (resourceType || "").toLowerCase();
    if (["image", "font", "stylesheet", "media"].includes(rt)) return false;

    const ct = (contentType || "").toLowerCase();
    if (ct.includes("image/") || ct.includes("font/") || ct.includes("video/")) return false;

    return true;
  }

  const browser = await chromium.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
    ],
  });

  const context = await browser.newContext({
    userAgent: USER_AGENT,
    viewport: { width: 1365, height: 900 },
    locale: "en-US",
    timezoneId: "America/Chicago",
  });

  // basic anti-bot hardening (doesn't guarantee bypass)
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });

  const page = await context.newPage();

  page.on("response", async (res) => {
    try {
      const url = res.url();
      const status = res.status();
      const ct = res.headers()["content-type"] || "";
      const req = res.request();
      const rt = req.resourceType();

      if (!considerCapture(url, status, ct, rt)) return;

      seen.add(url);

      let body = "";
      try {
        body = await res.text();
      } catch {
        body = "";
      }

      const body_snippet = body ? body.slice(0, MAX_BODY_CHARS) : "";
      const rec = {
        url,
        status,
        content_type: ct,
        resource_type: rt,
        page_url: page.url(),
        body_len: body ? body.length : 0,
        body_snippet,
        ts: new Date().toISOString(),
      };
      captured.push(rec);

      for (const u of extractUrlsFromText(body_snippet)) discoveredUrls.add(u);
    } catch {
      // ignore
    }
  });

  async function visit(url) {
    console.log(`Opening ${url}`);
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: PAGE_TIMEOUT_MS });
      await page.waitForLoadState("networkidle", { timeout: PAGE_TIMEOUT_MS }).catch(() => {});
      await page.waitForTimeout(2500);

      const slug = url.replace(/^https?:\/\//, "").replace(/[^\w]+/g, "_").slice(0, 80);
      const html = await page.content();
      safeWrite(path.join(OUT_SNAP_DIR, `${slug}.html`), html);
      await page.screenshot({ path: path.join(OUT_SNAP_DIR, `${slug}.png`), fullPage: true });

      for (const u of extractUrlsFromText(html)) discoveredUrls.add(u);

      const scriptSrcs = await page.evaluate(() => Array.from(document.scripts).map(s => s.src).filter(Boolean));
      for (const s of scriptSrcs) discoveredUrls.add(s);

    } catch (e) {
      console.warn(`WARN: failed to load ${url}: ${e.message || e}`);
    }
  }

  for (const u of ENTRY_PAGES) await visit(u);

  await page.close();
  await context.close();
  await browser.close();

  const candidates = [];

  for (const r of captured) {
    const text = r.body_snippet || "";
    if (!text || text.length < MIN_DATASET_CHARS) continue;
    if (looksLikeCSV(text) || looksLikeJSON(text) || looksLikeGviz(text)) {
      candidates.push({ ...r, origin: "network" });
    }
  }

  const probeResults = [];
  const discoveredList = Array.from(discoveredUrls)
    .filter(u => !BLOCK_URL_RE.test(u))
    .filter(u => /https?:\/\//.test(u))
    .slice(0, 300);

  async function probeUrl(u) {
    try {
      const res = await fetch(u, {
        redirect: "follow",
        headers: { "User-Agent": USER_AGENT, "Accept": "*/*" },
      });
      const ct = res.headers.get("content-type") || "";
      const status = res.status;
      if (!res.ok) {
        probeResults.push({ url: u, status, content_type: ct, ok: false });
        return;
      }
      const text = await res.text();
      const snip = text.slice(0, MAX_BODY_CHARS);
      const rec = {
        url: u,
        status,
        content_type: ct,
        ok: true,
        body_len: text.length,
        body_snippet: snip,
        origin: "probe",
      };
      probeResults.push({ url: u, status, content_type: ct, ok: true, body_len: text.length });

      if (text.length >= MIN_DATASET_CHARS && (looksLikeCSV(snip) || looksLikeJSON(snip) || looksLikeGviz(snip))) {
        candidates.push(rec);
      }
    } catch (e) {
      probeResults.push({ url: u, status: null, content_type: null, ok: false, error: String(e) });
    }
  }

  const priority = (u) => {
    if (/docs\.google\.com\/spreadsheets\/d\//.test(u)) return 0;
    if (/\b(csv|json)\b/i.test(u)) return 1;
    if (/squarespace|static1\.squarespace/.test(u)) return 2;
    return 3;
  };
  discoveredList.sort((a, b) => priority(a) - priority(b));

  for (const u of discoveredList) {
    if (/\.(png|jpg|jpeg|gif|webp|svg)(\?|$)/i.test(u)) continue;
    await probeUrl(u);
  }

  candidates.sort((a, b) => scoreDatasetCandidate(b) - scoreDatasetCandidate(a));
  const top = candidates.slice(0, 15).map(c => ({
    url: c.url,
    origin: c.origin,
    score: scoreDatasetCandidate(c),
    status: c.status,
    content_type: c.content_type,
    body_len: c.body_len,
  }));

  safeWrite(
    OUT_SOURCES,
    JSON.stringify(
      {
        fetched_at: startedAt,
        entry_pages: ENTRY_PAGES,
        candidates_top15: top,
        captured_count: captured.length,
        discovered_url_count: discoveredUrls.size,
        probe_results: probeResults.slice(0, 500),
        captured_index: captured.map(r => ({
          url: r.url,
          status: r.status,
          content_type: r.content_type,
          resource_type: r.resource_type,
          page_url: r.page_url,
          body_len: r.body_len,
        })),
      },
      null,
      2
    )
  );

  let chosen = null;
  let rows = null;
  let datasetFormat = null;

  function tryParseCandidate(c) {
    const text = c.body_snippet || "";
    if (!text || text.length < MIN_DATASET_CHARS) return null;

    if (looksLikeGviz(text)) {
      const g = parseGviz(text);
      if (g && g.table && g.table.cols && g.table.rows) {
        const cols = g.table.cols.map(col => col.label || col.id || "");
        const out = g.table.rows.map(r => {
          const o = {};
          r.c.forEach((cell, i) => {
            o[cols[i]] = cell ? (cell.f ?? cell.v ?? "") : "";
          });
          return o;
        });
        return { rows: out, format: "gviz" };
      }
    }

    if (looksLikeJSON(text)) {
      try {
        const j = JSON.parse(text);
        if (Array.isArray(j)) return { rows: j, format: "json" };
        if (Array.isArray(j.data)) return { rows: j.data, format: "json" };
        if (Array.isArray(j.rows)) return { rows: j.rows, format: "json" };
        if (j.table && Array.isArray(j.table.rows)) return { rows: j.table.rows, format: "json" };
        return { rows: [j], format: "json" };
      } catch {
        return null;
      }
    }

    if (looksLikeCSV(text)) {
      const parsed = parseCSV(text);
      if (parsed.records && parsed.records.length) return { rows: parsed.records, format: "csv" };
    }

    return null;
  }

  for (const c of candidates) {
    const parsed = tryParseCandidate(c);
    if (!parsed) continue;

    const sample = parsed.rows[0] || {};
    const keys = Object.keys(sample).map(normKey);
    const hasPollishKey = keys.some(k => ["pollster","start_date","end_date","sample","sample_size","approve","disapprove","dem","gop","rep","race","contest"].includes(k));
    if (!hasPollishKey && parsed.rows.length < 50) continue;

    chosen = c;
    rows = parsed.rows;
    datasetFormat = parsed.format;
    break;
  }

  if (!rows || !rows.length) {
    console.error(`No plausible sheet/json dataset found. See ${OUT_SOURCES} -> candidates_top15 / probe_results / captured_index and ${OUT_SNAP_DIR}/ for snapshots.`);
    process.exit(3);
  }

  const out = {
    updatedAt: new Date().toISOString(),
    meta: {
      fetched_at: startedAt,
      source: "racetothewh",
      entry_url: ENTRY_PAGES[0],
      dataset_url: chosen.url,
      dataset_format: datasetFormat,
      notes: "Heuristic extraction. See rtwh_sources.json and rtwh_debug/ for debug artifacts.",
    },
    genericBallot: [],
    approval: [],
    races: {},
  };

  let kept = 0;
  let dropped = 0;

  for (const r of rows) {
    const poll = normalizeRowToPoll(r, null);
    if (!poll || !poll.answers || !poll.answers.length) {
      dropped++;
      continue;
    }

    const bucket = classifyPoll(poll);
    if (bucket === "approval") out.approval.push(poll);
    else if (bucket === "genericBallot") out.genericBallot.push(poll);
    else {
      const rk = String(poll.race || "Unknown").trim();
      if (!out.races[rk]) out.races[rk] = [];
      out.races[rk].push(poll);
    }
    kept++;
  }

  function dedup(list) {
    const seen = new Set();
    const out = [];
    for (const p of list) {
      const k = [p.pollster || "", p.end_date || "", p.race || ""].join("|").toLowerCase();
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(p);
    }
    return out;
  }
  out.approval = dedup(out.approval);
  out.genericBallot = dedup(out.genericBallot);
  for (const k of Object.keys(out.races)) out.races[k] = dedup(out.races[k]);

  const hasAny = out.approval.length || out.genericBallot.length || Object.keys(out.races).length;
  if (!hasAny) {
    console.error("Parsed a dataset candidate but produced 0 polls after normalization. Inspect rtwh_sources.json and rtwh_debug snapshots.");
    process.exit(4);
  }

  safeWrite(OUT_JSON, JSON.stringify(out, null, 2));

  console.log(`OK: wrote ${OUT_JSON}`);
  console.log(`  dataset_url: ${out.meta.dataset_url}`);
  console.log(`  genericBallot: ${out.genericBallot.length}`);
  console.log(`  approval:      ${out.approval.length}`);
  console.log(`  races:         ${Object.keys(out.races).length} keys`);
  console.log(`  dropped_rows:  ${dropped} (kept ${kept})`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
