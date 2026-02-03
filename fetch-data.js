"use strict";

/**
 * RTWH DOM table scraper -> polls.json
 *
 * Why this works:
 * - RTWH renders a poll table in HTML (your screenshot). We extract rows from DOM.
 * - We parse the "Data" cell: "Name 54.0% Other 46.0%" into answers[].
 *
 * Outputs:
 * - polls.json (on success): { updatedAt, meta, genericBallot, approval, races }
 * - rtwh_status.json (always): { ok, fetched_at, reason?, counts?, pages? }
 * - rtwh_debug/* (always): per-page HTML+PNG snapshots
 */

const fs = require("fs");
const path = require("path");

const OUT_POLLS = path.join(__dirname, "polls.json");
const OUT_STATUS = path.join(__dirname, "rtwh_status.json");
const OUT_DEBUG_DIR = path.join(__dirname, "rtwh_debug");

const USER_AGENT =
  process.env.RTWH_UA ||
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const PAGE_TIMEOUT_MS = Number(process.env.RTWH_PAGE_TIMEOUT_MS || "90000");
const WAIT_MS = Number(process.env.RTWH_WAIT_MS || "8000");

// Keep it focused. Add more pages later if needed.
const PAGES = [
  { url: "https://www.racetothewh.com/allpolls", defaultYear: 2026 },
  { url: "https://www.racetothewh.com/polls/genericballot", defaultYear: 2026 },
  { url: "https://www.racetothewh.com/trump", defaultYear: 2026 },
  { url: "https://www.racetothewh.com/senate/26polls", defaultYear: 2026 },
  { url: "https://www.racetothewh.com/governor/26polls", defaultYear: 2026 },
  { url: "https://www.racetothewh.com/house/26polls", defaultYear: 2026 },
  { url: "https://www.racetothewh.com/president/2028/polls", defaultYear: 2028 },
  { url: "https://www.racetothewh.com/president/2028/dem", defaultYear: 2028 },
  { url: "https://www.racetothewh.com/president/2028/gop", defaultYear: 2028 },
];

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
function normWS(s) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}
function slugify(url) {
  return url.replace(/^https?:\/\//, "").replace(/[^\w]+/g, "_").slice(0, 90);
}
function monthNum(mon) {
  const m = mon.toLowerCase().slice(0, 3);
  const map = {
    jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
    jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12
  };
  return map[m] || null;
}
function ymd(y, m, d) {
  const mm = String(m).padStart(2, "0");
  const dd = String(d).padStart(2, "0");
  return `${y}-${mm}-${dd}`;
}
function firstNumber(x) {
  if (x === null || x === undefined) return null;
  const s = String(x)
    .trim()
    .replace(/\u2212/g, "-")
    .replace(/,/g, "");
  if (!s) return null;
  const m = s.match(/-?\d+(\.\d+)?/);
  if (!m) return null;
  const n = Number(m[0]);
  return Number.isFinite(n) ? n : null;
}

// Parse name + pct pairs from the Data cell.
// Ex: "Democrat 54.0% GOP 46.0%" or "Letlow 27.0% Cassidy 21.0% ..."
function parseNamePctPairs(s) {
  const text = normWS(s);
  if (!text) return [];
  if (/^no data$/i.test(text)) return [];

  const out = [];
  const re = /([A-Za-z0-9][A-Za-z0-9 .,&'/-]{0,60}?)\s+(-?\d+(?:\.\d+)?)%/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const name = normWS(m[1]);
    const pct = Number(m[2]);
    if (!name || !Number.isFinite(pct)) continue;
    // guard against "D+20.0%" style artifacts
    if (/^[DR]\+?$/i.test(name)) continue;
    out.push({ choice: name, pct });
  }
  return out;
}

// Extract pollster + date range + sample/pop from the "Poll" cell text.
// Typical: "Jan 28 - 29: HarrisX (B-), 2000 RV"
function parsePollCell(pollText, defaultYear) {
  const s = normWS(pollText);
  if (!s) return { pollster: null, start_date: null, end_date: null, sample_size: null, population: null };

  const parts = s.split(":");
  const left = normWS(parts[0] || "");
  const right = normWS(parts.slice(1).join(":") || "");

  // infer year from ", 26" or ", 2026" in left (or anywhere)
  let year = defaultYear;
  const y2 = s.match(/,\s*(\d{2})\b/);
  const y4 = s.match(/\b(20\d{2})\b/);
  if (y4) year = Number(y4[1]);
  else if (y2) year = 2000 + Number(y2[1]);

  // date range parse
  // handles:
  //  - "Jan 28 - 29"
  //  - "Jan 28 - Feb 2"
  //  - "Jan 28" (single)
  let start_date = null;
  let end_date = null;

  const cleanedLeft = left.replace(/,\s*\d{2,4}\b/g, "").replace(/\u2013|\u2014/g, "-");
  const mRange = cleanedLeft.match(
    /^([A-Za-z]{3,9})\s+(\d{1,2})\s*-\s*([A-Za-z]{3,9})?\s*(\d{1,2})$/
  );
  const mSingle = cleanedLeft.match(/^([A-Za-z]{3,9})\s+(\d{1,2})$/);

  if (mRange) {
    const m1 = monthNum(mRange[1]);
    const d1 = Number(mRange[2]);
    const m2 = monthNum(mRange[3] || mRange[1]);
    const d2 = Number(mRange[4]);
    if (m1 && m2) {
      start_date = ymd(year, m1, d1);
      end_date = ymd(year, m2, d2);
    }
  } else if (mSingle) {
    const m1 = monthNum(mSingle[1]);
    const d1 = Number(mSingle[2]);
    if (m1) {
      start_date = ymd(year, m1, d1);
      end_date = start_date;
    }
  }

  // pollster: right side until "(" or "," (whichever first)
  let pollster = null;
  if (right) {
    const stop = right.search(/[,(]/);
    pollster = normWS(stop === -1 ? right : right.slice(0, stop));
    if (!pollster) pollster = null;
  }

  // sample size: last number with >=3 digits in right
  let sample_size = null;
  let population = null;
  if (right) {
    const nums = Array.from(right.matchAll(/(\d[\d,]{2,})/g)).map(m => m[1]);
    if (nums.length) {
      const last = nums[nums.length - 1].replace(/,/g, "");
      const n = Number(last);
      if (Number.isFinite(n)) sample_size = Math.trunc(n);
      const after = right.slice(right.lastIndexOf(nums[nums.length - 1]) + nums[nums.length - 1].length);
      const pop = normWS(after).match(/\b(LV|RV|A|Adults|Voters)\b/i);
      if (pop) population = pop[1].toUpperCase();
    }
  }

  return { pollster, start_date, end_date, sample_size, population };
}

// Normalize section cell. Often has "Senate" on first line and race detail on next line(s).
function parseSectionCell(sectionText) {
  const lines = String(sectionText || "")
    .split("\n")
    .map(normWS)
    .filter(Boolean);
  const group = lines[0] || null;
  const detail = lines.slice(1).join(" • ") || null;
  return { group, detail };
}

function classify(group, dataPairs) {
  const g = (group || "").toLowerCase();
  const choices = dataPairs.map(a => String(a.choice).toLowerCase());

  const hasApprove = choices.some(c => c.includes("approve")) && choices.some(c => c.includes("disapprove"));
  if (g.includes("approval") || hasApprove) return "approval";

  const hasDem = choices.some(c => c === "dem" || c.includes("democrat") || c.includes("dem"));
  const hasGop = choices.some(c => c === "gop" || c === "rep" || c.includes("republican"));
  if (g.includes("generic ballot") || (hasDem && hasGop)) return "genericBallot";

  return "race";
}

function dedupPolls(list) {
  const seen = new Set();
  const out = [];
  for (const p of list) {
    const key = [
      p.pollster || "",
      p.end_date || "",
      p.race || "",
      (p.answers || []).map(a => `${a.choice}:${a.pct}`).join("|"),
      p.url || ""
    ].join("||").toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(p);
  }
  return out;
}

async function scrapeTables(page) {
  // Return an array of tables with headers and row cells (text + href).
  return await page.evaluate(() => {
    const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
    const tables = Array.from(document.querySelectorAll("table"));

    function cellObj(td) {
      const a = td.querySelector("a[href]");
      return {
        text: norm(td.innerText),
        href: a ? a.href : null
      };
    }

    const out = [];
    for (const t of tables) {
      const headerRow =
        t.querySelector("thead tr") ||
        t.querySelector("tr");

      if (!headerRow) continue;

      const headerCells = Array.from(headerRow.querySelectorAll("th,td")).map(el => norm(el.innerText));
      if (!headerCells.length) continue;

      const bodyRows = Array.from(t.querySelectorAll("tbody tr")).length
        ? Array.from(t.querySelectorAll("tbody tr"))
        : Array.from(t.querySelectorAll("tr")).slice(1);

      const rows = bodyRows
        .map(tr => Array.from(tr.querySelectorAll("td,th")).map(cellObj))
        .filter(r => r.some(c => c.text));

      out.push({ headers: headerCells, rows });
    }
    return out;
  });
}

function findLikelyPollTables(tables) {
  // Prefer tables that include "Poll" and "Data".
  const scored = tables.map(t => {
    const h = t.headers.map(x => x.toLowerCase());
    let score = 0;
    if (h.some(x => x.includes("poll"))) score += 5;
    if (h.some(x => x.includes("data"))) score += 6;
    if (h.some(x => x.includes("leader"))) score += 2;
    if (h.some(x => x.includes("section"))) score += 2;
    if (t.rows.length >= 5) score += 2;
    return { t, score };
  }).sort((a,b) => b.score - a.score);

  // keep any with a decent signal
  return scored.filter(x => x.score >= 7).map(x => x.t);
}

function indexOfHeader(headers, needle) {
  const n = needle.toLowerCase();
  for (let i = 0; i < headers.length; i++) {
    if (headers[i].toLowerCase().includes(n)) return i;
  }
  return -1;
}

async function main() {
  ensureDir(OUT_DEBUG_DIR);
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

  const page = await context.newPage();

  const debugPages = [];
  const extracted = [];

  for (const spec of PAGES) {
    const { url, defaultYear } = spec;
    console.log("Opening", url);

    let okLoad = true;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: PAGE_TIMEOUT_MS });

      // scroll to trigger lazy-load
      for (let i = 0; i < 2; i++) {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await page.waitForTimeout(1200);
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.waitForTimeout(700);
      }
      await page.waitForTimeout(WAIT_MS);
    } catch {
      okLoad = false;
    }

    const slug = slugify(url) + (okLoad ? "" : "_failed");
    try {
      const html = await page.content();
      fs.writeFileSync(path.join(OUT_DEBUG_DIR, `${slug}.html`), html, "utf8");
      await page.screenshot({ path: path.join(OUT_DEBUG_DIR, `${slug}.png`), fullPage: true });
    } catch {
      // ignore snapshot failures
    }

    const tables = await scrapeTables(page);
    const pollTables = findLikelyPollTables(tables);

    debugPages.push({
      url,
      defaultYear,
      okLoad,
      tables: tables.length,
      pollTables: pollTables.length,
      pollTableHeaders: pollTables.slice(0, 3).map(t => t.headers)
    });

    for (const t of pollTables) {
      const headers = t.headers;
      const idxAdded = indexOfHeader(headers, "added");
      const idxSection = indexOfHeader(headers, "section");
      const idxType = indexOfHeader(headers, "type");
      const idxPoll = indexOfHeader(headers, "poll");
      const idxLeader = indexOfHeader(headers, "leader");
      const idxData = indexOfHeader(headers, "data");

      for (const row of t.rows) {
        // Basic requirement: we need a Poll cell and some Data-like content (either Data column or numeric pairs elsewhere)
        const pollCell = idxPoll >= 0 ? row[idxPoll] : null;
        const dataCell = idxData >= 0 ? row[idxData] : null;

        const pollText = pollCell?.text || "";
        const pollUrl = pollCell?.href || null;

        const sectionText = idxSection >= 0 ? (row[idxSection]?.text || "") : "";
        const { group, detail } = parseSectionCell(sectionText);

        const typeText = idxType >= 0 ? (row[idxType]?.text || "") : "";
        const leaderText = idxLeader >= 0 ? (row[idxLeader]?.text || "") : "";

        // answers: prefer Data column; otherwise attempt to parse from any cell containing % pairs
        let answers = [];
        if (dataCell && dataCell.text) {
          answers = parseNamePctPairs(dataCell.text);
        } else {
          // fallback: scan all cells
          for (const c of row) {
            const pairs = parseNamePctPairs(c.text);
            if (pairs.length >= 2) { answers = pairs; break; }
          }
        }
        if (answers.length < 2) continue;

        const parsedPoll = parsePollCell(pollText, defaultYear);

        // race key
        let raceKey = null;
        if (detail) raceKey = detail;
        else if (group) raceKey = group;
        else raceKey = "Other";

        // attach type if it helps uniqueness (e.g., National / Primary etc.)
        const typeNorm = normWS(typeText);
        if (typeNorm && !raceKey.toLowerCase().includes(typeNorm.toLowerCase())) {
          // only append for non-generic/approval
          if (!/generic ballot/i.test(raceKey) && !/approval/i.test(raceKey)) {
            raceKey = `${raceKey} • ${typeNorm}`;
          }
        }

        const poll = {
          pollster: parsedPoll.pollster,
          start_date: parsedPoll.start_date,
          end_date: parsedPoll.end_date,
          sample_size: parsedPoll.sample_size,
          population: parsedPoll.population,
          url: pollUrl,
          race: raceKey,
          section_group: group,
          section_type: typeNorm || null,
          leader: leaderText || null,
          answers
        };

        extracted.push(poll);
      }
    }
  }

  await browser.close();

  // Bucket
  const genericBallot = [];
  const approval = [];
  const races = {};

  for (const p of extracted) {
    const bucket = classify(p.section_group || p.race, p.answers);
    const cleaned = {
      pollster: p.pollster,
      start_date: p.start_date,
      end_date: p.end_date,
      sample_size: p.sample_size,
      population: p.population,
      url: p.url && /^https?:\/\//i.test(p.url) ? p.url : null,
      race: p.race,
      answers: p.answers
    };

    if (bucket === "genericBallot") genericBallot.push(cleaned);
    else if (bucket === "approval") approval.push(cleaned);
    else {
      const k = cleaned.race || "Other";
      (races[k] ||= []).push(cleaned);
    }
  }

  const out = {
    updatedAt: nowISO(),
    meta: {
      fetched_at: nowISO(),
      source: "racetothewh",
      entry_url: "https://www.racetothewh.com/allpolls",
      method: "dom_table_scrape"
    },
    genericBallot: dedupPolls(genericBallot),
    approval: dedupPolls(approval),
    races: Object.fromEntries(Object.entries(races).map(([k, v]) => [k, dedupPolls(v)]))
  };

  const ok =
    (out.genericBallot.length > 0) ||
    (out.approval.length > 0) ||
    (Object.keys(out.races).length > 0);

  if (!ok) {
    writeStatus(false, { reason: "no polls parsed from DOM tables", pages: debugPages });
    // still write an empty-ish polls.json for inspection
    writeJSON(OUT_POLLS, out);
    process.exit(4);
  }

  writeJSON(OUT_POLLS, out);
  writeStatus(true, {
    counts: {
      genericBallot: out.genericBallot.length,
      approval: out.approval.length,
      races: Object.keys(out.races).length
    },
    pages: debugPages
  });

  console.log("OK: wrote polls.json");
  console.log("  genericBallot:", out.genericBallot.length);
  console.log("  approval:", out.approval.length);
  console.log("  races:", Object.keys(out.races).length);
}

main().catch((e) => {
  try { writeStatus(false, { reason: "unhandled exception", error: String(e?.stack || e) }); } catch {}
  console.error(e);
  process.exit(1);
});
