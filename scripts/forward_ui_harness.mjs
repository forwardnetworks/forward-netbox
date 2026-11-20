#!/usr/bin/env node
// Deterministic UI harness for the Forward NetBox plugin, driven by the
// `agent-browser` CLI (Chrome over CDP). Every UI check in this repo is a
// journey under scripts/ui/journeys/; this runner seeds the stack, runs them in
// file order in one logged-in browser session, and writes one report with an
// entry per journey. A failing journey fails the run (`invoke ui-test`, and so
// the release gate), but the rest still run so one report shows everything.
import { readdirSync, readFileSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";

import { h } from "./ui/lib.mjs";

const journeysDir = path.join(import.meta.dirname, "ui", "journeys");
// Journeys the release gate requires. Deleting or renaming one is a change to the
// gate itself: it must be called out in the commit and reviewed on its own.
const requiredFile = path.join(import.meta.dirname, "ui", "required-journeys.txt");
const requiredJourneys = readFileSync(requiredFile, "utf8")
  .split("\n")
  .map((line) => line.trim())
  .filter((line) => line && !line.startsWith("#"));

// Uncaught errors the page threw while a journey ran. A page that works for the
// assertions can still be throwing underneath.
function pageErrorList() {
  const parsed = JSON.parse(h.browser(["errors"], { json: true }));
  return ((parsed.data && parsed.data.errors) || []).map((error) =>
    String(error.text || error.message || error),
  );
}

async function loadJourneys() {
  const files = readdirSync(journeysDir)
    .filter((name) => name.endsWith(".mjs"))
    .sort();
  const journeys = [];
  for (const file of files) {
    const module = await import(path.join(journeysDir, file));
    journeys.push({ ...module.default, file });
  }
  const ids = journeys.map((journey) => journey.id);
  h.assert(new Set(ids).size === ids.length, `duplicate journey ids: ${ids}`);
  return journeys;
}

async function main() {
  mkdirSync(h.artifactDir, { recursive: true });
  if (process.env.FORWARD_UI_SKIP_MIGRATE !== "true") {
    h.runDockerManage("migrate --noinput");
  }
  h.startStub();
  h.runDockerManage("forward_seed_ui_harness");
  await h.waitForWebReady(`${h.baseURL}/login/`);

  const journeys = await loadJourneys();
  const report = { baseURL: h.baseURL, journeys: [], screenshots: [], checks: [] };

  try {
    for (const journey of journeys) {
      h.browser(["set", "viewport", "1440", "1000"]);
      const evidence = { screenshots: [], checks: [] };
      const entry = { id: journey.id, title: journey.title, file: journey.file };
      const started = Date.now();
      try {
        // The list may keep earlier errors whatever --clear does, so judge only what
        // the journey added to it.
        const before = pageErrorList().length;
        await journey.run({ evidence });
        const errors = pageErrorList().slice(before).join(" | ");
        if (errors) throw new Error(`the page threw errors during the journey: ${errors.slice(0, 600)}`);
        if (!evidence.checks.length) throw new Error("the journey recorded no check, so it proved nothing");
        entry.status = "passed";
      } catch (error) {
        entry.status = "failed";
        entry.error = error && error.message ? error.message : String(error);
        try {
          console.error(h.stubLog());
        } catch (_) {
          /* the log is a courtesy */
        }
        try {
          h.browser(["screenshot", path.join(h.artifactDir, `failed-${journey.id}.jpg`)]);
        } catch (_) {
          /* the picture is a courtesy */
        }
      }
      entry.seconds = Math.round((Date.now() - started) / 1000);
      entry.checks = evidence.checks;
      report.journeys.push(entry);
      report.screenshots.push(...evidence.screenshots);
      report.checks.push(...evidence.checks);
      console.log(`${entry.status.toUpperCase()} ${journey.id} (${entry.seconds}s)`);
      if (journey.id === "authentication" && entry.status === "passed") {
        await h.login();
      }
    }
  } finally {
    try {
      h.browser(["close"]);
    } catch (error) {
      console.error(`agent-browser close failed: ${error.message}`);
    }
  }

  report.passed = report.journeys.filter((entry) => entry.status === "passed").length;
  report.failed = report.journeys.filter((entry) => entry.status !== "passed").length;
  writeFileSync(
    path.join(h.artifactDir, "forward-ui-summary.json"),
    `${JSON.stringify(report, null, 2)}\n`,
    "utf8",
  );
  console.log(JSON.stringify(report, null, 2));
  if (report.failed) {
    const failed = report.journeys.filter((entry) => entry.status !== "passed");
    throw new Error(
      `${report.failed} UI journey(s) failed: ` +
        failed.map((entry) => `${entry.id}: ${entry.error}`).join("; "),
    );
  }
  h.assert(report.passed > 0, "no UI journey ran");
  const passedIds = new Set(report.journeys.filter((e) => e.status === "passed").map((e) => e.id));
  const missing = requiredJourneys.filter((id) => !passedIds.has(id));
  h.assert(
    missing.length === 0,
    `required UI journeys did not run and pass: ${missing.join(", ")}`,
  );
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
