#!/usr/bin/env node
// Deterministic UI harness for the Forward NetBox plugin, driven by the
// `agent-browser` CLI (Chrome over CDP). Every UI check in this repo is a
// journey under scripts/ui/journeys/; this runner seeds the stack, runs them in
// file order in one logged-in browser session, and writes one report with an
// entry per journey. A failing journey fails the run (`invoke ui-test`, and so
// the release gate), but the rest still run so one report shows everything.
import { readdirSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";

import { h } from "./ui/lib.mjs";

const journeysDir = path.join(import.meta.dirname, "ui", "journeys");

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
        await journey.run({ evidence });
        entry.status = "passed";
      } catch (error) {
        entry.status = "failed";
        entry.error = error && error.message ? error.message : String(error);
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
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
