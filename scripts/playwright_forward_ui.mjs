#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { chromium } from "playwright";

const repoRoot = path.resolve(import.meta.dirname, "..");
const artifactDir = path.resolve(
  repoRoot,
  process.env.PLAYWRIGHT_ARTIFACT_DIR || ".playwright-artifacts",
);
const baseURL = (process.env.NETBOX_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
const username = process.env.NETBOX_USERNAME || "admin";
const password = process.env.NETBOX_PASSWORD || "admin";
const dockerProjectName =
  process.env.PLAYWRIGHT_DOCKER_PROJECT_NAME || "forward-netbox";
const dockerProjectDirectory =
  process.env.PLAYWRIGHT_DOCKER_PROJECT_DIRECTORY || "development";

const dockerComposeArgs = [
  "--project-name",
  dockerProjectName,
  "--project-directory",
  dockerProjectDirectory,
  "exec",
  "-T",
  "netbox",
  "bash",
  "-lc",
];

function runDockerManage(command) {
  execFileSync(
    "docker",
    [
      "compose",
      ...dockerComposeArgs,
      `cd /opt/netbox/netbox && python manage.py ${command}`,
    ],
    {
      cwd: repoRoot,
      env: {
        ...process.env,
        NETBOX_UI_TEST_USERNAME: username,
        NETBOX_UI_TEST_PASSWORD: password,
      },
      stdio: "inherit",
    },
  );
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function expectVisible(page, text) {
  await page
    .getByText(text, { exact: false })
    .filter({ visible: true })
    .first()
    .waitFor({ state: "visible" });
}

async function assertNoHorizontalOverflow(page, label) {
  const overflow = await page.evaluate(() => {
    const documentWidth = document.documentElement.scrollWidth;
    const viewportWidth = document.documentElement.clientWidth;
    return Math.max(0, documentWidth - viewportWidth);
  });
  assert(overflow <= 4, `${label} has horizontal overflow of ${overflow}px`);
}

async function login(page) {
  await page.goto(`${baseURL}/login/?next=/plugins/forward/sync/`, {
    waitUntil: "domcontentloaded",
  });
  await page.locator('input[name="username"]').fill(username);
  await page.locator('input[name="password"]').fill(password);
  await page.getByRole("button", { name: /sign in|log in/i }).click();
  await page.waitForURL(/\/plugins\/forward\/sync\/?$/);
}

async function screenshot(page, name, { fullPage = true } = {}) {
  const target = path.join(artifactDir, name);
  await page.screenshot({ path: target, fullPage, type: "jpeg", quality: 85 });
  return target;
}

async function waitForWebReady(url, timeoutMs = 180000) {
  // The seed runs via `docker exec manage.py` (no web server needed), so a
  // successful seed does NOT mean gunicorn is serving yet. Poll the URL until
  // it responds before driving the browser, otherwise the first navigation
  // times out against a cold container.
  const deadline = Date.now() + timeoutMs;
  let lastErr = "no response";
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url, { redirect: "manual" });
      if (res.status > 0) return;
    } catch (err) {
      lastErr = err && err.message ? err.message : String(err);
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  throw new Error(
    `web server not ready at ${url} within ${timeoutMs}ms (last: ${lastErr})`,
  );
}

async function main() {
  mkdirSync(artifactDir, { recursive: true });
  if (process.env.PLAYWRIGHT_SKIP_MIGRATE !== "true") {
    runDockerManage("migrate --noinput");
  }
  runDockerManage("forward_seed_ui_harness");
  await waitForWebReady(`${baseURL}/login/`);

  const browserLaunchOptions = {
    headless: process.env.PLAYWRIGHT_HEADLESS !== "false",
  };
  const executablePath = process.env.PLAYWRIGHT_EXECUTABLE_PATH?.trim();
  if (executablePath) browserLaunchOptions.executablePath = executablePath;
  const browser = await chromium.launch(browserLaunchOptions);
  const context = await browser.newContext({
    baseURL,
    viewport: { width: 1440, height: 1000 },
  });
  const page = await context.newPage();
  const evidence = {
    baseURL,
    screenshots: [],
    checks: [],
  };

  try {
    const unauthenticatedResponse = await page.goto(
      `${baseURL}/plugins/forward/sync/`,
      { waitUntil: "domcontentloaded" },
    );
    const unauthenticatedURL = new URL(page.url());
    const redirectedToLogin =
      unauthenticatedResponse?.ok() &&
      unauthenticatedURL.pathname === "/login/" &&
      unauthenticatedURL.searchParams.get("next") ===
        "/plugins/forward/sync/";
    const rejectedUnauthenticated =
      unauthenticatedResponse?.status() === 401 &&
      unauthenticatedURL.pathname === "/plugins/forward/sync/";
    assert(
      redirectedToLogin || rejectedUnauthenticated,
      `unauthenticated sync list did not require authentication ` +
        `(status=${unauthenticatedResponse?.status() ?? "none"}, ` +
        `url=${page.url()})`,
    );
    evidence.checks.push("unauthenticated sync list requires authentication");

    await login(page);
    await expectVisible(page, "Forward Syncs");
    await expectVisible(page, "ui-harness-sync");
    await assertNoHorizontalOverflow(page, "desktop sync list");
    evidence.checks.push("sync list renders seeded fixture");

    await page.getByRole("link", { name: "ui-harness-sync" }).first().click();
    await expectVisible(page, "Sync Information");
    await expectVisible(page, "Enabled Models");
    await expectVisible(page, "Adhoc Ingestion");
    await expectVisible(page, "Validate");
    await expectVisible(page, "Drift Policy");
    await expectVisible(page, "Latest Validation");
    await expectVisible(page, "Workload Preview");
    await expectVisible(page, "Analysis Summary");
    await expectVisible(page, "Advisory Summary");
    await expectVisible(page, "Export Support Bundle");
    await expectVisible(page, "Health");
    await expectVisible(page, "ui-harness-drift-policy");
    await expectVisible(page, "latestProcessed");
    await expectVisible(page, "max_changes_per_staging_item");
    await expectVisible(page, "Current activity");
    await assertNoHorizontalOverflow(page, "desktop sync detail");
    evidence.screenshots.push(await screenshot(page, "desktop-sync-detail.jpg"));
    evidence.checks.push(
      "sync detail exposes validation, single-branch run controls, support export, and current activity",
    );

    await page.getByRole("link", { name: "Drift Report", exact: true }).click();
    await expectVisible(page, "Drift Report");
    await expectVisible(page, "Latest Sync Evidence");
    await expectVisible(page, "Not confirmed");
    await expectVisible(page, "Same as preview");
    await expectVisible(page, "Run this sync again against the same snapshot");
    await expectVisible(page, "Not measured");
    await assertNoHorizontalOverflow(page, "desktop drift report");
    evidence.screenshots.push(await screenshot(page, "desktop-drift-report.jpg"));
    evidence.checks.push(
      "drift report distinguishes workload estimates from same-snapshot convergence evidence",
    );

    await page.goto(`${baseURL}/plugins/forward/sync/`, {
      waitUntil: "domcontentloaded",
    });
    await page.getByRole("link", { name: "ui-harness-sync" }).first().click();

    await page.locator('a[href*="/sync/"][href$="/health/"]').first().click();
    await expectVisible(page, "Health Summary");
    await expectVisible(page, "Export Live Source Check");
    await expectVisible(page, "Query Binding");
    await expectVisible(page, "Local Query Drift");
    await expectVisible(page, "Export Live Query Drift Check");
    await expectVisible(page, "Export Live Data File Check");
    await expectVisible(page, "Publish Bundled Queries");
    assert(
      (await page.getByText("Refresh Query IDs", { exact: true }).count()) === 0,
      "sync health should not expose the retired Refresh Query IDs action",
    );
    await expectVisible(page, "Forward API Usage");
    await expectVisible(page, "Dependency Lookup Cache");
    await expectVisible(page, "Density Learning");
    await expectVisible(page, "Ownership finalization");
    await expectVisible(page, "Diff-capable maps");
    await expectVisible(page, "Next run");
    await expectVisible(page, "Health Details");
    await assertNoHorizontalOverflow(page, "desktop sync health");
    evidence.screenshots.push(
      await screenshot(page, "desktop-sync-health.jpg", { fullPage: false }),
    );
    evidence.checks.push(
      "sync health tab renders local diagnostics, live query-path publishing, and explicit live source/query/data-file exports without the retired refresh action",
    );

    await page.goto(`${baseURL}/plugins/forward/validation-run/`, {
      waitUntil: "domcontentloaded",
    });
    await expectVisible(page, "Forward Validation Runs");
    await expectVisible(page, "ui-harness-sync");
    await expectVisible(page, "Passed");
    await assertNoHorizontalOverflow(page, "desktop validation run list");
    evidence.checks.push("validation run list renders seeded validation records");

    await page.goto(`${baseURL}/plugins/forward/sync/`, {
      waitUntil: "domcontentloaded",
    });
    await page.getByRole("link", { name: "ui-harness-sync" }).first().click();
    await page.getByRole("link", { name: "Passed" }).first().click();
    await expectVisible(page, "Validation Run");
    await expectVisible(page, "Drift Summary");
    await expectVisible(page, "Model Results");
    await expectVisible(page, "ui-harness-sync");
    await assertNoHorizontalOverflow(page, "desktop validation detail");
    evidence.screenshots.push(await screenshot(page, "desktop-validation-detail.jpg"));
    evidence.checks.push("validation detail renders drift summary and model results");

    await page.goto(`${baseURL}/plugins/forward/sync/`, {
      waitUntil: "domcontentloaded",
    });
    await page.getByRole("link", { name: "ui-harness-sync" }).first().click();
    await page
      .getByRole("link", { name: /ui-harness-sync \(Ingestion \d+\)/ })
      .click();
    await expectVisible(page, "Ingestion Information");
    await expectVisible(page, "Progress");
    await expectVisible(page, "Statistics");
    await expectVisible(page, "Forward Snapshot Metrics");
    await expectVisible(page, "Workload Preview");
    await expectVisible(page, "Analysis Summary");
    await expectVisible(page, "Advisory Summary");
    await expectVisible(page, "Validation");
    await expectVisible(page, "Model Results");
    await expectVisible(page, "Sync Results");
    await expectVisible(page, "Export Logs");
    await expectVisible(page, "Synthetic UI harness ingestion completed.");
    await assertNoHorizontalOverflow(page, "desktop ingestion detail");
    evidence.screenshots.push(await screenshot(page, "desktop-ingestion-detail.jpg"));
    evidence.checks.push("ingestion detail renders progress, statistics, metrics, and logs");

    await page.goto(`${baseURL}/plugins/forward/sync/add/`, {
      waitUntil: "domcontentloaded",
    });
    await expectVisible(page, "Forward Sync");
    await expectVisible(page, "Model Selection");
    await expectVisible(page, "Execution");
    await expectVisible(page, "Drift policy");
    await expectVisible(page, "Auto merge");
    await expectVisible(page, "Use safe bulk ORM models");
    await expectVisible(page, "Diff fallback mode");
    await assertNoHorizontalOverflow(page, "desktop sync form");
    evidence.checks.push(
      "sync creation form exposes single-branch merge, apply-engine, and diff fallback controls",
    );

    const sourceEditResponse = await page.goto(
      `${baseURL}/plugins/forward/source/1/edit/`,
      {
        waitUntil: "domcontentloaded",
      },
    );
    assert(
      sourceEditResponse?.ok(),
      "source edit form did not return a success response",
    );
    assert(
      new URL(page.url()).pathname === "/plugins/forward/source/1/edit/",
      `source edit form redirected to ${page.url()}`,
    );
    await expectVisible(page, "Forward Source");
    for (const fieldLabel of [
      "Apply Device Scope Tags",
      "Import SNMP Endpoints as Devices",
      "Import Generic SNMP Endpoints as Devices",
      "Scope SNMP Endpoints by Include Tags",
    ]) {
      const field = page.getByLabel(fieldLabel, { exact: true });
      assert((await field.count()) === 1, `source form is missing ${fieldLabel}`);
      assert(await field.isVisible(), `source form hides ${fieldLabel}`);
    }
    await assertNoHorizontalOverflow(page, "desktop source form");
    evidence.screenshots.push(await screenshot(page, "desktop-source-form.jpg"));
    evidence.checks.push(
      "source form exposes scope tags, console-server import, generic endpoint opt-in, and endpoint include scope",
    );

    await page.goto(`${baseURL}/plugins/forward/nqe-map/add/`, {
      waitUntil: "domcontentloaded",
    });
    await expectVisible(page, "Forward NQE Map");
    await expectVisible(page, "Query Definition Mode");
    await expectVisible(page, "Forward Source for Query Lookup");
    await expectVisible(page, "Query Repository");
    await expectVisible(page, "Query Folder");
    await expectVisible(page, "Query Path");
    await expectVisible(page, "Query ID");
    await expectVisible(page, "Commit ID");
    const queryModeTag = await page
      .locator('[name="query_mode"]')
      .evaluate((element) => element.tagName);
    assert(queryModeTag === "SELECT", "NQE map query mode should render as a select");
    assert(
      (await page.locator('input[type="radio"][name="query_mode"]').count()) === 0,
      "NQE map query mode should not render as mis-styled radio inputs",
    );
    await assertNoHorizontalOverflow(page, "desktop NQE map form");
    evidence.checks.push("NQE map form exposes repository path, direct query ID, and commit selectors");

    await page.goto(`${baseURL}/plugins/forward/nqe-map/`, {
      waitUntil: "domcontentloaded",
    });
    await expectVisible(page, "Forward NQE Maps");
    await page.locator('input[name="pk"]').first().check();
    await page.getByRole("button", { name: /Edit Selected/ }).click();
    await expectVisible(page, "Bulk Edit");
    await expectVisible(page, "Bulk Query Reference");
    await expectVisible(page, "Query Bulk Operation");
    const bulkOperationOptions = await page
      .locator('[name="query_bulk_operation"] option')
      .allTextContents();
    assert(
      bulkOperationOptions.includes("Use repository query paths (query IDs resolve at sync time)"),
      "NQE map bulk edit should offer repository path binding",
    );
    assert(
      bulkOperationOptions.includes("Publish bundled queries and use repository query paths"),
      "NQE map bulk edit should offer bundled query publishing",
    );
    assert(
      bulkOperationOptions.includes("Restore bundled raw query text"),
      "NQE map bulk edit should offer raw query restore",
    );
    await expectVisible(page, "Forward Source for Query Lookup");
    await expectVisible(page, "Query Repository");
    await expectVisible(page, "Repository Folder");
    await expectVisible(page, "Overwrite existing repository queries");
    await expectVisible(page, "Commit message");
    await expectVisible(page, "Map Query Path Choices");
    await expectVisible(page, "Forward Locations");
    await expectVisible(page, "Pin current commit");
    await expectVisible(page, "Repository-path operations clear direct query IDs");
    await expectVisible(page, "resolved at sync time");
    await assertNoHorizontalOverflow(page, "desktop NQE map list");
    evidence.checks.push(
      "native NQE map bulk edit exposes bidirectional query reference controls",
    );

    await page.setViewportSize({ width: 390, height: 900 });
    await page.goto(`${baseURL}/plugins/forward/sync/`, {
      waitUntil: "domcontentloaded",
    });
    await expectVisible(page, "Forward Syncs");
    await expectVisible(page, "ui-harness-sync");
    await assertNoHorizontalOverflow(page, "mobile sync list");
    evidence.screenshots.push(await screenshot(page, "mobile-sync-list.jpg"));
    evidence.checks.push("mobile sync list fits without horizontal overflow");

    await page.getByRole("link", { name: "ui-harness-sync" }).first().click();
    await page.getByRole("link", { name: "Drift Report", exact: true }).click();
    await expectVisible(page, "Latest Sync Evidence");
    await expectVisible(page, "Not measured");
    await assertNoHorizontalOverflow(page, "mobile drift report");
    evidence.screenshots.push(await screenshot(page, "mobile-drift-report.jpg"));
    evidence.checks.push("mobile drift report fits without horizontal overflow");

    writeFileSync(
      path.join(artifactDir, "forward-ui-summary.json"),
      `${JSON.stringify(evidence, null, 2)}\n`,
      "utf8",
    );
    console.log(JSON.stringify(evidence, null, 2));
  } finally {
    await context.close();
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
