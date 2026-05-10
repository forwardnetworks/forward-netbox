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

const dockerComposeArgs = [
  "--project-name",
  "forward-netbox",
  "--project-directory",
  "development",
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

async function screenshot(page, name) {
  const target = path.join(artifactDir, name);
  await page.screenshot({ path: target, fullPage: true, type: "jpeg", quality: 85 });
  return target;
}

async function main() {
  mkdirSync(artifactDir, { recursive: true });
  if (process.env.PLAYWRIGHT_SKIP_MIGRATE !== "true") {
    runDockerManage("migrate --noinput");
  }
  runDockerManage("forward_seed_ui_harness");

  const browser = await chromium.launch({
    headless: process.env.PLAYWRIGHT_HEADLESS !== "false",
  });
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
    await page.goto(`${baseURL}/plugins/forward/sync/`);
    await page.waitForURL(/\/login\/\?next=/);
    evidence.checks.push("unauthenticated sync list redirects to login");

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
    await expectVisible(page, "ui-harness-drift-policy");
    await expectVisible(page, "latestProcessed");
    await expectVisible(page, "max_changes_per_branch");
    await expectVisible(page, "Current activity");
    await expectVisible(page, "Resolving snapshot, running query preflight, and building shard plan.");
    await assertNoHorizontalOverflow(page, "desktop sync detail");
    evidence.screenshots.push(await screenshot(page, "desktop-sync-detail.jpg"));
    evidence.checks.push(
      "sync detail exposes validation, native branch budget, run controls, and preflight activity",
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
    await expectVisible(page, "Validation");
    await expectVisible(page, "Model Results");
    await expectVisible(page, "Sync Results");
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
    await expectVisible(page, "Max changes per branch");
    await expectVisible(page, "Auto merge");
    await assertNoHorizontalOverflow(page, "desktop sync form");
    evidence.checks.push("sync creation form exposes branch budget and auto merge controls");

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
      bulkOperationOptions.includes("Bind selected maps to repository query paths"),
      "NQE map bulk edit should offer repository path binding",
    );
    assert(
      bulkOperationOptions.includes("Publish bundled queries to Org Repository and bind selected maps"),
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
    await expectVisible(page, "Map Query Choices");
    await expectVisible(page, "Forward Locations");
    await expectVisible(page, "Pin current commit");
    await expectVisible(page, "resolves query IDs from selected query paths during sync");
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
