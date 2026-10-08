#!/usr/bin/env node
// Deterministic UI harness for the Forward NetBox plugin, driven by the
// `agent-browser` CLI (Chrome over CDP). Every check either passes or throws;
// the first failure exits non-zero, so `invoke ui-test` is a real gate.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";

const repoRoot = path.resolve(import.meta.dirname, "..");
const artifactDir = path.resolve(
  repoRoot,
  process.env.FORWARD_UI_ARTIFACT_DIR || ".ui-artifacts",
);
const baseURL = (process.env.NETBOX_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
const username = process.env.NETBOX_USERNAME || "admin";
const password = process.env.NETBOX_PASSWORD || "admin";
const dockerProjectName =
  process.env.FORWARD_UI_DOCKER_PROJECT_NAME || "forward-netbox";
const dockerProjectDirectory =
  process.env.FORWARD_UI_DOCKER_PROJECT_DIRECTORY || "development";
const isolatedHarness = process.env.FORWARD_UI_HARNESS_ISOLATED === "true";
const agentBrowserBin = path.join(
  repoRoot,
  "node_modules",
  ".bin",
  "agent-browser",
);
const sessionName = `forward-ui-harness-${process.pid}`;
const stepTimeoutMs = 25000;

if (!isolatedHarness) {
  throw new Error(
    "The UI harness may only run through the isolated " +
      "`invoke ui-test` runtime.",
  );
}

if (!existsSync(agentBrowserBin)) {
  throw new Error(
    "agent-browser is not installed; run `npm ci` before `invoke ui-test`.",
  );
}

const dockerComposeArgs = [
  "--project-name",
  dockerProjectName,
  "--project-directory",
  dockerProjectDirectory,
  "exec",
  "-T",
  "--env",
  "FORWARD_UI_HARNESS_ISOLATED=true",
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// One `agent-browser` call. A non-zero exit throws with the CLI's own message.
function browser(args, { json = false } = {}) {
  const fullArgs = json ? [...args, "--json"] : args;
  try {
    return execFileSync(agentBrowserBin, fullArgs, {
      cwd: repoRoot,
      env: {
        ...process.env,
        AGENT_BROWSER_SESSION: sessionName,
        AGENT_BROWSER_SCREENSHOT_FORMAT: "jpeg",
        AGENT_BROWSER_SCREENSHOT_QUALITY: "85",
      },
      encoding: "utf8",
      timeout: stepTimeoutMs * 2,
      stdio: ["ignore", "pipe", "pipe"],
    });
  } catch (error) {
    const detail = `${error.stdout ?? ""}${error.stderr ?? ""}`.trim();
    throw new Error(
      `agent-browser ${args.join(" ")} failed: ${detail || error.message}`,
    );
  }
}

// Evaluate JavaScript in the page and return its JSON-serialisable result.
function evaluate(expression) {
  const parsed = JSON.parse(browser(["eval", expression], { json: true }));
  assert(parsed.success, `eval failed: ${parsed.error}`);
  return parsed.data.result;
}

// Poll an in-page predicate until it is truthy or the step times out.
async function waitFor(expression, description) {
  const deadline = Date.now() + stepTimeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      last = evaluate(expression);
      if (last) return last;
    } catch (error) {
      last = error.message;
    }
    await sleep(250);
  }
  throw new Error(`timed out waiting for ${description} (last: ${last})`);
}

const js = (value) => JSON.stringify(value);

// Visible rendered text, case-insensitive: the NetBox theme uppercases some
// headings with CSS, which `innerText` reflects but the DOM source does not.
async function expectVisible(text) {
  await waitFor(
    `document.body.innerText.toLowerCase().includes(${js(text.toLowerCase())})`,
    `visible text ${js(text)}`,
  );
}

function countExactText(text) {
  return evaluate(
    `Array.from(document.querySelectorAll("body *")).filter(` +
      `(el) => el.children.length === 0 && el.textContent.trim() === ${js(text)}` +
      `).length`,
  );
}

function assertNoHorizontalOverflow(label) {
  const overflow = evaluate(
    `Math.max(0, document.documentElement.scrollWidth - document.documentElement.clientWidth)`,
  );
  assert(overflow <= 4, `${label} has horizontal overflow of ${overflow}px`);
}

function currentURL() {
  return new URL(browser(["get", "url"]).trim());
}

function open(url) {
  browser(["open", url]);
}

// Click a visible link by its accessible name (substring unless exact).
async function clickLink(name, { exact = false } = {}) {
  await waitFor(
    `Array.from(document.querySelectorAll("a")).some((a) => {` +
      `const t = a.textContent.trim();` +
      `return ${exact ? `t === ${js(name)}` : `t.includes(${js(name)})`} && a.getClientRects().length > 0;` +
      `})`,
    `link ${js(name)}`,
  );
  browser(["find", "role", "link", "click", "--name", name, ...(exact ? ["--exact"] : [])]);
}

async function clickLinkMatching(pattern) {
  await waitFor(
    `Array.from(document.querySelectorAll("a")).some((a) =>` +
      ` new RegExp(${js(pattern)}).test(a.textContent.trim()) && a.getClientRects().length > 0)`,
    `link matching ${pattern}`,
  );
  evaluate(
    `Array.from(document.querySelectorAll("a")).find((a) =>` +
      ` new RegExp(${js(pattern)}).test(a.textContent.trim())).click(), true`,
  );
}

async function clickHref(selector) {
  await waitFor(
    `document.querySelector(${js(selector)}) !== null`,
    `element ${selector}`,
  );
  evaluate(`document.querySelector(${js(selector)}).click(), true`);
}

async function login() {
  open(`${baseURL}/login/?next=/plugins/forward/sync/`);
  await waitFor(`document.querySelector('input[name="username"]') !== null`, "login form");
  browser(["fill", 'input[name="username"]', username]);
  browser(["fill", 'input[name="password"]', password]);
  browser(["find", "role", "button", "click", "--name", "Sign In"]);
  await waitFor(
    `/\\/plugins\\/forward\\/sync\\/?$/.test(location.pathname)`,
    "post-login redirect to the sync list",
  );
}

function screenshot(name, { fullPage = true } = {}) {
  const target = path.join(artifactDir, name);
  browser(["screenshot", target, ...(fullPage ? ["--full"] : [])]);
  assert(existsSync(target), `screenshot ${name} was not written`);
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
    await sleep(1000);
  }
  throw new Error(
    `web server not ready at ${url} within ${timeoutMs}ms (last: ${lastErr})`,
  );
}

async function main() {
  mkdirSync(artifactDir, { recursive: true });
  if (process.env.FORWARD_UI_SKIP_MIGRATE !== "true") {
    runDockerManage("migrate --noinput");
  }
  runDockerManage("forward_seed_ui_harness");
  await waitForWebReady(`${baseURL}/login/`);

  const evidence = {
    baseURL,
    screenshots: [],
    checks: [],
  };

  try {
    browser(["set", "viewport", "1440", "1000"]);

    // The status code needs no browser: an anonymous request must be bounced
    // to the login page or rejected outright.
    const anonymous = await fetch(`${baseURL}/plugins/forward/sync/`, {
      redirect: "manual",
    });
    const location = anonymous.headers.get("location") ?? "";
    const bouncedToLogin =
      [301, 302, 303, 307, 308].includes(anonymous.status) &&
      location.includes("/login/") &&
      location.includes("next=%2Fplugins%2Fforward%2Fsync%2F");
    assert(
      bouncedToLogin || anonymous.status === 401,
      `unauthenticated sync list did not require authentication ` +
        `(status=${anonymous.status}, location=${location})`,
    );
    open(`${baseURL}/plugins/forward/sync/`);
    const unauthenticatedURL = currentURL();
    assert(
      (unauthenticatedURL.pathname === "/login/" &&
        unauthenticatedURL.searchParams.get("next") ===
          "/plugins/forward/sync/") ||
        anonymous.status === 401,
      `unauthenticated browser visit was not sent to login (${unauthenticatedURL})`,
    );
    evidence.checks.push("unauthenticated sync list requires authentication");

    await login();
    await expectVisible("Forward Syncs");
    await expectVisible("ui-harness-sync");
    assertNoHorizontalOverflow("desktop sync list");
    evidence.checks.push("sync list renders seeded fixture");

    await clickLink("ui-harness-sync");
    await expectVisible("Sync Information");
    await expectVisible("Enabled Models");
    await expectVisible("Adhoc Ingestion");
    await expectVisible("Validate");
    await expectVisible("Drift Policy");
    await expectVisible("Latest Validation");
    await expectVisible("Workload Preview");
    await expectVisible("Analysis Summary");
    await expectVisible("Advisory Summary");
    await expectVisible("Export Support Bundle");
    await expectVisible("Health");
    await expectVisible("ui-harness-drift-policy");
    await expectVisible("latestProcessed");
    await expectVisible("max_changes_per_staging_item");
    await expectVisible("Current activity");
    assertNoHorizontalOverflow("desktop sync detail");
    evidence.screenshots.push(screenshot("desktop-sync-detail.jpg"));
    evidence.checks.push(
      "sync detail exposes validation, single-branch run controls, support export, and current activity",
    );

    await clickLink("Drift Report", { exact: true });
    await expectVisible("Drift Report");
    await expectVisible("Latest Sync Evidence");
    await expectVisible("Not confirmed");
    await expectVisible("Same as preview");
    await expectVisible("Run this sync again against the same snapshot");
    await expectVisible("Not measured");
    assertNoHorizontalOverflow("desktop drift report");
    evidence.screenshots.push(screenshot("desktop-drift-report.jpg"));
    evidence.checks.push(
      "drift report distinguishes workload estimates from same-snapshot convergence evidence",
    );

    open(`${baseURL}/plugins/forward/sync/`);
    await clickLink("ui-harness-sync");

    await clickHref('a[href*="/sync/"][href$="/health/"]');
    await expectVisible("Health Summary");
    await expectVisible("Export Live Source Check");
    await expectVisible("Query Binding");
    await expectVisible("Local Query Drift");
    await expectVisible("Export Live Query Drift Check");
    await expectVisible("Export Live Data File Check");
    await expectVisible("Publish Bundled Queries");
    assert(
      countExactText("Refresh Query IDs") === 0,
      "sync health should not expose the retired Refresh Query IDs action",
    );
    await expectVisible("Forward API Usage");
    await expectVisible("Dependency Lookup Cache");
    await expectVisible("Density Learning");
    await expectVisible("Ownership finalization");
    await expectVisible("Diff-capable maps");
    await expectVisible("Next run");
    await expectVisible("Health Details");
    assertNoHorizontalOverflow("desktop sync health");
    evidence.screenshots.push(
      screenshot("desktop-sync-health.jpg", { fullPage: false }),
    );
    evidence.checks.push(
      "sync health tab renders local diagnostics, live query-path publishing, and explicit live source/query/data-file exports without the retired refresh action",
    );

    open(`${baseURL}/plugins/forward/validation-run/`);
    await expectVisible("Forward Validation Runs");
    await expectVisible("ui-harness-sync");
    await expectVisible("Passed");
    assertNoHorizontalOverflow("desktop validation run list");
    evidence.checks.push("validation run list renders seeded validation records");

    open(`${baseURL}/plugins/forward/sync/`);
    await clickLink("ui-harness-sync");
    await clickLink("Passed");
    await expectVisible("Validation Run");
    await expectVisible("Drift Summary");
    await expectVisible("Model Results");
    await expectVisible("ui-harness-sync");
    assertNoHorizontalOverflow("desktop validation detail");
    evidence.screenshots.push(screenshot("desktop-validation-detail.jpg"));
    evidence.checks.push("validation detail renders drift summary and model results");

    open(`${baseURL}/plugins/forward/sync/`);
    await clickLink("ui-harness-sync");
    await clickLinkMatching("ui-harness-sync \\(Ingestion \\d+\\)");
    await expectVisible("Ingestion Information");
    await expectVisible("Progress");
    await expectVisible("Statistics");
    await expectVisible("Forward Snapshot Metrics");
    await expectVisible("Workload Preview");
    await expectVisible("Analysis Summary");
    await expectVisible("Advisory Summary");
    await expectVisible("Validation");
    await expectVisible("Model Results");
    await expectVisible("Sync Results");
    await expectVisible("Export Logs");
    await expectVisible("Synthetic UI harness ingestion completed.");
    assertNoHorizontalOverflow("desktop ingestion detail");
    evidence.screenshots.push(screenshot("desktop-ingestion-detail.jpg"));
    evidence.checks.push("ingestion detail renders progress, statistics, metrics, and logs");

    open(`${baseURL}/plugins/forward/sync/add/`);
    await expectVisible("Forward Sync");
    await expectVisible("Model Selection");
    await expectVisible("Execution");
    await expectVisible("Drift policy");
    await expectVisible("Auto merge");
    await expectVisible("Use safe bulk ORM models");
    await expectVisible("Diff fallback mode");
    assertNoHorizontalOverflow("desktop sync form");
    evidence.checks.push(
      "sync creation form exposes single-branch merge, apply-engine, and diff fallback controls",
    );

    open(`${baseURL}/plugins/forward/source/1/edit/`);
    assert(
      currentURL().pathname === "/plugins/forward/source/1/edit/",
      `source edit form redirected to ${currentURL()}`,
    );
    const sourceTitle = evaluate("document.title");
    assert(
      !/not found|server error|forbidden/i.test(sourceTitle),
      `source edit form did not return a success response (title: ${sourceTitle})`,
    );
    await expectVisible("Forward Source");
    for (const fieldLabel of [
      "Apply Device Scope Tags",
      "Import SNMP Endpoints as Devices",
      "Import Generic SNMP Endpoints as Devices",
      "Scope SNMP Endpoints by Include Tags",
    ]) {
      const fields = evaluate(
        `Array.from(document.querySelectorAll("label")).filter(` +
          `(label) => label.textContent.trim() === ${js(fieldLabel)}` +
          `).map((label) => {` +
          `const control = label.control;` +
          `return control ? control.getClientRects().length > 0 : null;` +
          `})`,
      );
      assert(fields.length === 1, `source form is missing ${fieldLabel}`);
      assert(fields[0] === true, `source form hides ${fieldLabel}`);
    }
    assertNoHorizontalOverflow("desktop source form");
    evidence.screenshots.push(screenshot("desktop-source-form.jpg"));
    evidence.checks.push(
      "source form exposes scope tags, console-server import, generic endpoint opt-in, and endpoint include scope",
    );

    open(`${baseURL}/plugins/forward/nqe-map/add/`);
    await expectVisible("Forward NQE Map");
    await expectVisible("Query Definition Mode");
    await expectVisible("Forward Source for Query Lookup");
    await expectVisible("Query Repository");
    await expectVisible("Query Folder");
    await expectVisible("Query Path");
    await expectVisible("Query ID");
    await expectVisible("Commit ID");
    const queryModeTag = evaluate(
      `document.querySelector('[name="query_mode"]').tagName`,
    );
    assert(queryModeTag === "SELECT", "NQE map query mode should render as a select");
    assert(
      evaluate(`document.querySelectorAll('input[type="radio"][name="query_mode"]').length`) === 0,
      "NQE map query mode should not render as mis-styled radio inputs",
    );
    assertNoHorizontalOverflow("desktop NQE map form");
    evidence.checks.push("NQE map form exposes repository path, direct query ID, and commit selectors");

    open(`${baseURL}/plugins/forward/nqe-map/`);
    await expectVisible("Forward NQE Maps");
    await waitFor(`document.querySelector('input[name="pk"]') !== null`, "row checkbox");
    evaluate(`document.querySelector('input[name="pk"]').click(), true`);
    await waitFor(
      `document.querySelector('input[name="pk"]').checked`,
      "row checkbox to be checked",
    );
    browser(["find", "role", "button", "click", "--name", "Edit Selected"]);
    await expectVisible("Bulk Edit");
    await expectVisible("Bulk Query Reference");
    await expectVisible("Query Bulk Operation");
    const bulkOperationOptions = evaluate(
      `Array.from(document.querySelectorAll('[name="query_bulk_operation"] option')).map((o) => o.textContent.trim())`,
    );
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
    await expectVisible("Forward Source for Query Lookup");
    await expectVisible("Query Repository");
    await expectVisible("Repository Folder");
    await expectVisible("Overwrite existing repository queries");
    await expectVisible("Commit message");
    await expectVisible("Map Query Path Choices");
    await expectVisible("Forward Locations");
    await expectVisible("Pin current commit");
    await expectVisible(
      "Repository selection resolves and saves each published query ID",
    );
    // The sync-time wording lives in a <select> option label, which is not
    // rendered page text; the control itself is asserted above via
    // "Query Bulk Operation".
    assertNoHorizontalOverflow("desktop NQE map list");
    evidence.checks.push(
      "native NQE map bulk edit exposes bidirectional query reference controls",
    );

    browser(["set", "viewport", "390", "900"]);
    open(`${baseURL}/plugins/forward/sync/`);
    await expectVisible("Forward Syncs");
    await expectVisible("ui-harness-sync");
    assertNoHorizontalOverflow("mobile sync list");
    evidence.screenshots.push(screenshot("mobile-sync-list.jpg"));
    evidence.checks.push("mobile sync list fits without horizontal overflow");

    await clickLink("ui-harness-sync");
    await clickLink("Drift Report", { exact: true });
    await expectVisible("Latest Sync Evidence");
    await expectVisible("Not measured");
    assertNoHorizontalOverflow("mobile drift report");
    evidence.screenshots.push(screenshot("mobile-drift-report.jpg"));
    evidence.checks.push("mobile drift report fits without horizontal overflow");

    writeFileSync(
      path.join(artifactDir, "forward-ui-summary.json"),
      `${JSON.stringify(evidence, null, 2)}\n`,
      "utf8",
    );
    console.log(JSON.stringify(evidence, null, 2));
  } finally {
    try {
      browser(["close"]);
    } catch (error) {
      console.error(`agent-browser close failed: ${error.message}`);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
