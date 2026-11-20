// Shared helpers for the agent-browser UI harness: every UI check in this repo
// drives a real browser through the `agent-browser` CLI using these.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";

const repoRoot = path.resolve(import.meta.dirname, "../..");
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
  // A timeout with no picture of the page is hard to act on: say where the
  // browser was and what it was showing, and keep a screenshot.
  let where = "page state unavailable";
  try {
    const state = evaluate(
      `({url: location.href, title: document.title, text: document.body.innerText.slice(0, 600)})`,
    );
    where = `url=${state.url} title=${js(state.title)} text=${js(state.text)}`;
    browser(["screenshot", path.join(artifactDir, "failure.jpg")]);
  } catch (error) {
    where = `page state unavailable: ${error.message}`;
  }
  throw new Error(
    `timed out waiting for ${description} (last: ${last}); ${where}`,
  );
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

// Click the first visible link whose text matches (substring unless exact). The
// click happens in the page, on the element itself, so it never lands on a
// different link that happens to share part of the name.
async function clickLink(name, { exact = false } = {}) {
  const matches = `(a) => { const t = a.textContent.trim(); ` +
    `return ${exact ? `t === ${js(name)}` : `t.includes(${js(name)})`} ` +
    `&& a.getClientRects().length > 0; }`;
  await waitFor(
    `Array.from(document.querySelectorAll("a")).some(${matches})`,
    `link ${js(name)}`,
  );
  evaluate(
    `Array.from(document.querySelectorAll("a")).find(${matches}).click(), true`,
  );
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

export const h = {
  repoRoot, artifactDir, baseURL, username, password, assert, sleep, browser,
  evaluate, waitFor, js, expectVisible, countExactText, assertNoHorizontalOverflow,
  currentURL, open, clickLink, clickLinkMatching, clickHref, login, screenshot,
  waitForWebReady, runDockerManage, sessionName,
};
