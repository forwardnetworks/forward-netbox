import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "drift-report",
  title: 'drift report separates workload estimates from convergence evidence',
  async run({ evidence }) {
    open(`${baseURL}/plugins/forward/sync/`);
    await clickLink("ui-harness-sync");
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
  },
};
