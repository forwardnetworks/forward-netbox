import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "ingestion-detail",
  title: 'ingestion detail renders progress, statistics, metrics and logs',
  async run({ evidence }) {
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
  },
};
