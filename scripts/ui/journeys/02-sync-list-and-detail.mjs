import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "sync-list-and-detail",
  title: 'sync list and detail render the seeded fixture',
  async run({ evidence }) {
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
  },
};
