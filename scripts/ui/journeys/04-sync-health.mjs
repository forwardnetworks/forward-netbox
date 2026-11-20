import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "sync-health",
  title: 'sync health tab renders diagnostics and query publishing',
  async run({ evidence }) {
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
  },
};
