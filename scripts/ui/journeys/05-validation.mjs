import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "validation",
  title: 'validation run list and detail render',
  async run({ evidence }) {
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
  },
};
