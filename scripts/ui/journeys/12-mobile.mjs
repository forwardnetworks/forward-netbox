import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "mobile",
  title: 'mobile layouts fit without horizontal overflow',
  async run({ evidence }) {
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
  },
};
