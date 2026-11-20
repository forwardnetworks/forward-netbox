import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "sync-form",
  title: 'sync form exposes execution controls',
  async run({ evidence }) {
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
  },
};
