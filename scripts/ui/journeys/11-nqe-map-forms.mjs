import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "nqe-map-forms",
  title: 'NQE map forms and bulk edit',
  async run({ evidence }) {
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
  },
};
