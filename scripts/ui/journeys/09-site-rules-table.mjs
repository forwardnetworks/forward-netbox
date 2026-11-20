import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, assertNoHorizontalOverflow,
  open, screenshot, baseURL,
} = h;

const EDIT = `${baseURL}/plugins/forward/source/1/edit/`;
const row = (n) => `tr[data-rule]:nth-of-type(${n})`;

async function openForm() {
  open(EDIT);
  await waitFor(`document.querySelector('[data-forward-site-rule-table]') !== null`, "rules table");
}

// Rows as "kind|value|site" strings: the browser tool returns object keys sorted,
// so compare an order that cannot change.
const rowsOf = () =>
  evaluate(
    `Array.from(document.querySelectorAll('tr[data-rule]')).map((tr) => [` +
      `tr.querySelector('[data-rule-kind]').value,` +
      `tr.querySelector('[data-value]').value,` +
      `tr.querySelector('[data-rule-site]').value].join('|'))`,
  );
// Click Save in the page: the sticky NetBox navbar can cover a pointer click.
const save = () =>
  evaluate(
    `Array.from(document.querySelectorAll('form button[type="submit"]')).find(` +
      `(b) => b.textContent.trim() === 'Save').click(), true`,
  );

// The rules as a table of rows with pickers, in a real NetBox page, against a Forward
// stand-in that has 300 tags and 404 locations.
export default {
  id: "site-rules-table",
  title: "site mapping rules table: pickers, suggestions, round trips",
  async run({ evidence }) {
    await openForm();

    // NetBox wraps plain selects in its own dropdown at page load; that wrapper
    // keeps only the options it saw then, so Forward's tags never reached it.
    assert(
      evaluate(`document.querySelectorAll('[data-forward-site-rule-table] .ts-wrapper, [data-forward-site-rule-builder] .ts-wrapper').length`) === 0,
      "NetBox wrapped a rule select in its own dropdown",
    );
    assert(evaluate(`document.querySelector('[data-forward-site-rule-table]').hidden`) === false, "the rules table is hidden");
    assert((await rowsOf()).length === 0, "a source with no rules should start with no rows");

    // A location row: typed pattern, Forward's location names offered, free text kept.
    browser(["click", "[data-forward-site-rule-table] [data-add-row]"]);
    browser(["fill", `${row(1)} [data-value]`, "DC01*"]);
    browser(["fill", `${row(1)} [data-rule-site]`, "dc1"]);
    await waitFor(
      `document.querySelectorAll('#id_site_rules_locations option').length === 404`,
      "all 404 Forward location names offered as suggestions",
    );

    // A tag row: the select lists every tag Forward has, loaded when first used.
    browser(["click", "[data-forward-site-rule-table] [data-add-row]"]);
    browser(["select", `${row(2)} [data-rule-kind]`, "tag"]);
    await waitFor(`document.querySelector('${row(2)} select[data-value]') !== null`, "tag select");
    evaluate(`document.querySelector('${row(2)} select[data-value]').dispatchEvent(new Event('mousedown')), true`);
    await waitFor(
      `document.querySelector('${row(2)} select[data-value]').options.length === 301`,
      "the tag select to list Forward's 300 tags (plus the blank choice)",
    );
    browser(["select", `${row(2)} select[data-value]`, "Tag-007"]);
    browser(["fill", `${row(2)} [data-rule-site]`, "dc2"]);

    const added = await rowsOf();
    assert(
      JSON.stringify(added) === JSON.stringify(["location|DC01*|dc1", "tag|Tag-007|dc2"]),
      `rows were ${JSON.stringify(added)}`,
    );
    assert(
      evaluate(`document.getElementById('id_site_rules').value`) ===
        "location DC01* -> dc1\ntag Tag-007 -> dc2\n",
      "the posted text did not follow the rows",
    );
    assertNoHorizontalOverflow("source form with rules table");
    evidence.screenshots.push(screenshot("site-rules-table.jpg", { fullPage: false }));
    evidence.checks.push(
      "rules table: location and tag rows from Forward's names (300 tags, 404 locations), and the posted text follows the rows",
    );

    // A half-filled row stops the save instead of being dropped.
    browser(["click", "[data-forward-site-rule-table] [data-add-row]"]);
    browser(["fill", `${row(3)} [data-rule-site]`, "only-a-site"]);
    save();
    await waitFor(
      `document.querySelector('[data-table-status]').textContent.includes('needs both')`,
      "the half-filled row to stop the save",
    );
    assert(new URL(browser(["get", "url"]).trim()).pathname.endsWith("/edit/"), "a half-filled row let the form submit");
    // And Edit as text refuses while it is half filled.
    browser(["click", "[data-forward-site-rule-table] [data-edit-text]"]);
    assert(evaluate(`document.querySelector('[data-forward-site-rule-table]').hidden`) === false, "Edit as text discarded a half-filled row");
    browser(["click", `${row(3)} [data-remove]`]);
    evidence.checks.push("rules table: a half-filled row blocks the save and Edit as text");

    // Edit as text and back: the same rules, in the one-per-line form.
    browser(["click", "[data-forward-site-rule-table] [data-edit-text]"]);
    await waitFor(`document.getElementById('id_site_rules').hidden === false`, "the text editor");
    browser(["fill", "#id_site_rules", "location DC02* -> dc2\nlocation *roseland* -> roseland\n"]);
    browser(["click", "[data-show-table]"]);
    await waitFor(`document.querySelectorAll('tr[data-rule]').length === 2`, "two rows from the text");
    assert(
      JSON.stringify(await rowsOf()) ===
        JSON.stringify(["location|DC02*|dc2", "location|*roseland*|roseland"]),
      "text did not become the same rows",
    );
    evidence.checks.push("rules table: Edit as text and Show as table round-trip");

    // A text box line the table cannot show leaves the text editor in charge.
    browser(["click", "[data-forward-site-rule-table] [data-edit-text]"]);
    browser(["fill", "#id_site_rules", "this is not a rule\n"]);
    open(EDIT);
    await waitFor(`document.querySelector('[data-forward-site-rule-table]') !== null`, "reloaded form");
    assert((await rowsOf()).length === 0, "unsaved edits survived a reload");
    evidence.checks.push("rules table: reloading the form restores the saved rules, not unsaved edits");
  },
};
