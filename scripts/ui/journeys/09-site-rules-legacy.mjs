import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "site-rules-legacy",
  title: 'site mapping rules field and builder (to be replaced)',
  async run({ evidence }) {
    open(`${baseURL}/plugins/forward/source/1/edit/`);
    const siteRules = evaluate(
      `(() => {` +
        `const area = document.getElementById("id_site_rules");` +
        `const builder = document.querySelector("[data-forward-site-rule-builder]");` +
        `if (!area || !builder) { return {ok: false}; }` +
        `builder.querySelector("[data-location]").value = "HQ_*";` +
        `builder.querySelector("[data-site]").value = "hq-campus";` +
        `builder.querySelector("[data-add]").click();` +
        `const table = document.querySelector("[data-forward-site-rule-table]");` +
        `const rows = table ? table.querySelectorAll("tr[data-rule]").length : -1;` +
        `const rowSite = table && table.querySelector("tr[data-rule] [data-rule-site]");` +
        `if (rowSite) { rowSite.value = "hq-renamed"; rowSite.dispatchEvent(new Event("input")); }` +
        `const afterEdit = area.value;` +
        `const removeBtn = table && table.querySelector("tr[data-rule] [data-remove]");` +
        `if (removeBtn) { removeBtn.click(); }` +
        `const afterRemove = area.value;` +
        `const tableVisible = !!table && table.getClientRects().length > 0;` +
        `builder.querySelector("[data-location]").value = "HQ_*";` +
        `builder.querySelector("[data-site]").value = "hq-campus";` +
        `builder.querySelector("[data-add]").click();` +
        `table.querySelector("[data-add-row]").click();` +
        `const wrapped = document.querySelectorAll('[data-forward-site-rule-table] .ts-wrapper, [data-forward-site-rule-builder] .ts-wrapper').length;` +
        `const rowCount = table.querySelectorAll("tr[data-rule]").length;` +
        `const half = table.querySelectorAll("tr[data-rule]")[table.querySelectorAll("tr[data-rule]").length - 1];` +
        `half.querySelector("[data-rule-site]").value = "only-a-site";` +
        `half.querySelector("[data-rule-site]").dispatchEvent(new Event("input"));` +
        `const submit = new Event("submit", {cancelable: true, bubbles: true});` +
        `area.form.dispatchEvent(submit);` +
        `const blocked = submit.defaultPrevented;` +
        `table.querySelector("[data-edit-text]").click();` +
        `const stayedInTable = !table.hidden;` +
        `half.querySelector("[data-remove]").click();` +
        `table.querySelector("[data-edit-text]").click();` +
        `const inText = table.hidden && !area.hidden;` +
        `const back = document.querySelector("[data-show-table]");` +
        `if (back) { back.click(); }` +
        `const backInTable = !table.hidden && !document.querySelector("[data-show-table]");` +
        `const kind = builder.querySelector("[data-kind]");` +
        `kind.value = "tag";` +
        `kind.dispatchEvent(new Event("change"));` +
        `return {` +
          `ok: true,` +
          `value: area.value,` +
          `rows, afterEdit, afterRemove, tableVisible, rowCount, wrapped, blocked, stayedInTable, inText, backInTable,` +
          `visible: tableVisible || area.getClientRects().length > 0,` +
          `help: document.body.innerText.includes("unless a mapping applies"),` +
          `tagControl: !!builder.querySelector("[data-tag]"),` +
          `locationList: builder.querySelector("[data-location]").getAttribute("list"),` +
          `locationOptions: !!document.getElementById(` +
            `builder.querySelector("[data-location]").getAttribute("list")),` +
          `status: builder.querySelector("[data-status]").textContent.trim(),` +
        `};` +
      `})()`,
    );
    assert(siteRules.ok, "source form has no site mapping rules field and builder");
    assert(siteRules.visible, "source form hides the site mapping rules field");
    assert(siteRules.help, "source form does not say the Forward location is the default");
    assert(
      siteRules.value.includes("location HQ_* -> hq-campus"),
      `the rule builder did not add a line (field: ${siteRules.value})`,
    );
    assert(siteRules.tableVisible, "the site mapping rules are not shown as a table");
    assert(siteRules.rows === 1, `the table did not show the added rule as a row (${siteRules.rows})`);
    assert(
      siteRules.afterEdit.includes("location HQ_* -> hq-renamed"),
      `editing a row did not rewrite the field (${siteRules.afterEdit})`,
    );
    assert(siteRules.afterRemove.trim() === "", "removing the row did not clear the field");
    assert(
      siteRules.wrapped === 0,
      `NetBox wrapped ${siteRules.wrapped} of the rule selects in its own dropdown, so Forward's tags cannot reach them`,
    );
    assert(siteRules.rowCount === 2, "Add row did not add a row");
    assert(siteRules.blocked, "a half-filled rule row did not stop the form from submitting");
    assert(siteRules.stayedInTable, "Edit as text discarded a half-filled row");
    assert(siteRules.inText && siteRules.backInTable, "Edit as text / Show as table did not round-trip");
    assert(siteRules.tagControl, "the rule builder has no tag control for a tag rule");
    assert(
      siteRules.locationList && siteRules.locationOptions,
      "the location field has no suggestion list for Forward's location names",
    );
    assert(siteRules.status.length > 0, "the rule builder said nothing about loading tags");
    assertNoHorizontalOverflow("desktop source form");
    evidence.screenshots.push(screenshot("desktop-source-form.jpg"));
    evidence.checks.push(
      "source form shows the site mapping rules field, says the Forward location is the default, and its builder adds a rule and asks Forward for tags",
    );
  },
};
