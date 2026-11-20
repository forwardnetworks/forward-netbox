import { h } from "../lib.mjs";

const { assert, browser, evaluate, waitFor, open, baseURL } = h;

const EDIT = `${baseURL}/plugins/forward/source/1/edit/`;
// Click Save in the page: the sticky NetBox navbar can cover a pointer click.
const save = () =>
  evaluate(
    `Array.from(document.querySelectorAll('form button[type="submit"]')).find(` +
      `(b) => b.textContent.trim() === 'Save').click(), true`,
  );
const row = (n) => `tr[data-rule]:nth-of-type(${n})`;

// After Save: wait for the saved page, or fail with whatever the form said.
async function saved() {
  await waitFor(
    `!location.pathname.endsWith('/edit/') || document.querySelector('.alert-danger, .invalid-feedback')`,
    "the form to be saved or to say why not",
  );
  const stayed = evaluate(`location.pathname.endsWith('/edit/')`);
  if (stayed) {
    const said = evaluate(
      `Array.from(document.querySelectorAll('.alert-danger, .invalid-feedback')).map((e) => e.innerText.trim()).join(' | ')`,
    );
    throw new Error(`the form did not save: ${said}`);
  }
}

async function openForm() {
  open(EDIT);
  await waitFor(`document.querySelector('[data-forward-site-rule-table]') !== null`, "rules table");
}

const rows = () =>
  evaluate(
    `Array.from(document.querySelectorAll('tr[data-rule]')).map((tr) => [` +
      `tr.querySelector('[data-rule-kind]').value,` +
      `tr.querySelector('[data-value]').value,` +
      `tr.querySelector('[data-rule-site]').value].join('|'))`,
  );

// Saving through the real form (the source test reaches the Forward stand-in), then
// reading the rules back after a reload, an edit, and a rejected rule.
export default {
  id: "site-rules-save",
  title: "site mapping rules save, reload, reject and clear",
  async run({ evidence }) {
    await openForm();
    browser(["click", "[data-forward-site-rule-table] [data-add-row]"]);
    browser(["fill", `${row(1)} [data-value]`, "DC01*"]);
    browser(["fill", `${row(1)} [data-rule-site]`, "dc1"]);
    browser(["click", "[data-forward-site-rule-table] [data-add-row]"]);
    browser(["select", `${row(2)} [data-rule-kind]`, "tag"]);
    evaluate(`document.querySelector('${row(2)} select[data-value]').dispatchEvent(new Event('mousedown')), true`);
    await waitFor(`document.querySelector('${row(2)} select[data-value]').options.length === 301`, "tags");
    browser(["select", `${row(2)} select[data-value]`, "Tag-042"]);
    browser(["fill", `${row(2)} [data-rule-site]`, "dc2"]);
    save();
    await saved();

    await openForm();
    assert(
      JSON.stringify(await rows()) === JSON.stringify(["location|DC01*|dc1", "tag|Tag-042|dc2"]),
      `saved rules came back as ${JSON.stringify(await rows())}`,
    );
    assert(
      evaluate(`document.querySelector('tr[data-rule]:nth-of-type(2) select[data-value]').value`) === "Tag-042",
      "the saved tag is not the selected option",
    );
    evidence.checks.push("rules save through the source form and come back as the same rows after a reload");

    // A rule the server refuses is shown on the field, and nothing is lost.
    browser(["fill", `${row(1)} [data-value]`, "*"]);
    save();
    await waitFor(`document.body.innerText.includes('every device')`, "the refusal on the field");
    assert(new URL(browser(["get", "url"]).trim()).pathname.endsWith("/edit/"), "a refused rule left the form");
    evidence.checks.push("a rule the server refuses is shown on the form, not saved");

    // Editing one rule keeps the others.
    await openForm();
    browser(["fill", `${row(1)} [data-rule-site]`, "dc1-renamed"]);
    save();
    await saved();
    await openForm();
    assert(
      JSON.stringify(await rows()) === JSON.stringify(["location|DC01*|dc1-renamed", "tag|Tag-042|dc2"]),
      `after an edit the rules were ${JSON.stringify(await rows())}`,
    );
    evidence.checks.push("editing one rule keeps the others");

    // Remove everything and save: the source is left clean for the journeys after this.
    browser(["click", `${row(2)} [data-remove]`]);
    browser(["click", `${row(1)} [data-remove]`]);
    save();
    await saved();
    await openForm();
    assert((await rows()).length === 0, "the removed rules came back");
    evidence.checks.push("removing every rule and saving clears them");
  },
};
