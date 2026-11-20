import { h } from "../lib.mjs";

const { assert, browser, evaluate, waitFor, open, clickLink, baseURL } = h;

const EDIT = `${baseURL}/plugins/forward/source/1/edit/`;
const save = () =>
  evaluate(
    `Array.from(document.querySelectorAll('form button[type="submit"]')).find(` +
      `(b) => b.textContent.trim() === 'Save').click(), true`,
  );

async function setRules(lines) {
  open(EDIT);
  await waitFor(`document.querySelector('[data-forward-site-rule-table]') !== null`, "rules table");
  browser(["click", "[data-forward-site-rule-table] [data-edit-text]"]);
  browser(["fill", "#id_site_rules", lines]);
  evaluate(`document.getElementById('id_site_rules').dispatchEvent(new Event('change')), true`);
  save();
  await waitFor(`!location.pathname.endsWith('/edit/')`, "the source to save");
}

// The support bundle is what an operator sends instead of screenshots: it has to
// show that rules are set, and never say what they are.
export default {
  id: "support-bundle",
  title: "support bundle counts the site rules and keeps their words out",
  async run({ evidence }) {
    await setRules("location UATSECRETPATTERN* -> uat-secret-site\ntag Tag-001 -> uat-other-site\n");
    open(`${baseURL}/plugins/forward/sync/`);
    await clickLink("ui-harness-sync");
    const href = evaluate(
      `Array.from(document.querySelectorAll('a')).find((a) => a.textContent.includes('Export Support Bundle')).getAttribute('href')`,
    );
    const body = evaluate(
      `fetch(${JSON.stringify(href)}, {credentials: 'same-origin'}).then((r) => r.text())`,
    );
    JSON.parse(body); // the bundle is valid JSON
    assert(body.includes('"site_rule_count": 2') || body.includes('"site_rule_count":2'), "the bundle does not count the two site rules");
    for (const secret of ["UATSECRETPATTERN", "uat-secret-site", "uat-other-site"]) {
      assert(!body.includes(secret), `the bundle exports the rule text ${secret}`);
    }
    evidence.checks.push("support bundle counts the site rules and does not export their patterns or site names");

    await setRules("");
    open(`${baseURL}/plugins/forward/sync/`);
    await clickLink("ui-harness-sync");
    const cleared = evaluate(
      `fetch(${JSON.stringify(href)}, {credentials: 'same-origin'}).then((r) => r.text())`,
    );
    assert(cleared.includes('"site_rule_count": 0') || cleared.includes('"site_rule_count":0'), "clearing the rules left a count in the bundle");
    evidence.checks.push("clearing the rules brings the bundle count back to zero");
  },
};
