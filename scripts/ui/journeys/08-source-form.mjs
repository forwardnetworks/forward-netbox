import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "source-form",
  title: 'source form exposes scope controls',
  async run({ evidence }) {
    open(`${baseURL}/plugins/forward/source/1/edit/`);
    assert(
      currentURL().pathname === "/plugins/forward/source/1/edit/",
      `source edit form redirected to ${currentURL()}`,
    );
    const sourceTitle = evaluate("document.title");
    assert(
      !/not found|server error|forbidden/i.test(sourceTitle),
      `source edit form did not return a success response (title: ${sourceTitle})`,
    );
    await expectVisible("Forward Source");
    for (const fieldLabel of [
      "Apply Device Scope Tags",
      "Import SNMP Endpoints as Devices",
      "Import Generic SNMP Endpoints as Devices",
      "Scope SNMP Endpoints by Include Tags",
    ]) {
      const fields = evaluate(
        `Array.from(document.querySelectorAll("label")).filter(` +
          `(label) => label.textContent.trim() === ${js(fieldLabel)}` +
          `).map((label) => {` +
          `const control = label.control;` +
          `return control ? control.getClientRects().length > 0 : null;` +
          `})`,
      );
      assert(fields.length === 1, `source form is missing ${fieldLabel}`);
      assert(fields[0] === true, `source form hides ${fieldLabel}`);
    }
    assertNoHorizontalOverflow("desktop source form");
    evidence.screenshots.push(screenshot("desktop-source-form.jpg"));
    evidence.checks.push(
      "source form exposes scope tags, console-server import, generic endpoint opt-in, and endpoint include scope",
    );
  },
};
