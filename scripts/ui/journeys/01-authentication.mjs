import { h } from "../lib.mjs";

const {
  assert, browser, evaluate, waitFor, js, expectVisible, countExactText,
  assertNoHorizontalOverflow, currentURL, open, clickLink, clickLinkMatching,
  clickHref, screenshot, baseURL,
} = h;

export default {
  id: "authentication",
  title: 'unauthenticated requests are sent to login',
  async run({ evidence }) {
    // The status code needs no browser: an anonymous request must be bounced
    // to the login page or rejected outright.
    const anonymous = await fetch(`${baseURL}/plugins/forward/sync/`, {
      redirect: "manual",
    });
    const location = anonymous.headers.get("location") ?? "";
    const redirect = new URL(location, baseURL);
    const bouncedToLogin =
      [301, 302, 303, 307, 308].includes(anonymous.status) &&
      redirect.pathname === "/login/" &&
      redirect.searchParams.get("next") === "/plugins/forward/sync/";
    assert(
      bouncedToLogin || anonymous.status === 401,
      `unauthenticated sync list did not require authentication ` +
        `(status=${anonymous.status}, location=${location})`,
    );
    open(`${baseURL}/plugins/forward/sync/`);
    const unauthenticatedURL = currentURL();
    assert(
      (unauthenticatedURL.pathname === "/login/" &&
        unauthenticatedURL.searchParams.get("next") ===
          "/plugins/forward/sync/") ||
        anonymous.status === 401,
      `unauthenticated browser visit was not sent to login (${unauthenticatedURL})`,
    );
    evidence.checks.push("unauthenticated sync list requires authentication");
  },
};
