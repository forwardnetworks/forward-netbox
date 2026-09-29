# Config backup reaches the repository the way NetBox does

## Goal

A customer's config backup fails at `fetch_remote` with `GitProtocolError`,
while the same git data source syncs fine through NetBox. NetBox's git backend
applies its proxy settings (`HTTP_PROXIES` / `PROXY_ROUTERS`), and config
backup ignored them. Close that gap, along with four others found on the way:

- every successful HTTPS push wrote the credentialed url to the worker's
  stderr;
- a branch the remote refused was counted as pushed;
- a `GitProtocolError` said nothing more than its class name;
- the support bundle redacted the job's error by key, so an exported failure
  carried nothing at all.

The next failure on any estate should name its own cause in the GUI and in
the bundle.

## Constraints

- **Transport settings come from NetBox.** `DataSource.get_backend()` is the
  same call NetBox's own sync makes. It is identical on 4.6 and 4.7 and does
  no network I/O. An unsupported proxy scheme fails the `resolve` stage with
  a category, the way NetBox's own sync would fail.
- **Nothing that can carry a url, a credential or a response body leaves the
  module.** Failures reduce to a closed category and a fixed sentence.
  Exported fields are closed tokens validated on read, never the proxy url.
- **No behaviour change for a remote that works today.** That covers a local
  path, `file://`, ssh, and HTTP(S) with or without credentials and with or
  without a proxy.

## Touched Surfaces

- `forward_netbox/utilities/config_backup.py`:
  - `_RemoteConnection` / `_remote_connection` replace the url embedding.
  - `_apply_proxy_to_repo` is new.
  - The fetch goes through `dulwich.client.get_transport_and_path`.
  - The push gets `NoneStream` and `ref_status` checks.
  - `CONFIG_BACKUP_FAILURE_CATEGORIES` and `_classify_remote_failure` are new.
  - `ConfigBackupError(category=)`.
- `forward_netbox/jobs.py`: job data gains `stage` and `failure_category`.
- `forward_netbox/utilities/health.py`:
  - transport facts and the last run in `config_backup_delivery_state`;
  - both exported through the bundle payload;
  - Health gives the exact `device_config_path` value, the stricter layout
    check, and the last failure with its proxy hint.
- Tests: `test_config_backup.py` and `test_config_backup_job_and_health.py`.

## Approach

1. **Connection.**
   - The url is the data source's own and is never rewritten.
   - Credentials are dulwich `username`/`password` arguments, only for
     HTTP(S), and only when the url carries none (as NetBox's GitBackend
     does).
   - The HTTP proxy is read from the backend's dulwich config; a SOCKS proxy
     becomes NetBox's `ProxyPoolManager`.
2. **Fetch.**
   - The proxy is written into the temporary bare repo's config.
   - The fetch is `get_transport_and_path(url, config=repo stack, **kwargs)`
     followed by `client.fetch`. `porcelain.fetch` accepts neither a config
     nor a pool manager.
3. **Push.**
   - `porcelain.push` reads the same repo config, gets the same kwargs, and
     `errstream=NoneStream()`.
   - A truthy `ref_status` for our branch is `push_rejected`.
4. **Classification.**
   - Walk `__cause__`/`__context__` and urllib3's `MaxRetryError.reason`.
   - Categories:
     - 401 / 407 / 404;
     - `http_NNN` (from dulwich's fixed "unexpected http resp NNN");
     - TLS, proxy connect, DNS, refused, timeout;
     - a non-git response (a proxy or SSO page), a redirect;
     - push rejected;
     - otherwise `other:<ExceptionType>`.
   - Sentences reuse the needles `diagnostics.failure_reason` already slugs.
5. **Reporting.**
   - The job stores `stage` and `failure_category`.
   - Delivery state reads the latest config-backup job, validating both
     tokens.
   - The bundle adds `url_scheme`, `credentials_set`, `proxy_applies`,
     `proxy_kind`, `proxy_config_errored`, `env_proxy_set`,
     `last_run_status`, `last_run_at`, `last_failure_stage` and
     `last_failure_category`.
   - Health names the last failure and, when a proxy applies (or doesn't),
     the likely fix.
   - The Validity path check requires `configs/` and `.cfg`, and the message
     gives `configs/{{device.name}}.cfg`.

## Validation

- **`RemoteConnectionTest`:**
  - the url is unchanged and credentials pass as kwargs;
  - a username alone is passed alone;
  - credentials embedded in the url win;
  - ssh and local remotes carry no credentials;
  - an HTTP proxy is picked up, and a SOCKS proxy becomes a pool manager;
  - an unusable proxy is a `resolve` failure.
- **`RemoteFailureClassifierTest`:**
  - HTTP statuses, auth, and every transport cause through the chain;
  - an SSO page is `non_git_response` and its body is never echoed;
  - a login redirect;
  - anything else is its type name only.
- **Over the in-process smart-HTTP remote:**
  - a proxy 403, a 503 and an HTML SSO page each land on their category;
  - `HTTP_PROXIES` reaches the fetch config;
  - the job's `stage`/`failure_category` reach the bundle, with `url_scheme`
    and `credentials_set`;
  - the existing 401, empty-remote, explicit-branch and refused-push cases
    still hold.
- **Local repo:** a rejected ref fails the push; the push always runs with
  `NoneStream`.
- **Health:** an unset or wrong path gives the exact value, and `.txt` under
  `configs/` now warns.
- Regression: every other config-backup and health test, then the full
  `invoke ci`.

## Rollback

No migration and no model change. Reverting restores url embedding, and with
it the stderr credential leak.

## Decision Log

- **The transport client, not `porcelain.fetch`.** It is the only fetch entry
  point in dulwich 1.2.11 and 1.2.14 that takes a config and a pool manager,
  which is what NetBox's proxy settings need.
- **The proxy goes into the temporary repo's config rather than a kwarg.**
  `porcelain.push` has no config parameter and reads the repository's stack;
  writing it once makes fetch and push agree.
- **The Validity path check is stricter, not rendered through Validity.**
  Rendering a template through Validity's own device path resolution would be
  exact, but it ties Health to Validity internals. Requiring both `configs/`
  and `.cfg` catches the misconfigurations seen so far, and the message now
  gives the literal value to set.
