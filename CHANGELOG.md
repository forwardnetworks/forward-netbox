# Changelog

Generated from the README compatibility table by `scripts/gen_changelog.py`. Do not edit by hand.

## v2.8.2

Release candidate; Fix: **a sync no longer tries to delete the shared catalogues, global IPAM and operator-gated rows that Forward stopped reporting**. Baseline reconciliation has refused to remove sites, device types, platforms, manufacturers, device roles, prefixes, VLANs, VRFs, devices and the software-version catalogue since 2.7.13, for reasons written out beside the list: shared catalogues may be Device Type Library imports, global IPAM is never pruned by device scope, and device and site removal is operator-gated through Scope Reconciliation -> Prune orphans because absence from a query result is not evidence an object is gone. The Forward-diff delete path enforced none of it and deleted every one of them unattended. One deployment reached the delete for a VRF, a device type and three sites in a single sync, and only a database PROTECT constraint stopped it - a site or device type with no children has nothing holding it. Both paths now read one allowlist, and a test requires every model with a delete handler to be classified by it, so a model added later cannot inherit "deletable" by omission. Renames are deliberately unaffected: a row whose identity key changed is superseded by a row written in the same batch, and that delete still applies. Operator-visible change: a device or site Forward stops reporting now stays in NetBox until someone prunes it. Fix: **a protected-delete skip names the NetBox row it is about**. Five skips reading `dcim.site row processing skipped (...; still referenced by dcim.device)` were each correct and none actionable - two byte-identical to each other - because everything that would identify the row is a name or a slug and is redacted before it persists. The record now carries the NetBox primary key, which resolves to the object page. Also: the release checks name the release and commit they measured instead of silently reporting on the wrong one, and the hash-pinned release toolchain moves to `cryptography` 50.0.0. No migration; upgrade and re-run the sync.

## v2.8.1

Fix: **the drift report measures actual drift instead of counting every fetched row**. `In sync`, `Drifted models` and `Total drift` read "Not measured" on every run for every deployment, permanently - `EXACT_COMPARISON` was defined in the code and produced nowhere, because the dependency preview had nothing to compare against and treated each fetched row as a change. One deployment read 694,477 "estimated apply work" against roughly 4,000 devices, which is the row count rather than a divergence. The preview now compares Forward rows against NetBox for ten models - sites, manufacturers, device types, VLANs, VRFs, prefixes, MAC addresses, IP addresses, interfaces and devices - by running the apply path in a read-only mode rather than reimplementing the comparison, so the figure cannot drift from what a sync would actually do. Coverage is partial by design: the models that cannot be compared yet are named in the report rather than folded into the total, and `In sync` deliberately stays unanswered while any model is uncompared, because zero drift across part of an estate is not a statement about the estate. Fix: **a recorded merge rejection names the NetBox row it is about**. A skipped `ipam.ipaddress` row reported the rule it violated but not which row violated it, leaving an operator to find one address among thousands by hand; the record now carries the NetBox primary key, which resolves to the object page where the edit has to happen. Contributed by @sujeito-operator. No migration and no operator step; upgrade and re-run the sync.

## v2.8.0

Fix: **a device that is merely disabled in Forward is no longer deleted on the next sync**. A disabled device vanishes from `network.devices` and from the REST inventory alike, so from every interface the plugin has, disabled is indistinguishable from decommissioned - and with orphan pruning enabled the next sync deleted it permanently. 76 devices went that way in a single run at one deployment. An orphan now has to stay absent for a number of consecutive promoted runs AND a stretch of wall-clock time before the prune will touch it, both operator-settable on the source (3 runs and 72 hours by default, and both are required because either alone is defeated by a plausible sync schedule). The held-back devices are named on the Scope Reconciliation panel, and the manual Prune orphans button carries an override so a person looking at the list can still act; the unattended path - the one that caused the harm - has no override. Fix: **the quarantine can actually release**. A device absent from Forward loses its scope claim on the first run that observes the absence, and orphan status is derived from live claims, so an absent device silently stopped being an orphan on the second run: the streak could never pass 1, nothing was ever eligible, and the panel reported nothing held. An open absence row now counts as previously managed alongside the live claims, which also removes a pre-existing dependency on whether the prune job happened to run before the tag-reconciliation job. Fix: **a wrapped template comment no longer renders as visible text** on the scope reconciliation and ingestion merge panels. Feature: **netbox-dlm `0.9.1` is supported** across the install pin and every runtime gate, so upgrading the optional lifecycle plugin no longer silently disables the fast baseline. `0.9.0` is deliberately not supported: it cannot render its own views. Feature: **a packaged agent skill** for driving the plugin. No migration beyond `0052`; upgrade and re-run the sync.

## v2.7.13

Fix: **a full sync no longer deletes devices**. The removal reconciliation added in `2.7.11` compared every model against the promoted baseline with no exclusions, so a full run staged deletion for any row absent from the current result - including devices. Device removal is gated behind Scope Reconciliation -> Prune orphans, with a shrink guard and a warning to confirm in Forward before deleting anything, precisely because absence from a query result is not evidence a device is gone; reconciling devices in the fetch path bypassed that gate and did it unattended on every run. One deployment saw 54 deletions applied in a single sync, a device delete refused by a protected reference, five protected-delete skips against the software-version catalogue, and its untagged device count fall by 18. Removals are now restricted by an allowlist to rows the plugin solely authors and that derive from a device which still exists - interfaces, MAC addresses, inventory items, modules, cables, IP addresses, FHRP groups, the per-device lifecycle rows and the routing rows. Devices, sites, shared catalogues, global IPAM and the software-version catalogue are excluded, each for its own reason, and a model added later is excluded until someone decides otherwise. Feature: **the scope report says which untagged devices this sync owns**. "Carries neither include tag" covers two opposite situations, and a deployment reported 407 such devices while orphans read zero - not a contradiction, because an orphan is a device the sync previously claimed and no longer sees, so one it never claimed is not an orphan of it. The panel now splits them by whether the sync holds an identity for the device: owned means it created it and either the device left scope or the tag was never applied, unclaimed means another source or an operator did, or it is a leftover from a configuration that no longer applies. Reports only; nothing is deleted. No migration and no operator step; upgrade and re-run the sync.

## v2.7.12

Fix: **a snapshot that collected almost nothing is refused before anything is staged**. A cancelled Forward collection still produces a snapshot that processes green with a normal device count, so the newest-processed selector takes it; every bundled query filters on a completed collection, the whole estate then reads as departed, and a deployment had tens of thousands of objects staged for deletion with validation reporting PASSED. It passed because the row-count floor compares only models executed in full - a diff run is structurally exempt from every collapse check. Collection health is a property of the snapshot rather than of any model's result, so it is decidable before a row is staged and it holds for both execution modes. The guard sits above the drift-policy check: a sync with no policy is covered, and a policy can tune the threshold but its absence cannot disable it. It compares against the last baseline ingestion rather than the previous run, so a run of bad snapshots cannot walk the threshold down one step at a time. Fix: **stale DLM hardware notices are identified by what Forward emits, not by whether a device type holds devices**. The previous rule flagged 33 notices in a deployment where 5 were stale: a Device Type Library import leaves thousands of legitimately empty device types, and notices are written network-wide while devices are imported tag-scoped, so hardware outside the include tags permanently has none and its notice is correct. Acting on that rule would have deleted notices Forward re-creates on the next sync. Because the hardware-notice query is network-complete, its result is authoritative, which also reaches rows orphaned before the contributor baseline holding them was superseded - the ones the baseline comparison cannot see. Reconciliation runs automatically on a full execution, an empty result is refused as indistinguishable from a failed fetch, and device types are never deleted. Feature: **the Scope Reconciliation panel clears them on a button**, so removing legacy rows needs no command line; the job records which device types it cleared. Compatibility: **netbox-dlm `0.8.0` is accepted**, validated by running the suite against it rather than on its changelog - the upstream change is user-interface plus one badge colour, with no model or migration change. The install cap that would have refused it outright is lifted. No migration and no operator step; upgrade and re-run the sync.

## v2.7.11

published as `2.7.11`: `2.7.10` was tagged but never published, so no `2.7.10` artifact exists on PyPI - the release lineage carried only three commits from the prior post-release bridge, and the publish workflow requires a bootstrap, production and evidence commit before it will build anything. 2.7.11 is the same tree with that lineage in place. Fix: **a device name shared by two NetBox rows no longer refuses the whole ownership domain**. `v2.7.9` made an absent device name non-fatal and left the neighbouring case - a name that resolves to SEVERAL devices - refusing as before, so a deployment with one duplicated device name out of thousands still failed both the scope-tag and status-tag domains on every run: ownership never completed, convergence stayed blocked, and every drift figure still read "Not measured". Such a name is now HELD rather than refused. It is never tagged on a guess and never released, and its existing claims are refreshed to the current generation - a held claim left at an older generation counts as a stale claim, which gates completion, so holding without refreshing would have replaced one permanent block with another. The devices keep exactly the tags they already had. A name Forward no longer reports is still released, or a device that genuinely left scope could never be untagged. Resolution also excludes candidates this sync already binds to a different Forward device, which the uniqueness constraint made ineligible anyway, so the commonest duplicate - a device that moved site or was re-created alongside its predecessor - resolves outright instead of tying. A device whose Forward name CHANGED is held rather than treated as absent, because its previous binding excludes every candidate and calling that absent would have stripped its Forward tags silently on every run. The counts are reported on the job. Feature: **an out-of-scope device now says which kind of absence put it there**. Membership is decided purely by absence from the tag-scope result, and three unrelated situations produce it: the device is gone from the Forward snapshot entirely, it is still there but no longer matches the tag predicate, or it is classified as a custom-command source that every bundled query filters out. Only the first unambiguously means the device left, and telling them apart previously needed a live query an operator cannot run - which matters most when it is most dangerous, because a query that silently narrowed presents as a large second group and Prune orphans would delete live devices. The Scope Reconciliation panel and the audit command now break the count down. It costs one extra query and only when orphans exist, so a converged sync adds none, and a failure to classify leaves the report intact. Feature: **`forward_device_name_ambiguity_audit`** names the NetBox devices behind a held name, and marks the pairs an existing binding already resolves. Persisted diagnostics carry counts because device names are customer data; this computes the answer on the operator's own console instead. No migration and no operator step; upgrade and re-run the sync.

## v2.7.9

Fix: **a device Forward reports that NetBox does not have no longer refuses the whole ownership domain**. Tag reconciliation resolves each reported device name to a NetBox row, and it refused the entire mutation if any name failed to resolve - so a handful of devices carrying the include tags in Forward but absent from NetBox blocked the scope-tag and status-tag domains on every run. Ownership never completed, convergence stayed blocked, and every drift figure read "Not measured", with no remedy short of creating those devices or editing the tags in Forward. A name with no device in NetBox is skipped and counted now: there is nothing to tag and nothing to release, so no NetBox row changes. A name shared by SEVERAL devices still refuses, because the desired set drives both the tag additions and the removals - dropping such a name could release a claim from a device that holds one, and resolving it could tag the wrong device - and that refusal now reports how many names were ambiguous and how many were absent rather than naming the devices.

## v2.7.6

Fix: **an operator's force-allow did not carry forward to the next run**. The override read the sync's latest validation run to decide whether a previous force-allow still applied, but the run being recorded is created before blocking reasons are evaluated, so the lookup always returned that run - whose override flag is never set. The override could not fire on the sync path at all, for every blocking reason, so an operator who accepted a blocked run was blocked again by the same reason with nothing to say why. Fix: **a skipped row now says which way its dependency points**. Almost every dependency skip is waiting on something absent, but a prune refused because children still reference the row is the inverse, and both recorded the same wording - so `netbox_dlm.softwareversion row processing skipped (...; netbox_dlm.inventoryitemsoftware)` read as a missing parent when it meant a surviving child. The two now read as "waiting on" and "still referenced by". Fix: **every dependency skip names the model it is waiting on**. Sixteen of twenty-four raisers recorded only the exception class, so a run of identical `(ForwardDependencySkipError)` rows said nothing about which parent was missing; the slug is taken from the guard that admitted the raise, and a structural test asserts every raiser names one. Fix: **the release provenance allowance no longer covers the release pair**. It was expressed as the first three lineage positions, but a reviewed lineage may be exactly three commits, in which case it covered every commit including the production and release commits; it is anchored to the end of the lineage now. Fix: **the job-statistics persist throttle no longer reads inverted** - it asked to write when nothing had changed, which was inert only because the single non-forced caller marks the data dirty on the line above the call.

## v2.7.5

Fix: **ownership reconciliation could fail permanently on a tag that already existed**. A status tag whose slug Forward reserves - `forward-backfilled`, `forward-out-of-scope` - was refused rather than adopted unless the plugin had just created it, which is exactly when there is nothing to adopt, so the refusal was unconditional. That tag exists in any deployment that has ever had a collection failure, so status-tag reconciliation raised on every run, the ownership domain never completed, convergence stayed blocked, and every drift figure read "Not measured" with no operator remedy short of deleting the tag. The tag is adopted now; assignments an operator made themselves are recorded and preserved, and restored when ownership is released. Fix: **an old ingestion could not be deleted once a device left scope**. Ownership evidence records which run last asserted it, and that stamp protected the run. Evidence is only re-pointed for what the current run sees, so a device that left the Forward scope froze its evidence on the last ingestion that saw it and pinned that ingestion permanently - one more undeletable ingestion for every scope change. The evidence is not stale and is never pruned: the device still exists and is still owned, so only the stamp is dropped. Fix: **a refused ownership reconciliation now names which rule refused it**. Nine conditions raise the same error and only four were catalogued, all of them device-identity conditions, so every failure of the scope-tag job could report nothing but `unrecognized-ownership-conflict`. Fix: **a scope tag is resolved by the name that was configured**. A dotted tag name normalizes to a slug with the dot dropped, so a tag stored under the other convention was invisible to the lookup and any unrelated tag holding the derived slug turned the whole reconciliation into a hard failure. Fix: **an orphan prune is refused when the Forward scope collapsed**. Out-of-scope membership is decided purely by absence from the current query result, so a partially narrowed query manufactured orphans out of live devices and the only guard refused at zero devices returned.

## v2.7.4

published as `2.7.4`: `2.7.3` was tagged but never published, so no `2.7.3` artifact exists on PyPI - the release commit recorded no `## Release Authorization` evidence, and the publish workflow refuses a tag whose plan cannot show which gates ran against that exact tree, so it stopped before building anything. 2.7.4 is the same tree with that evidence recorded. Fix: **one interface NetBox refuses no longer fails the whole sync**. A `ValidationError` raised while validating an interface left the apply engine entirely and surfaced as a terminating failure that named no model and no row, so a run that had staged 172 changes ended having applied none and the only evidence produced identified neither the device nor the interface. The row is now recorded against `dcim.interface` and the rest of the shard proceeds. Fix: **a rejection now says which rule rejected it, not just which field**. The rule catalogue read only the errors NetBox declines to attach to a field, which made a rule legible exactly when NetBox would not name a field and illegible whenever it did. `untagged_vlan` alone carries two unrelated NetBox rules - an interface whose mode admits no untagged VLAN, and a VLAN outside the device's site - with different causes and different fixes, and no evidence the plugin produced could tell them apart. Every field is read now, both rules are catalogued, and the field and the rule are reported together. Fix: **a rejection about state the sync did not write is skipped rather than failed**. Validation covers the whole object while an update writes only the fields that differ, so an untagged VLAN left behind by a device that moved sites fails a later sync that changes only the MTU. Refusing to write is right; calling it a failure is not, because a failure also fails the row's dependents and no retry can change the answer. A catalogued rule on a field the row does not write is now recorded and skipped. A rule the catalogue cannot name stays a failure, deliberately. Fix: **both apply paths reach the same verdict**. The bulk engine and the row-oriented adapter had come to opposite conclusions about the same interface while each was locally correct, and the parity harness could not see it because both write nothing in that case. One shared rule now answers it for both. Fix: **the merge stops filing its own defects as unfixable**. Every validation rejection at merge counted as unsatisfiable, which is right for a rule that belongs to the destination and wrong for a rejection the merge itself caused, where retrying is the correct response. Only a catalogued rule landing on a field the change writes moves; an uncatalogued rule still skips, so nothing can newly block a baseline. Fix: **a spent baseline is no longer called the current one**. Every ingestion that promotes leaves a baseline record behind, and promotion only marks the previous one superseded - it clears the contents but keeps the row, which goes on protecting its ingestion. The refusal told an operator with three undeletable ingestions that each was the baseline for the sync, true of at most one, and pointed at a repair the product does not offer. It now says the record is spent, that it still protects the ingestion, and that nothing releases it today. Collecting spent baselines is still a known gap. Feature: **a command that finds the interfaces NetBox will refuse**. `forward_interface_vlan_audit` reports every interface whose untagged VLAN belongs to another site, and every interface carrying one with no 802.1Q mode, naming the device, the interface, both sites and the VLAN ID. Persisted diagnostics carry schema identifiers and never collected values, which is why a rejected row cannot name itself; this computes the answer on demand and writes it to the operator's console instead. Fix: **an ingestion whose baseline has been superseded can be deleted**. Every ingestion that promotes leaves a baseline record behind, and promotion only marks the previous one superseded - it clears the contents but keeps the row, which goes on protecting its ingestion. Nothing ever removed one, so every ingestion that had promoted was permanently undeletable and the backlog grew by one per successful sync. A spent record now goes with its ingestion; the live one is still kept, by every route rather than only through the delete button. Fix: **an ingestion whose job is still running is refused**, the same protection a sync already had. Deleting one mid-flight removed the job record while its worker carried on against it. Fix: **a skipped row says which model it was waiting for**, and says skipped rather than failed. Six identical rows naming only an exception class told an operator nothing about which parent was missing. Rows skipped while pruning are the inverse case and now name the records still referencing the object, which is why the prune was refused. Compatibility: **netbox-dlm `0.7.0` is accepted**, validated against the full suite rather than accepted on its changelog. No migration and no operator step; upgrade and re-run the sync.

## v2.7.2

Fix: **a query bound by ID no longer has to prove a commit before it can run**. A map bound to a Forward query by direct ID with no stored commit was rejected before execution, and because one ineligible map empties the whole plan, nothing was scheduled and the sync ended having fetched nothing - while every Forward call returned 200. The plugin was resolving a commit on Forward's behalf by walking the query history for one whose source matched its own bundled copy; Forward never required it, and the execution payload already omitted it for head. A query ID now executes at Forward's latest. An explicitly stored commit is still honoured, and diff execution is unchanged because there the two commits are part of the request path. A query whose parameters have moved on now fails on its own model, naming the map, instead of silently emptying the run. Fix: **a run whose model comes back far smaller than its baseline is refused**. Dropping the commit check removed the comparison that used to notice a query changing underneath a map, which left one case invisible: a query that still accepts the same parameters and still returns the same shape, but fewer rows. Those rows are authoritative, so the absent ones reconcile as deletions with nothing reported. A model returning more than 30% fewer rows than its last promoted baseline, and at least 20 fewer, now blocks the run before anything is staged. It is on by default and a sync with no drift policy is still covered; a policy can widen or disable it, the absence of one cannot. A first run, a diff run, a model already failing, and a scope the operator narrowed are all excluded. Fix: **a failed sync now says why, per model**. The reason was reduced to an exception class name before anything recorded it, so no log, database row, or support bundle could recover it - one deployment ran five consecutive failed syncs that could not be diagnosed from any evidence it produced. Failures now carry a catalogued reason alongside the class, resolved once in a shared place rather than as another per-case exception. Two paths that composed their own message, and two job outcomes that were exported as redacted because they were phrased differently, were folded into it. Messages that embed collected values are still reduced to wording and slugs. Release flow: **a post-release commit that carries executable content fails the harness**. The first commit after a release tag must be documentation only, and it cannot be reclaimed once taken, so getting it wrong makes every later release unverifiable. No migration beyond the drift-policy fields, which apply themselves, and no operator step; upgrade and re-run the sync.

## v2.7.1

Fix: **a merge delete blocked by a hidden PROTECT reference is now predicted instead of discovered at apply time**. 2.7.0 shipped this as a known limitation because widening the predictor on reasoning alone kept a device that should have been deleted; the predictor now accounts for the provenance rows the merge clears inside the same transaction, so a delete is scheduled when nothing outside the merge still protects the row and is still refused when something does. Fix: **a CVE and the software version referencing it no longer fail when both are created in the same merge**. The dependency graph modelled scalar foreign keys but not many-to-many references, so the target could be ordered after the row pointing at it. Latent since 2.5.11 and only reachable once the CVE maps were enabled. Fix: **deleting an ingestion now takes its own scope-reconciliation rows with it**. Those rows protect the ingestion, so an ingestion that had reconciled could never be removed and the refusal named a model the operator has no way to clear. Fix: **the ingestion issues list no longer returns a 500**. The view was registered but its URL was never included, so the list route resolved to nothing while the detail route worked. A test now sweeps every model with a detail route for the same omission. Fix: **a row NetBox can never accept is now recorded as skipped rather than failed**. Any failed row blocks baseline promotion, and without promotion there is no bookkeeping and drift reports Not measured, so a handful of permanently unsatisfiable addresses could mask thousands of correct changes. Integrity and unrecognised errors still fail and still block. Security: takes the `cryptography` fix for CVE-2026-69247. Release flow: dependency advisories are audited before the gate rather than after the tag, a skipped post-release step now fails the harness instead of passing quietly, and a release opens its own close-out work. No migration and no operator step; upgrade and re-run the sync.

## v2.7.0

Fix: **every ingestion was undeletable, and the refusal explained nothing**. `ForwardIngestionProvenanceMixin` declares its ingestion foreign key as PROTECT with `related_name="+"`, and Django hides such relations from `_meta.related_objects` - which is exactly what the protection check iterated, so of the five PROTECT relations pointing at an ingestion it could see one. An ingestion held only by device identities reported no refusal, so the delete view fell through to NetBox's confirmation page, rendered one dependent row per synced device, and then failed with `ProtectedError` on confirm. Every ingestion owns one identity per device, which is why the count grew with every sync rather than staying at one. Fix: **a primary IP moving between devices now converges**. The ownership proof was read through the active branch, and a branch is a snapshot of the moment it was provisioned, so an identity finalized after that provision was invisible and read as "not ours" - suppressing a release we were entitled to make. The holder lookup and the writes stay branch-native; only the ownership proof is pinned to the control plane. Feature: **CIMC firmware is reported into device lifecycle**. netbox-dlm `0.6.0` adds `InventoryItemSoftware`, so the firmware running on each Cisco management controller now lands in DLM alongside every device's OS version. The version is read from the SNMP sysDescr Forward already collects, so the map requests no additional collection; an endpoint whose sysDescr carries no firmware clause yields no row rather than a guessed one. Opt-in, and it attaches to the inventory items the CIMC endpoint map creates. Fix: **an optional plugin version that did not match exactly silently disabled its whole integration**. `required_package_version` was an equality gate, and an unmatched version skips every model of that integration; it now expresses a set of supported versions, the same shape the fast baseline already uses for the same reason. Known limitation: a merge delete blocked by one of those hidden PROTECT relations is still not predicted, so it is scheduled and fails at apply time, and a failed row blocks baseline promotion. Widening the merge predictor was tried and measured: it silently kept a device that should have been deleted, because the merge clears that reference in the same transaction. Deferred rather than shipped on reasoning. No migration and no operator step; upgrade and re-run the sync.

## v2.6.12

Published as `2.6.12`: `2.6.10` and `2.6.11` were tagged but never published, so no artifact exists for either and PyPI went from `2.6.9` to this release. 2.6.12 carries both tranches plus the release-path fix below. Fix: **a merge failure that names no field now names its rule**. A non-field validation error reports `__all__` — a field name meaning "no field" — so a customer's `ipam.ipaddress` merge failure was indistinguishable from any other cause. NetBox interpolates the message before raising, putting the address inside the string, so it cannot be persisted as-is: known rules resolve to slugs this plugin defines, and anything uncatalogued is reduced to wording with every value-bearing token masked. Confirmed in the field: a failure that had read only `ValidationError` now reads `violating primary-ip-reassignment-blocked`. Fix: **a row NetBox refuses on its own validation rule no longer wedges the baseline**. Any failed row blocks baseline promotion, and re-running cannot change a validation rejection because the rule belongs to the destination rather than the incoming data, so a few permanently unsatisfiable addresses could stop thousands of correct changes from ever being attested. Those rows are recorded and skipped; integrity and other errors still fail and still block. Fix: **the ingestion log download contains the issues behind the run** — message, exception and structured diagnosis, with an explicit truncation flag past 200 rows. The exception text was previously discarded before anything persisted it, so searching server logs for it could not have matched on any deployment. Fix: **the one ingestion that cannot be deleted no longer reads as an error** — it is the baseline, the durable record of what has already converged, and NetBox protects it deliberately; the refusal now says so and is raised as a warning. Fix: **the scope reconciliation refresh button no longer returns a 500 while also queueing a job that never runs**. The view handed the job class to the queue where a callable was required; RQ rejects a class, but only at dispatch, which happens on transaction commit — after the Job row is already written. Fix: **a failed ownership reconciliation names which conflict refused it**. Eight sites raised the error bare and the message was stripped before the job record was written, so the customer-visible failure carried the exception class and nothing else. The messages embed device names, so an allowlist maps each known conflict to a slug, and an unmatched message is recorded as unrecognised rather than falling silent. Release flow: **the release provenance gate is now satisfiable**. Two of its requirements had not been jointly satisfiable since the 2.6 series began: every commit in the reviewed range must still have a successful main-push run, which GitHub expires, while the security-bootstrap check required the three scanner files to appear in a diff from the anchor — true only while the anchor sat immediately before the single commit that introduced them. A fixed anchor expires runs and a moved anchor empties that diff, which is why `2.6.10` and then `2.6.11` both failed at the tag. The scanner is now verified at the release commit against pinned digests, which is strictly stronger than the diff test it replaces because that test never looked at content, and which frees the anchor to advance. No migration and no operator step; upgrade and re-run the sync.

## v2.6.9

Published as `2.6.9`: `2.6.7` and `2.6.8` were tagged but never published, so no artifact exists for either — a release-time gate resolved the version to upgrade from by reading git tags, and a tag records only what was tagged, not what reached the index. 2.6.9 carries both tranches plus the fixes below. Fix: **a merge failure names the constraint or field it violated** in the issue list, not only in the record behind it — the detail had been stored since 2.6.6 but the list still showed just the exception class, so a merge `ValidationError` looked identical whatever caused it. Fix: **the one ingestion that cannot be deleted says so first**. Deleting the baseline ingestion rendered every dependent object — one per synced device — and only then refused, so the refusal arrived after several hundred device names. Release flow: the upgrade gate moved out of the tag-triggered publish into pull-request CI, where a failure blocks a merge rather than consuming a version number that cannot be reused, and the version to upgrade from is resolved from the package index rather than from tags. Fix: **a module whose type re-creates a component another module already owns** no longer fails the row. NetBox adopts an existing component only when it belongs to no module, so a console port already claimed by a different module can be neither adopted nor recreated and the database rejects the duplicate name; because a failed row blocks baseline promotion, one such module stalled a whole sync. The collision is now detected before the write and reported as a rolled-up skip. Fix: **two devices resolving the same management address** no longer fail the ingestion. NetBox allows an address to be the primary IP of exactly one device, and the `Mgmt_`-tag path assigned it without checking, so a shared management address ended the sync at `dcim_device_primary_ip4_id_key` after every workload had already been staged. Each address is claimed once and the conflicts are reported. Release flow: the build job now checks out with tags, and a harness check fails closed if a job running a tag-dependent task ever loses them again. Convergence, diagnosability and apply-performance release. Fix: **a device could fail to adopt its primary IP** with `The specified IP address is not assigned to this device` — when the device and the address both already existed and a sync only re-pointed them, nothing ordered the two updates and whichever change was recorded first won. A race, which is why it presented as a single device out of thousands and never reproduced. Fix: **a delete the database will always refuse no longer fails the row** — a delete held by a PROTECT reference that survives the run was still attempted, and because any failed row blocks baseline promotion, one operator-owned object could wedge a sync's convergence bookkeeping indefinitely. It is now reported and skipped, leaving the intent visible in drift. Fix: **a failed sync explains itself in the UI** — a terminating failure recorded no model and no detail, so the one issue that says why a run died was the least informative thing on the page and the constraint was recoverable only from server logs. Constraint, table and invalid-field names are now recorded on every sync failure, in the issue message and over the API; the key values behind them still are not. Fix: **one invalid MAC row no longer aborts the whole batch**, and two stored rows sharing a canonical address are reported instead of one being overwritten at random. Feature: **an opt-in COPY/SQL apply engine for MAC addresses**, off by default and version-pinned, measured 8.98x faster on a realistic mix with full fallback to the existing engine. Performance: **IP address creation issues a third fewer queries** by not re-fetching an interface and device that validation never needed. Feature: **a license-tier refusal from Forward is named as one** — organization-authored queries require a NETWORK-facet tier and CVE data additionally requires SECURITY, so the failure says which capability is missing instead of surfacing a raw HTTP error. Fix: **an optional plugin can be upgraded again** — `netbox-dlm` was pinned to an exact `0.4.1`, so `0.5.0` was refused at install; worse, the fast baseline, set-based merge and COPY/SQL engines each compared the whole optional-plugin tuple exactly, so getting past the install pin would have switched all three off with nothing an operator could see — turning a first sync from minutes into hours. Each engine now lists the versions validated against it, and `0.5.0` is one of them; anything unlisted is still refused. Fix: **NetBox and Branching patch releases no longer block the plugin** — the plugin refused to load on anything but NetBox `4.6.5` and `netbox-branching` `1.1.1`, so a patch upgrade to `4.6.6` or `1.1.2` stopped it dead; and where it did load, the version-pinned engines switched themselves off silently. Any `4.6.x` and `1.1.x` is accepted now, with behavioural contract checks in place of version equality: the set-based merge engine verifies the live MAC search index and the fast baseline verifies the fields of every model it loads directly, so a patch that really does change something underneath still fails closed. NetBox `4.6.6` added a third indexed MAC field, which is exactly what that check catches — set-based merge (opt-in, off by default) declines there until it emits that field. Release flow: the gate no longer has to run on port 8000, and an upgrade from the previous release is tested before publish rather than only a clean install. No migration and no operator step; upgrade and re-run the sync.

## v2.6.6

Diagnostics and convergence release, from a 2.6.5 upgrade in the field. Fix: **a shared DLM catalogue row could be created twice in one branch**, failing at merge with `IntegrityError` and blocking the baseline — and because a partial merge is retryable but the failure deterministic, every retry failed identically. Three defects lined up: `netbox_dlm.cve` was missing the `reuse_on_unique_conflict` policy every comparable catalogue carries; that policy only caught `IntegrityError` and never the `ValidationError` `full_clean()` raises first, so it had never worked on the common path for any model; and the direct upsert helper hard-coded `strict`, so the create-if-missing path bypassed the policy table entirely while the map's own apply honoured it. Fixed at staging rather than merge, because an inbound foreign key makes merge-time convergence unsafe. Fix: **a query moved to another folder could not resolve a verified revision** — history records the pre-move path, so asking for the current path at an old commit returns 404 and the model was skipped. Fix: **ingestion delete**, absent since 2.6.3 when a crash was worked around by removing the action rather than registering the missing view; a protected reference is now reported instead of a 500. Fix: **scope reconciliation ran two live NQE fetch-all queries during page load**, returning 504 on any real fabric; it now runs as a background job and the page renders the stored report. Fix: **a normal query binding was reported as a contract failure** — proven by two bundles showing the same signal while a sync applied nothing and after it applied 24,748 changes. Feature: **merge failures record a diagnosis** (constraint, table, invalid field names — schema identifiers only, never key values) and ingestion issues are openable, so a failure can be acted on. Feature: **model change density is learned from measurement** instead of a hardcoded table that no deployment ever escaped. Feature: **a merge stalled by a row it can never apply is no longer a dead end** — any failed row returned the branch to Ready to merge without completing, so the baseline never promoted and drift stayed *Not measured*, ownership *Incomplete*, and diffs unavailable; when the failure was deterministic that state was permanent. **Accept failures & merge** on the ingestion page completes the merge over the recorded failures and promotes the baseline. The failures remain recorded and the acceptance is stored with who accepted it, when, and how many, so an accepted-over baseline is never mistaken for a clean one. Release flow: all four version surfaces bump together, `main` carries a `.dev0` marker between releases, and the evidence base commit is checked before CI rather than after. No migration and no operator step; upgrade and re-run the sync.

## v2.6.5

Convergence, churn and performance release. Fix: **a protected parent was deleted before the child protecting it**, so the database refused the delete and the row survived every sync. The framework builds child-before-parent delete ordering from each child's branch preimage, but a branch-native bulk delete can carry none, so the edge was never created - the same gap already closed for the update case. Protecting references are now read from the destination, which needs no preimage, and the missing ordering edges are restored. A reference whose row survives the run deliberately gets no edge, so a delete the database genuinely must refuse still fails strictly rather than being reordered into a false success; mutually protecting rows are detected and dropped from the ordering instead of aborting the whole merge. A refused delete now also names the model and count holding it. Fix: **one malformed MAC address failed every other MAC in the same shard** - the existing-row prefetch prepared each value through NetBox's MAC field, so a single unparseable string raised while the query was being built, before any row was examined. Unparseable values are now excluded from the lookup and rejected per row. Fix: **every BGP peer was rewritten on every sync** - live counters (session state, advertised and received prefixes) were rendered into `comments`, which drives change detection, so 360 peers produced 360 updates, change records and branch changes per run despite unchanged configuration. Only stable descriptive state is rendered now; Forward remains authoritative for the counters and `status` still carries active/offline. Feature: **set-based merge engine**, default off behind `enable_set_based_merge`, with merge observability and opt-in profiling; landed with its paired-branch equivalence and fault-injection proofs passing. Schema migration `0045`. No operator step; upgrade and re-run the sync.

## v2.6.4

Corrective release. Fix: **maps bound to a query ID could never run a full sync** - **Resolve to Query ID** stores the query ID and moves the repository path aside, but head-commit resolution only read the original path, so the execution contract rejected every such map as `unresolved_full_commit` and skipped its model. A sync could therefore fetch almost nothing. Head resolution now works for either binding: a map keeping its path resolves through it, and a map bound only by a query ID resolves head from the repository query index. A commit is adopted only when the path still resolves to the bound query ID, an ambiguous or unresolvable ID leaves the map untouched with its contract reason reported, and an explicit commit pin is never overwritten. Fix: **a run whose fetches all failed reported success** - an empty workload was recorded as a completed, branchless, zero-change ingestion, which is indistinguishable from genuine convergence and left Drift reporting "not measured". Such a run now fails closed and names the failing models. Fix: **a worker timeout during head resolution** no longer degrades into an unresolved commit. Observability: execution-contract preflight now separates map rejections that skip a model from inert diff-only findings, which previously buried the blocking ones. No migration and no operator step; upgrade and re-run the sync.

## v2.6.3

Corrective release. Published as `2.6.3`: `2.6.2` was tagged but never published, so no `2.6.2` artifact exists on PyPI. Fix: **Ingestions list crashed** with `NoReverseMatch` for `forwardingestion_delete`; ingestion rows now declare an explicit empty action set, and an installed-wheel route probe renders every plugin menu list from the packaged artifact so this class of failure cannot ship again. Fix: **sync blocked by legacy repository-path query binding** - maps bound by repository path with no stored query ID failed spec resolution and blocked validation. Maps now resolve read-only to their org query ID (exact path, then unique filename, then unique intent, failing closed on ambiguity), and a new **Resolve to Query ID** action lets an operator bind every map without NQE-library write permission. Fix: **publish permission** is now checked before any write, naming the missing capability instead of failing generically. Fix: **`dcim.platform` query crashed** on snapshots where a device reported no OS version (`matches()` received null); the bundled query defaults an absent version, and a source-level guard scans every bundled query for possibly-null values entering null-intolerant helpers. Fix: **execution-contract safety** - a full contract can never execute against a parameter-incompatible revision and a diff contract can never execute against a parameter-declaring revision; both fail closed with a type-only diagnostic and a preflight surfaces inconsistent contracts before a sync starts, replacing an opaque Forward `HTTP 400`. Fix: **protected DLM software-version deletion** was scheduled before the updates releasing its foreign key; destination values are now read in batches and update-before-delete ordering applied, while retained references still fail strictly. Feature: **fast baseline load** - a first sync into an empty NetBox applies directly to main in one transaction instead of staging and merging ~1.2M branch changes, cutting a first-time load from hours to roughly an hour on a 3,400-device dataset. Strictly gated: default off, exact pinned runtime tuple, full snapshots only, empty target and side tables, no prior ingestion or baseline, no competing branch, all rechecked under locks and rolled back entirely on any failure; every later sync uses the ordinary branch workflow. A read-only preflight reports eligibility, and a durable attestation records the engine and the branch evidence deliberately omitted. Performance: **NQE async polling** now backs off to a five-second plateau, cutting status calls by about 91% on a full sync. Observability: merge progress, rate and ETA are operator-visible, merge failures capture exit status and cause, and support-bundle job errors expose the inner exception type without customer data. Python/UI plus schema migrations `0043` and `0044`.

## v2.6.1

Compatibility fix: restores the supported Python range to `>=3.10,<3.15` (including Python 3.12) without changing runtime behavior.

## v2.6.0

Feature: **durable convergence control plane** - main-schema, per-sync identity and ownership claims are stamped with the exact merged ingestion generation. Managed scope/status tags and virtual-parent links materialize from the union of current claims, including shared tags across sources; only syncs with a completed baseline participate, every completed ingestion reconciles status ownership, and last-claim removal waits for every participating sync. Pending, failed, stale, conflicting, and missing materialization states block convergence in Drift, Health, support evidence, recovery, and the read-only ownership audit. Post-sync analysis, scope/status, and parent work runs through NetBox JobRunner with generation guards; invoking users become durable sync owners, and unattended execution fails closed without attributable ownership. A branch merge with any failed row remains retryable and cannot mark the ingestion complete or enqueue overlays. Merge recovery updates authoritative branch state instead of trusting stale worker objects, and every queued Forward job enforces a two-hour timeout minimum while preserving larger operator values. One Branching branch is used per sync; bounded workload shards target that branch, conservative density baselines shape unprofiled work, and module bays are created branch-natively. Migration `0037` converts retired execution keys, standing schedules, endpoint scope markers, owners, and built-in virtual-chassis state once; runtime rejects retired or unknown configuration instead of carrying compatibility shims. Unsupported no-op ACI maps and inventory contracts are removed. NetBox `4.6.5`, netbox-branching `1.1.1`, and Python `3.14` are exact startup, CI, and packaging requirements. Orphan deletion remains reviewed and manual. Python/UI plus schema and data migrations. Upgrades from any pre-2.6 release must run **Publish Bundled Queries** once with overwrite enabled before validation.

## v2.5.11

Fix: **post-2.5.10 DLM and scope convergence** - optional DLM maps seed when `netbox-dlm` migrates after Forward NetBox; runtime arguments are projected onto each NQE signature, including preflight; CVEs retain valid metadata when an optional advisory URL is malformed; observed vulnerabilities populate affected software; and platforms receive a manufacturer only when ownership is unambiguous. Fix: **endpoint and CIMC identity** - generic SNMP endpoints remain off unless explicitly enabled, imported console endpoints inherit their matched include tags, CIMCs are excluded from standalone devices and can map to exact-parent inventory through a disabled opt-in map, and legacy Opengear/Avocent software-bearing DeviceTypes are reported for reviewed cleanup. Fix: **upgrade and drift evidence** - Scope Reconciliation and support evidence classify stale catalog records, while dependency previews report workload upper bounds as not-measured drift. After upgrading, run **Publish Bundled Queries** once with overwrite enabled. Python and NQE; no migration.

## v2.5.10

Fix: **DLM CVE/Vulnerability execution** - remove unsupported `@primaryKey` annotations stacked with parameterized `@query` declarations; Forward rejected both published queries before reading CVE data. After upgrading, run **Publish Bundled Queries** once with overwrite enabled. These parameterized maps require the default **Allow full fallback** mode; Forward's diff endpoint rejects them without a primary-key annotation, and this runtime cannot combine that annotation with `@query`. Fix: **DLM installed-software associations** - device-scoped software rows now carry lifecycle dates and create the SoftwareVersion plus DeviceSoftware link together; the standalone catalog map can only enrich an associated version, so versions from Forward-only devices no longer appear with zero devices. Vulnerability imports ensure the same association. Fix: **SNMP endpoint safety and identity** - imported endpoints use the same scope rules during reconciliation; endpoint tag intersections feed NetBox scope tags; legacy sources with include tags fail closed to endpoint include scope until they explicitly save an opt-out; new sources default it on. Avocent and Opengear endpoints use stable hardware DeviceTypes and Console Server roles instead of software-bearing sysDescr strings. Endpoint probe failures abort reconciliation, and the UI/audit payload report endpoint counts. Fix: **architecture contract gate** - `invoke architecture-audit-check` now runs focused model, fetch, and query contract tests instead of calling a management command removed in 2.0. Fix: **drift report semantics** - dependency-preview workload estimates are labeled as apply work, not exact drift, so candidate rows and deletes are no longer double-counted or reported as mostly drift. Fix: **CIMC identity** - CIMC SNMP endpoints are excluded from standalone device import while the APIC CIMC inventory map remains authoritative. Fix: **release smoke validation** - restore the removed `forward_smoke_sync` command on the current single-branch backend with automatic existing-source selection and redacted evidence. Security: require `pyzipper` 0.4.0+, which removes plaintext-revealing CRC32 values from small AES-encrypted support-bundle entry headers (`PYSEC-2026-3044`). Python and NQE; no migration.

## v2.5.9

Fix: **DLM/query reliability** - Health now detects enabled optional maps whose models are not selected, base/alias hardware-notice mismatches, and missing DLM dependency readiness; dependency-related skips are rolled up into actionable ingestion issues. Fix: **live query-path publishing** - Publish Bundled Queries now updates the Forward Org Repository, clears stale direct query IDs, preserves explicit commit pins, and binds matching enabled maps to paths that resolve current head at each sync; the separate Refresh Query IDs action is removed. After upgrading, run **Publish Bundled Queries** once for each org-backed sync. Python-only; no migration and no bundled NQE source change.

## v2.5.8

Feature: **device CVE tab** — with netbox-dlm installed and the Vulnerability feed enabled, each device with findings gets a **CVEs** tab (severity totals + one row per CVE: id, severity, affected software version, description); registered only when netbox-dlm is present, hidden when a device has no findings (live-verified against netbox-dlm 0.2.0). Feature: **`forward_routing_dangling_audit`** read-only command reports netbox-routing BGP rows whose device references dangle (the post-prune sweep only covers plugin-pruned devices). Feature: **stability + scale hardening (all opt-in or default-identical, no query change)** — (1) `forward_stuck_job_recover` command recovers a sync wedged by a dead worker (idempotent merge requeue, bounded retries, or clean fail so schedules resume); (2) opt-in per-workload wall-clock **fetch budget** (`workload_fetch_timeout_seconds`) + circuit breaker so a slow shard can't silently hang a multi-hour sync; (3) opt-in **shard-key bucket-packing** (`enable_branch_budget_split`) splits an oversized unsharded model into co-located branch plan items, with an always-on warning when a workload exceeds budget; (4) bulk-apply **per-row isolation** (tree-model + virtual-chassis) so one bad row no longer rolls back a whole model batch, all unbounded `__in` lookups chunked, and event-queue hygiene so a failed isolated row's change events don't leak. Fix: REST `PATCH` of the standing-schedule intent keys now reconciles immediately; transactional schedule persist; a latent per-item stats-reset bug in the branch executor. Python-only; no NQE query change.

## v2.5.7

Feature: **standing schedules land in the UI** — the sync form's new **Standing Schedules** section sets recurring validation / dependency-preview intervals (blank disables), the sync detail page shows each schedule and its next run, and the intent is stored on the sync so schedules **self-heal**: recreated at the end of every sync run after a hard-killed worker, re-checked by every occurrence (a cancelled or re-configured chain stops or re-aligns itself — no more zombie or duplicate schedules from mid-run changes), and schedules created on 2.5.6 are adopted automatically. Cancel from the form (blank the field) or `POST {"interval": 0}`. Fix: **cron-friendly responses** — an already-active equivalent job now answers `202 {"status": "already_running"}` instead of `409` (idempotent for retry-blind schedulers, matching the webhook), prune during a sync run answers a distinct `202 {"status": "blocked_by_sync_run"}` (that prune did NOT run), idempotent schedule re-posts answer `200`, and schedule bodies on non-schedulable actions are rejected. Fix: recurring validation now **trims its own history** (newest 100 runs per sync; `PLUGINS_CONFIG["forward_netbox"]["validation_run_retention"]`, 0 disables). Fix: prune's `pruned_dangling_rows` tally and the 60-minute preview floor are enforced on raw parameter writes too. Python-only; no query change.

## v2.5.6

Feature: **operator-job automation** — the four operator buttons (dependency preview, prune orphans, tag delete-eligible IPAM, create module bays) are now REST actions (`POST /api/plugins/forward/sync/<id>/dependency-preview|prune-orphans|tag-delete-eligible-ipam|create-module-bays/`) with the button's permission, `201` + job on success and `409` when an equivalent job is already queued or running — duplicates never stack across HTML buttons, API calls, scheduled occurrences, and the post-sync ` (auto)` prune (shared overlap guard under an advisory lock). Feature: **standing schedules** — `validate` and `dependency-preview` accept `{"interval": minutes}` (optional future `schedule_at`) to keep one recurring schedule per sync: same-parameter re-posts are no-ops, changed parameters replace the schedule, cancel by deleting the scheduled job; the preview enforces a 60-minute interval floor (schedule it daily or less often — it is a full live dry-run); snapshot catch-up and drift lookups are name-scoped so schedules never suppress them, and deleting a sync cancels its scheduled jobs. Fix: duplicate **Validate** clicks/POSTs no longer stack concurrent validation runs (`409`/warning like the button jobs). Fix: pruning now also sweeps **dangling netbox-routing rows** (BGP routers/scopes/address-families/peers/settings whose generic references pointed at pruned devices) in dependency order and reports them as `pruned_dangling_rows`. Python-only; no query change.

## v2.5.5

Feature: **push-triggered sync (webhooks)** — trigger a sync from an external webhook (e.g. Forward firing on snapshot processed). Preferred path is the NetBox-native token-authenticated `POST /api/plugins/forward/sync/<id>/sync/`; for senders that cannot set an `Authorization` header, a new `POST .../sync/<id>/webhook/` endpoint authenticates with a per-sync **Webhook secret** (sync form, empty = disabled; `X-Forward-Webhook-Secret` header or `?secret=` fallback), is opaque on failure, and acknowledges an already-running sync without re-queueing so retries stay idempotent. Fix: **prune orphans no longer fails on protected plugin references** — with netbox-routing (or another optional plugin) in play, deleting out-of-scope devices hit `ProtectedError` (e.g. BGP peers whose peer/source IPs live on a pruned device's interfaces) and the single-transaction prune rolled back **everything**. The prune now sweeps the exact PROTECT-ing rows Django reports (children first, per the delete dependency order, plugin-agnostic) and retries, uses one transaction **per batch** so one stuck batch can't void the rest, and reports what it swept as `pruned_dependent_rows` in the job data and the reconciliation audit. A blocker owned by an in-scope neighbor (its peer FK targets a pruned device's IP) is swept too — the next sync recreates it from Forward. Python-only; no query change.

## v2.5.4

Fix: **tag-scoped SNMP endpoint + missing-interfaces clarity** — (1) new opt-in **Scope SNMP Endpoints by Include Tags** source toggle: imported endpoints must also carry the device include tags ("all"/"any" per the include match; default off preserves the 2.4.4 import-all-endpoints behavior; exclude tags always apply). Query change; **Publish Bundled Queries** + Refresh Query IDs after upgrading. (2) A tag scope matching **0 collected devices** while endpoints still import now logs an explicit warning — that state made devices appear while interfaces/IP addresses stayed empty (they require collected devices in scope; check the snapshot selector, e.g. `latestCollected`). (3) Duplicate device names across sites no longer fail the apply workload with `MultipleObjectsReturned` — the by-name lookup resolves deterministically to the earliest device and warns.

## v2.5.3

**Editions** — `forward-netbox` is now one package with two install profiles: core (`pip install forward-netbox`, NetBox-builtin models only, no optional-plugin dependencies) and integrations (`pip install forward-netbox[integrations]`, or per-plugin `[dlm]`/`[routing]`/`[aci]`/`[peering]`) which install the opt-in netbox-dlm / netbox-routing / netbox-cisco-aci / netbox-peering-manager maps (still disabled until the plugin is installed and enabled). Fix: enabling the **netbox-routing** models no longer crashes the sync with `TypeError: '<' not supported between instances of 'NoneType' and 'int'` — the BGP/OSPF dependency-lookup cache sorted scope keys whose global-table VRF pk is `None` against a VRF peer on the same router/device; the sort is now None-safe. Fix: **Drift Report clarity** — the report replays a cached dependency-preview, so a stale or empty-baseline preview could read as real drift (field report: 18/19 models showing 100% pending). Now (1) an **empty-baseline hint** when every model shows all Forward rows pending with zero removals (the "preview ran before data was ingested/merged" signature — it is everything Forward has, not real mismatches), (2) the **staleness banner** also fires when the preview is over a day old (not only when a newer sync ran since), and (3) a **Preview Dependencies** button on the report to recompute on the spot. Fix: **DLM hardware-notice skips** — operators running the **alias-aware** device query saw netbox-dlm hardware notices skipped (`device type ... is not in NetBox yet`) because the notice looked up the raw Forward model while the aliased device query created the DeviceType under its NetBox-library name; a new opt-in **Forward DLM Hardware Notices with NetBox Aliases** map applies the same alias mapping (live-verified: the 24 device types that skipped now resolve). If you run the aliased device query, enable that variant instead of the base one and **Publish Bundled Queries** after upgrading. The editions/routing/drift changes are Python only; the hardware-notice variant is the only query change.

## v2.5.2

Feature: optional **netbox-dlm CVE + Vulnerability** feed — two new opt-in NQE maps import Forward's security analysis into the netbox-dlm plugin: the **CVE catalog** (`network.cveDatabase.cves`, worst-case per-vendor severity mapped to the plugin's severity choices) and **per-device vulnerabilities** (`device.cveFindings`, one row per device↔CVE). Disabled by default; requires the netbox-dlm plugin (0.2.0+ ships migrations — run `migrate`; 0.1.0 needs `makemigrations netbox_dlm` first). The Vulnerability map is large (~16 rows/device) — enable it scoped or on a fresh branch first. Fix: **SNMP endpoint platform unification** — Avocent/Cyclades/AlterPath (enterprise OIDs `10418` + `2925` plus product-name signatures) now resolve to a single `Avocent` platform instead of fragmenting across `Avocent`/`AlterPath`/`SNMP`; a multiline `sysDescr` is whitespace-collapsed so it can't leak a junk platform name, and a missing `sysDescr` falls back to `Unknown` rather than a fake `SNMP` vendor. Query-only endpoint change; **Publish Bundled Queries** after upgrading.

## v2.5.1

Fix: rows with a blank `device_type` were rejected with `model: This field cannot be blank` — a device with no resolved model (`device.platform.model` null) and, more commonly, an SNMP endpoint reporting an empty `sysDescr`. The bundled queries now guard both (null-safe/empty-safe fallbacks to `Unknown` / `SNMP Endpoint`) instead of dropping the row (live-verified: 0 blank device types across 5645 rows). Query-only change; **Publish Bundled Queries** after upgrading.

## v2.5.0

Feature: optional **netbox-dlm** (Device Lifecycle Management) integration — three new opt-in NQE maps sync Forward's end-of-life analysis into the netbox-dlm plugin: OS software versions with vendor EOL dates per (platform, version), hardware end-of-life notices per device type (Cisco/Palo Alto/Fortinet part support), and each device's running software version. Disabled by default; requires the netbox-dlm plugin (run `makemigrations netbox_dlm && migrate` after installing it — it ships no migrations). Fix: syncs no longer crash mid-provision when an installed plugin's migrations were never applied (`relation ... does not exist`) — a preflight now fails in seconds with the app name and remedy, and a new **Database tables** Health check surfaces the gap before you sync.

## v2.4.5

Fix: sync no longer crashes on netbox-branching **1.1.1** (`SquashMergeStrategy has no attribute '_split_bidirectional_cycles'` — 1.1.1 removed that internal helper; the bidirectional-cycle split is now built into the plugin and the dependency is bounded to `<1.2`). Also fixes SNMP-endpoint rows failing validation: the bundled endpoint query branches now clamp sysDescr-derived `device_type` to NetBox's 100-char limit (`substring`) and guard empty slugs — the fix lives in the NQE queries (the source of truth), so **Publish Bundled Queries** again after upgrading (fixes the `Ensure this value has at most 100 characters` rejects and the `At least one coalesce lookup must be provided` error).

## v2.4.4

Fix: SNMP-endpoint import now works on **tag-scoped** syncs — the device-tag include scope silently excluded every endpoint, both query-side and in the plugin's local scope filter (whose scoped-device set was built from modeled devices only, so endpoint rows were always dropped; with prune enabled they would even be deleted). Endpoint import now ignores the include scope (exclude tags still apply) and endpoint names join the scoped set (validated live: 355 Avocent endpoints import under a tag-scoped sync). Also fixes the merge-phase `Tag with this Name already exists` issues: a same-named/same-slug tag already on main is now treated as merged instead of failing the branch's tag create.

## v2.4.3

Fix: the pinned-query opt-in Health warning (2.4.1) over-claimed failure — it reads "nothing new syncs" and fires on any pinned map with the feature on, even after the query is fixed, because the Health page can't read a pinned query's contents. Reworded to a "Pinned — can't verify locally" heads-up that points at **Export Live Query Drift** to confirm (`source_matches_bundled`), instead of asserting failure. No behavior change.

## v2.4.2

Fix: endpoint import (`sync_endpoints`) and device-tag sync (`sync_device_tags`) now work with the alias-aware and rules-aware query variants (`forward_devices_with_netbox_aliases`, `forward_device_feature_tags_with_rules`), not just the base queries — operators running the variants saw the toggles silently do nothing (validated live: 355 Avocent endpoints import; `Mgmt_*` tags sync). Adds a **Publish Bundled Queries** button on the sync Health page (beside Refresh Query IDs) and two Health warnings: when an opt-in feature is enabled but no enabled map provides it, and when a base query and its opt-in variant are both enabled (they double-apply rows for the same model and churn — enable one). The alias-aware device query now emits the clean role name (e.g. `ROUTER`) to match the base query — expect a one-time role update on alias-mapped devices.

## v2.4.1

Fix: opt-in features (SNMP endpoint import, device-tag sync) silently did nothing on sources that run org-managed **pinned** Forward query IDs predating the feature — the sync Health page now raises an actionable warning instead of a silent badge. Remediation: publish the bundled queries to your Forward org folder (Overwrite on), then use Refresh Query IDs, then re-sync.

## v2.4.0

Fix: the "Import SNMP Endpoints as Devices" toggle now renders on the source form (the field shipped in 2.3.2 but was not in any fieldset, so it never showed), letting operators enable endpoint import from the GUI.

## v2.3.2

Feature: optional import of Forward SNMP endpoints (e.g. Avocent console servers) as NetBox devices — off by default (`sync_endpoints`), enabled per source and scoped by the same device tags.

## v2.3.1



## v2.3.0

GA/enterprise hardening: encrypted Forward credential at rest, PyPI Trusted Publishing + SBOM, Prometheus metrics + stuck-job alert, populated-DB upgrade test, dead-code removal (multi_branch/density-learning), reliability fixes (jittered/Retry-After backoff, SaaS rate clamp, PK-anchored device prune), and supported-product framing. Drop-in from 2.2.5 — stored credentials auto-encrypt on save; rotating SECRET_KEY requires re-entering them.

## v2.2.5

Feature: operator-selectable **Sync Device Tags** — pick which Forward device tags (e.g. `Mgmt_*`) become NetBox device tags (replaces the hardcoded feature-tag set); Fix dependency-preview AttributeError + vsys job pile-up guard (hung pending); test/require NetBox 4.6.4

## v2.2.4

Hotfix: device-analysis NQE (bare foreach) errored refresh + CVE list; surface job errors into job.data

## v2.2.3

Field-feedback fixes: delete-count labeling, vsys/vdom auto-link, skip empty VRFs, per-device CVE list, churn pinpoint, query-ID status clarify

## v2.2.2

Fix 504 gateway timeouts on large syncs: stop recomputing change-explainability on every poll during a long merge + back off poll to 15s

## v2.2.1

Add read-only forward_apply_identity_audit diagnostic to pinpoint 1-created/1-deleted idempotency churn

## v2.2.0

Fix devices mis-assigned the ACI platform; link Palo vsys / Fortinet vdom firewalls to their physical chassis

## v2.1.5

Fix Prune orphans erroring on empty sites that still hold a VLAN/VM/prefix (delete only truly-empty sites)

## v2.1.4

Tag delete-eligible global IPAM (prefixes/VLANs/VRFs) for manual review

## v2.1.3

Prune empty orphan sites (zero devices + zero racks) alongside out-of-scope devices

## v2.1.2

Feature + docs: (1) new out-of-scope orphan health signal — the sync health summary now shows how many NetBox devices match none of the included Forward tags (removable via Scope Reconciliation -> Prune orphans), mirroring the backfilled signal, via a self-healing `forward-out-of-scope` device tag and a `?tag=forward-out-of-scope` filter; (2) docs: the "no covering prefix" diagnostic now names /32 and /128 host addresses (loopbacks, anycast, some VIPs), and the Operations Guide documents backfilled (in-scope, kept) vs out-of-scope (removable) devices. Drop-in from `2.1.1`.

## v2.1.1

Bugfix + diagnostics: (1) the IPv4/IPv6 IP queries global dedup now pins the chosen interface to the chosen device (mirroring the VRF and MAC dedup blocks), so a deduped global address can no longer be attributed to an interface on a different device — the source of spurious "target interface was not imported" skips; (2) new read-only `forward_primary_ip_audit` command buckets Mgmt_ primary-IP resolution per device (resolvable / device-not-in-netbox / interface-not-matched / interface-present-no-IP) to pinpoint why a device does not get a primary IP. Drop-in from `2.1.0`.

## v2.1.0

Feature: `forward_scope_ipam_audit` management command — a read-only audit listing network-global IPAM (prefixes, VLANs, VRFs) that NetBox holds but the sync's latest Forward fetch no longer reports, as manual-review candidates. Device-tag scope prune is device-derived and never removes global IPAM; this surfaces stale global objects without deleting anything (identity matching reuses the apply engine so verdicts match the sync). Drop-in from `2.0.8`.

## v2.0.8

Bugfix: progress bars now reach 100% on a completed sync. For relationship and two-phase models (cable+termination, device+primary_ip, module+moduletype, fhrp group+assignment) the per-model bar settled below 100% because the merge `total` counts ChangeDiff rows while `current` counts applied objects; a finished job now renders every model at 100%. Cosmetic only — no apply/merge/data change. Drop-in from `2.0.7`

## v2.0.7

Bugfixes + diagnostics: (1) a MAC whose target interface was not imported is now a benign aggregated skip like the IP path (with the canonical-name fallback), not a red `ForwardSearchError` failure; (2) the two benign IP diagnostics (filtered-unassignable, no-parent-prefix) collapse to one summary line each instead of a 20-row wall; (3) when a `require_diff` sync is blocked by a failed diff fetch, the block now names that cause and the `Allow full fallback` remedy. Drop-in from `2.0.6`

## v2.0.6

Bugfix: stop the pernicious FHRP-group sync churn. When a virtual IP is shared by two HSRP/VRRP groups (different group_id), the second group was created then immediately deleted every sync (VIP-conflict), so a fixed set of FHRP groups was added and removed on every run. The second group now persists with its interface assignment (the VIP stays attached to the first group; NetBox allows a VIP on only one group), and deleting a shared-VIP group no longer removes the other group's VIP. Drop-in from `2.0.5`

## v2.0.5

Branding + polish: the plugin is now presented as **Forward Field Integration** (NetBox plugin name, sidebar menu, docs/site titles). Adds a theme-aware Forward Networks logo + `#ff3506` accent bar at the top of the Source/Sync/Ingestion pages. Display-only: package `forward_netbox`, the `forward` URL prefix, NQE query names, and all APIs are unchanged. Drop-in from `2.0.4`

## v2.0.4

Patch: collapse the module-sync readiness warning wall into ONE summary. When module sync is enabled before a device's module bays exist in NetBox, every module row is skipped; 2.0.3 capped the per-row lines at 3, this replaces them entirely with a single actionable line per sync (total skipped + a few examples + the `forward_module_readiness` remedy). Other skip reasons are unchanged. No engine/schema/org changes; drop-in from `2.0.3`

## v2.0.3

Patch: (1) module-sync readiness warnings no longer flood the log — the per-row `module bay does not exist; run forward_module_readiness` skip is capped to a few examples plus a suppressed-count summary (was up to 20 near-identical lines per sync); (2) fixes the release `CI` gate (`CHANGELOG matches README`) that had been red since v1.7.2 — the generator no longer depends on git tag-date timing; (3) removes dead executor code (`ForwardFastBootstrapExecutor.run`) and refreshes stale internal docs. No engine/schema changes; drop-in from `2.0.2`

## v2.0.2

Patch: apply_device_scope_tags now works with multiple include tags in `any` match mode — each device is tagged with exactly the include tag(s) it carries (resolved per-device at fetch time), instead of skipping. Also silences the spurious `Skipping untagged VLAN 1` warning (VID 1 is NetBox's implicit access default and is intentionally not imported). No engine/schema changes; drop-in from `2.0.1`, no org republish

## v2.0.1

Patch: fixes two 2.0.0 regressions an operator hits immediately — a false `netbox_branching is not installed; syncs will fail` startup warning (the dependency check used the wrong distribution name), and a 500 on the Sync list page (`KeyError: 'available'` from a removed execution-ledger summary). No engine or data changes; drop-in upgrade from `2.0.0`

## v2.0.0

Breaking 2.0 — single-branch is the only execution path. Removed the per-shard branching/fast-bootstrap/resumable executor, 10k-change budget sharding, and the execution-ledger run-history; dropped the backend/max-changes/scheduler-overlap selectors

## v1.7.2

Collection-gap diagnostics: per-reason backfill breakdown + staleness, growth/trend escalation, per-device collection result, ACI delete safety valve, opt-in auto-tag

## v1.7.1

ACI BD/L3Out graduation + FHRP churn fix (replaces yanked 1.7.0 and 1.6.2)

## v1.7.0

ACI bridge domain and L3Out NQE maps; query publish hardening

## v1.6.2

completes the 1.6.1 line (1.6.1 was yanked — its PyPI build predated these): device tag scope now covers VLANs/VRFs and prefixes derive from connected interface subnets; the FHRP group churn (delete+recreate every sync) is fixed by identity-bucket sharding; device analysis is a first-class model with a fleet list view, REST API, and an Open in Forward deep-link.

## v1.6.1

matures the 1.6.0 features and tooling — Device Analysis is now a NetBox model with a fleet-wide list view, REST API, and per-device-FK panel scoping (with up-interface blast-radius and opt-in post-sync refresh); adds a schedulable collection-gap alert command, run-history drill-down links, and hardened release tooling (one-command release script, generated CHANGELOG, conventional-commit hook).

## v1.6.0

ships the blue-sky tranche — release automation (`invoke release`), an Operations Guide, a collection-gap health signal, a sync run-history panel, a read-only device analysis panel (GA reachability / connectivity-degree blast radius / CVE exposure), and a bidirectional per-model drift report.

## v1.5.10

promotes `ipam.prefix` into the default bulk-ORM safe set (the last model still on the adapter path) — it runs the per-object tree apply so NetBox prefix hierarchy `_depth` stays correct, with null-VRF (global) prefix identity and canonical-CIDR matching parity-tested against the adapter.

## v1.5.9

adds a maintained `forward-backfilled` NetBox tag so operators can see which in-scope devices were backfilled (not freshly collected) in the latest snapshot — a Tag backfilled devices button on the Scope Reconciliation page plus a link to the filtered device list (`?tag=forward-backfilled`); the tag self-heals as devices collect again.

## v1.5.8

`dcim.module` sync now **adopts** the device interfaces Forward already syncs instead of recreating them (fixes `dcim_interface_unique_device_name` IntegrityError when modules are enabled), and `ipam.fhrpgroup` no longer churns (delete+recreate the same HSRP groups every sync) — the snapshot diff no longer deletes a group it is simultaneously upserting. Preview Dependencies now runs as a background job (cached result on the preview page), fixing a 504 timeout on large fabrics.

## v1.5.7

**Prune orphans** and **Create missing module bays** now run as background jobs (watch the Jobs tab) instead of synchronously, fixing a 504 gateway timeout on large fabrics. Module Readiness `Ready` reflects missing bays only (out-of-scope-device rows no longer hold it `No`), and the bulk `ipam.ipaddress` path tolerates duplicate global IPs.

## v1.5.6

fixes an `ipam.ipaddress` sync failure (`Ambiguous coalesce lookup`) when a reused /30 link range leaves duplicate global (VRF-less) IPs for the same host — the adapter now resolves to one deterministically (preferring the copy already on the synced interface) and warns, instead of failing the row.

## v1.5.5

surfaces the orphan-prune and module-bay readiness workflows in the sync detail UI (no CLI or CSV): a **Scope Reconciliation** page with a **Prune orphans** button, and a **Module Readiness** page with a **Create missing module bays** button that creates the bays directly in NetBox.

## v1.5.4

adds `--prune-orphans`/`--apply` to `forward_device_scope_reconciliation_audit` to delete stale out-of-scope (orphan) NetBox devices left by an earlier broader sync that `device_tag_prune_out_of_scope` cannot reach (orphans are absent from the scoped Forward result). Dry-run by default; tagged-but-backfilled devices are preserved.

## v1.5.3

classifies APIC controllers onto the `APIC` platform (distinct from ACI switches) so controller and switch software versions model separately; splits IP address import into independent `Forward IPv4 IP Addresses` and `Forward IPv6 IP Addresses` maps (a migration removes the combined map) so address families toggle independently; promotes `dcim.interface` and `ipam.ipaddress` into the default bulk-ORM safe set and removes bulk-apply update churn across every bulk model so steady-state syncs issue no redundant writes; preserves operator platform-manufacturer overrides on bulk update; and adds the opt-in `Apply Device Scope Tags` source option plus the `forward_device_scope_reconciliation_audit` and `forward_apic_cimc_readiness_audit` commands.

## v1.5.2

collapses the flood of `dcim.modulebay` branch-merge failures (a NetBox Branching/MPTT limitation when a new device's module bays are auto-instantiated in a branch) into a single actionable `ModuleBayMergeUnsupported` ingestion issue that points at the `forward_module_readiness` import workflow. Device and interface sync are unaffected.

## v1.5.1

adds the `latestCollected` snapshot selector that skips backfilled (collection-canceled) snapshots and resolves to the most recent snapshot with a freshly-collected in-scope device, warns when a `latestProcessed` run finds every in-scope device backfilled instead of silently applying zero changes, records the resolved snapshot's own metadata for `latestCollected` runs, and adds an Architecture Flow reference doc.

## v1.5.0.1

fixes platform NQE query using `normalizePlatformName` to avoid evaluation failures on unsupported vendor/OS combinations, adds `--overwrite` flag to the validation-org repair command, and hardens the NQE org-publish commit loop to retry after 409 INVALID_CHANGE_PATH.

## v1.5.0

hardens ingest throughput via adaptive async-NQE poll backoff, ndjson streaming, webhook/event-rule signal suppression during the apply loop, targeted validation (skips DB-hitting uniqueness checks on existing objects in both simple and tree-model bulk paths), and async advanced-reachability trigger (FWD-53559). Full test suite green on NetBox 4.5.9 (1092/0/0) and 4.6.2 (1092/0/26 routing-plugin version-gated).

## v1.4.3

hardens query-path provenance by requiring source-backed query-id repair at preflight, enforces async NQE source parsing for 26.6 execution paths, proves CIMC/APIC custom-command updates in source and keeps the 1.4 production-hardening line intact.

## v1.4.2

adds CIMC platform separation, visible query-drift repair and dependency preview on the sync detail page, and keeps the module-bay merge hardening plus parent-interface description preservation from the prior patch line.

## v1.4.1.1

prevents optional `dcim.module` sync from emitting merge-breaking `dcim.modulebay` side-effect creates when module bays are missing and prevents LAG member rows from clearing existing parent interface descriptions.

## v1.4.1

keeps the hard parent-device sync contract, adds query-ID drift remediation plus support-bundle diagnostics, and carries the 1.4 production-hardening tranche forward as the release line.

## v1.4.0

enforces a hard parent-device sync contract so child models cannot run without `dcim.device`, which prevents stale sync configs from skipping the device shard and breaking dependent imports.

## v1.3.5.5

adds compressed support-bundle ZIP downloads with optional password protection, and folds live source health, live query-drift, and live data-file diagnostics into the troubleshooting bundle so operator support can work from one artifact.

## v1.3.5.4

repackaged the `1.3.5.3` query-contract hardening on a fresh patch tag and kept strict shipped-query parameter-contract validation, legacy tag alias stripping, and summary-only support-bundle previews.

## v1.3.5.3

keeps the `1.3.5.2` claimed-step and payload compaction behavior, adds strict shipped-query parameter-contract validation, strips legacy tag aliases from runtime NQE payloads, and keeps support-bundle previews summary-only.

## v1.3.5.1

removes raw `model_results` from the sync telemetry summary and prevents unparameterized query IDs from receiving source-level tag parameters, which keeps the sync detail view responsive and preserves the saved-query-ID path compatibility.

## v1.3.5

keeps the 1.3.x saved-query-ID path parameter-compatible, tightens ACI platform detection with command-inventory signals, and preserves the lower-noise execution accounting used by the 1.3.x sync path

## v1.3.4

makes non-retryable Branching merge failures visible in job logs, leaves failed merge branches in a terminal `Failed` state instead of stale `Merging`, and carries disabled async NQE client staging for future Forward 26.6 support

## v1.3.3

refreshes bundled NQE syntax for saved query-ID execution, keeps all shipped maps parameter-compatible with `forward_netbox_shard_keys`, and updates the saved validation-folder query IDs used by the 1.3.x sync path

## v1.3.2

adds optional `netbox-cisco-aci` integration maps and adapter support, keeps ACI maps disabled by default, preserves parameterized NQE execution, and validates repeat-sync idempotence for the proven ACI write path

## v1.3.1

preserves the `v1.3.0` parameterized NQE path, removes the legacy sync column-filter shard path, and fixes repeat prefix sync accounting so unchanged `ipam.prefix` rows report as unchanged instead of update churn

## v1.3.0

eliminates default Forward NQE column-filter shard fetches in favor of query-side `forward_netbox_shard_keys` parameters, keeps local shard safety filtering, and preserves branch boundaries while reducing Forward SaaS API/NQE pressure

## v1.2.3

further reduced Forward SaaS API/NQE pressure by coalescing compatible sibling shard EQUALS_ANY filters, added local change-explainability summaries, and kept staged branch boundaries unchanged

## v1.2.1

fixes prefix VRF churn by making `ipam.prefix` identity exact for global and VRF-scoped rows while preserving parameterized prefix shard NQE execution

## v1.2.0

adds optional NetBox-native HSRP/VRRP FHRP import, bounded access/native interface VLAN assignment from existing site-scoped VLANs, upgrade-safe FHRP VIP conflict handling, and NetBox 4.6 job-test compatibility hardening while preserving the 1.1 API/NQE limits

## v1.1.1

adds optional NetBox-native HSRP/FHRP import, upgrade-safe FHRP VIP conflict handling, and NetBox 4.6 job-test compatibility hardening while preserving the 1.1 API/NQE limits

## v1.1.0

reduces Forward SaaS API/NQE pressure with source-level API pacing, parameterized prefix shard queries, single-pass interface NQE, and release-validation smoke evidence

## v1.0.0

first 1.x release line with API/NQE stability groundwork but without 1.1 API pacing and scale-optimized query improvements

## v0.9.4.6

tightens delete-heavy device cleanup shard planning after live evidence showed device deletes still exceeded native Branching change-budget guidance

## v0.9.4.5

plans delete-heavy device cleanup shards more conservatively so tag-scope prune runs stay closer to native Branching change-budget guidance

## v0.9.4.4

clarifies large branching progress by clamping progress-bar display and surfacing current shard row progress in the ingestion UI

## v0.9.4.3

hardens delete behavior by converting protected-reference delete failures into dependency skips so tag-scope prune/device cleanup runs continue safely

## v0.9.4.1.1

keeps the shared-branch architecture, execution ledger, support logging, and scale hardening while preserving the read-only advisory surfaces from `v0.9.0`

## v0.9.0

adds read-only analysis, workload preview, advisory summaries, and native log export for troubleshooting

## v0.8.6.3

hardens beta routing scope resolution, invalid ASN filtering, conservative virtual chassis skips, and fast-bootstrap baseline readiness when only optional model issues remain

## v0.8.6.2

hardens issue and job-log rendering so unexpected nested payload objects stay JSON-safe in the UI and API

## v0.8.6.1

clarifies the native NQE map bulk edit workflow so repository-path mode and runtime query-ID resolution are explicit in the UI

## v0.8.6

refreshes org-repository query publishing with flattened built-ins, filters invalid IPv4 prefix artifacts, adds parent-prefix diagnostics, and hardens virtual chassis/device and routing issue handling

## v0.8.5

makes the beta routing and module maps broadly available by default while keeping virtual chassis conservative, hardens repository query lookup responses, and clears stale row progress when a sync fails or advances phases

## v0.8.4

stops importing Forward HA peers as NetBox virtual chassis by default, hardens repository query lookup responses, and clears stale row progress when a sync fails or advances phases

## v0.8.3

isolates per-model query failures, blocks positionless virtual-chassis assignments before NetBox save, and lets later shards such as routing continue while withholding dirty diff baselines

## v0.8.2

adds portable repository query-path execution with native NetBox selectors, publish-and-bind bulk edit, bidirectional restore, and fixes IP address rows whose Forward interface cannot be resolved

## v0.8.1.1

fixes virtual chassis NQE output so NetBox receives a member position with virtual chassis assignments

## v0.8.1

fixes fast-bootstrap native change tracking/statistics and adds timeout guidance plus transient Forward API HTTP retries

## v0.8.0

adds an opt-in fast bootstrap backend for trusted large baselines while keeping Branching as default, and skips NetBox-invalid LAG cable endpoints

## v0.7.1

keeps the NetBox-native multi-branch workflow, adds shard heartbeat visibility, and hardens large-shard retries and cable ingestion handling

## v0.7.0

extracts the 0.7 sync boundaries and adds shard heartbeat visibility

## v0.6.5

adds audited validation force-allow overrides and routing evidence enrichment; optional routing/peering import remains beta; native `dcim.module` import is beta

## v0.6.4

optional routing/peering import is beta; native `dcim.module` import is beta

## v0.6.3

native `dcim.module` import is beta

## v0.6.2

native `dcim.module` import is beta

## v0.6.1

native `dcim.module` import is beta

## v0.6.0

native `dcim.module` import is beta

## v0.5.9.1

Superseded by `v0.6.0`

## v0.5.9

Superseded by `v0.5.9.1`

## v0.5.8

Superseded by `v0.5.9`

## v0.5.7

Superseded by `v0.5.8`

## v0.5.2.1

Superseded by `v0.5.3`

## v0.4.0

Superseded by `v0.5.2.1`

## v0.3.1

Superseded by `v0.4.0`

## v0.3.0.1

Superseded by `v0.3.1`

## v0.3.0

Superseded by `v0.3.0.1`
