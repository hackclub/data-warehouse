# Hackatime category compatibility release

## Deployable scope

This release adds only `TRUE AS legacy_coding_row_present` to
`public_hackatime_analytics.daily_activity` and `hourly_project_activity`.
Their row sets, metadata, `hackatime_hours`, case-sensitive coding filter,
previous-coding-heartbeat calculation, 120-second cap, rounding and hourly
ceiling remain unchanged. No category expansion or consumer filter ships here.
Scheduled consumers work before and after either table refresh: none references
the new flag yet. The deferred candidate is stored only as a non-executable patch.

The flag records whether a row belongs to the original coding-only row set.
It is always true in this release so future consumers can preserve that eligibility.
The expansion will add category-only rows with zero legacy `hackatime_hours`
and set the flag false on those new rows; it must not be treated as an activity total.

The category expansion is ordered follow-up `idea-7d500256`, replacing
`idea-ebcad5a5` whose local-hash recovery instructions are obsolete. Its entry gates
below must be completed there before its code can land. This compatibility
release does not satisfy the original request for category columns.

The candidate is preserved in [the expansion patch](patches/hackatime-category-expansion.patch),
outside dbt's model tree. It is an incomplete starting point, not deployable code.
Apply it from the repository root with `git apply runbooks/patches/hackatime-category-expansion.patch` in the follow-up only,
then resolve context changes and complete every gate below. The patch excludes
this runbook so these gates remain authoritative. No local commit hash is needed.

## First-pass consumer decisions

| Consumer | Recommendation for the expansion |
| --- | --- |
| `unified_analytics/events.sql`, `hackatimeActivity` | Stay on positive legacy coding hours; add a separately named event if all-activity events are wanted. |
| `spring_2026_analytics/unified_time_log.sql` | Stay on credited coding hours. Broadening program credit needs an explicit policy decision. |
| `summer_2026_analytics/summer_unified_time_log.sql` | Stay on credited coding hours, including project/identity restrictions. Filter the email map by the legacy flag in the expansion. |
| Summer hours and active-user dashboards derived from that log | Stay on the log's credit and eligibility policy. |
| `dashbored/app/services/dau_preview.py` | Stay on positive coding hours to match production eligibility. |
| `stardance_analytics/journey_user_stages.sql` | Stay on coding-heartbeat days, including zero-hour days; add the legacy flag filter in the expansion. |
| Two conditional-sum direct readers seen in `pg_stat_statements` | Unidentified readers: stay on existing row semantics until owners and predicates are identified and counterfactuals measured. This is an expansion blocker. |
| Metabase saved reports and readers using service logins | Named reports and their SQL are an access gap. No specific report can yet be recommended to move. This is an expansion discovery gate. |

No verified named consumer currently qualifies to move to `total_hours`.
A general all-activity report should use it, but none has been identified.
Legacy Hackatime models and unrelated application fields remain unchanged.

Earlier read-only discovery reported October daily totals of 115,280.34 legacy
hours (137,406 rows; 16,732 users) and hourly totals of 114,982.13 legacy hours
(584,568 rows; 16,135 users). These are mart-wide baselines, not consumer
counterfactuals. The supplied 252k versus 108k sample discrepancy is still
unexplained; no category-aware October result has been verified.

## Expansion entry gates

1. Replace the candidate's non-hashable `IS NOT DISTINCT FROM` joins with
   hashable nullable keys (value plus null flag). Verify a hash join and a
   100k-row benchmark before attempting production measurement.
2. The earlier October candidate query exceeded the **agent login's** 50 GiB
   temporary-file limit. Its cause was not established; nested-loop joins are
   a candidate cause, not proof of excessive history sorting. The agent login
   also has a 30-minute statement timeout. Scheduled syncs use a superuser
   login, which may have unlimited temporary space; the agent cap does not
   protect scheduled builds. Do not raise limits or retry the old query.
3. Before any production benchmark, review an exact bounded read-only route,
   login, timeout, temp budget and disk headroom. No four-hour benchmark is
   authorized by this runbook. An elevated route needs a separate runbook step
   naming it and a written reason under the production-health runbook.
4. Record per-model runtime within 60 minutes, the full Hackatime DAU chain
   within 90 minutes and all-assets completion with headroom inside six hours.
   Include materialization, contention, overlapping jobs, locks, deadlocks,
   free disk and failed-run durations. Stop for design review if unverified.
5. Identify each direct conditional-sum reader by owner and predicate. Inspect
   named saved reports or disclose the access gap. Give each a final move/stay
   decision and aggregate before/after users, days and hours with its real
   identity joins, project restrictions, thresholds and deduplication. Preserve
   pre-October heartbeat context. Publish no individual data.
6. Test category-only rows, AI spelling normalization, NULL/unknown categories,
   duplicate-time id ordering and actual Summer/Stardance query eligibility.
   Tests must fail when those consumer filters are removed. Use TCP PostgreSQL
   readiness to avoid connecting to the image's temporary setup server.
   Preserve the zero-fill fix: `COALESCE` nullable legacy hours to zero on
   category-only rows, and nullable category measures to zero on legacy-only
   rows. Assert zero rather than NULL for every hour measure on unmatched rows.
   Restore model descriptions documenting the expanded row sets and false flag
   on category-only rows; check the hourly one-hour ceiling explicitly.
7. Update both query-tool schema descriptions with the expansion. Verify this
   compatibility release's flags exist before deploying consumers that use
   them. Record all gates before landing expansion; schedules must never be
   relied on to enforce a prose-only hold.

## Deployment after review and authorization

Fetch current main, rebase (main history has changed), inspect the final diff
and rerun synthetic checks. Use the existing Coolify deployment process.
Refresh the two marts through Dagster using the existing asset jobs. Consumers
need no pause or coordinated refresh because their SQL is unchanged.
Verify the flag exists and is true on every row, and compare all original
columns and grains against pre-refresh baselines for closed historical days.
Exclude recent days and use an identical heartbeat cutoff/source snapshot for
both comparisons; incoming or late-arriving heartbeats must not be mistaken
for a regression. Roll back code and rebuild
these two tables if verification fails. No source data or host settings change.
Do not apply or deploy the deferred patch as part of this release.

## Local validation

`python3 -m unittest orpheus_engine_tests.test_hackatime_category_hours`
executes original and compatibility SQL on synthetic data in PostgreSQL 17.
It checks exact legacy values/metadata, unchanged row counts, true flags, no
category columns and no premature consumer dependencies. Docker has
`--network=none`, no published ports or persistent volumes, and is stopped by
the suite. No live credentials are used. These tests establish compatibility,
not expansion correctness or production performance.
