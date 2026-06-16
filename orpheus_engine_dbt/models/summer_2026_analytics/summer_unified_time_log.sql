{{ config(
    schema='summer_2026_analytics',
    materialized='table'
) }}

-- ============================================================================
-- Summer 2026 — Unified Time Log (hourly, UTC) with cross-program dedup
--
-- Port of spring_2026_analytics.unified_time_log, scoped to the Summer 2026
-- comparison set and extended with per-program run windows.
--
-- Programs:
--   stardance  — current Summer 2026 program (ongoing, launched ~2026-05-31).
--   flavortown — prior program (ran ~2025-12-24 to 2026-05-01), kept for the
--                historical comparison.
--   summer_of_making — Summer of Making 2025 (ran 2025-06-16 to 2025-10-02),
--                kept for the year-over-year historical comparison.
--   shipwrecked — Shipwrecked 2025 (ran ~2025-05-28 to 2025-09-03, island
--                event 2025-08-08..11), kept for the year-over-year
--                historical comparison.
--   siege      — Siege (ran 2025-08-31 to ~2026-04-07), kept for the
--                historical comparison.
--   athena_award — Athena Award (girls-focused, ran ~2025-05-21 to 2026-02,
--                Airtable-backed app), kept for the year-over-year
--                historical comparison.
--   neighborhood — Neighborhood 2025 (ran ~2025-05 to 2025-07), kept for
--                the year-over-year historical comparison.
--   blueprint  — hardware/PCB program (ongoing, launched ~2025-09-23).
--   hack_club_the_game — gamified ship-anything program (ongoing, beta opened
--                ~2026-01-16, first public approvals 2026-02-17).
--   construct  — 3D/modeling program (ongoing, app activity began 2025-12).
--   shiba      — game-making program (ran ~2025-08 to 2026-02), Airtable-backed.
--   midnight   — game-making program (ran ~2025-11 to 2026-04), Hackatime-backed.
--   stack, offtrack, beest, stasis, horizons — public Summer 2026 programs
--                with coding/work-session activity in their own app mirrors.
--   sleepover  — Airtable-backed program (ongoing, launched ~2026-01-16);
--                Hackatime-only, ported from spring_2026_analytics.
--   high_seas  — High Seas (winter 2024, ran ~2024-10 to 2025-01-31),
--                Airtable-backed; Hackatime claims via ships.wakatime_project_name.
--                Kept for the year-over-year historical comparison.
--   arcade     — Arcade (Summer 2024, ran ~2024-06 to 2024-09), Airtable-backed;
--                custom time from live "Hack Hour" session minutes.
--   juice      — Juice (Jan-May 2025, juice.hackclub.com), Airtable-backed;
--                custom time from live work "stretches".
--
-- Sources per program:
-- OUTPUT DAU COLUMNS (see section 8): every row carries dau and dau_deduped,
--   fractional weights that spread each person's "1" across their many rows so a
--   SUM() aggregates back to a headcount. dau is the within-program share
--   (SUM over a program-day = that program's distinct users); dau_deduped is the
--   across-all-programs share (SUM over a day = the unique overall DAU, SUM over
--   a program-day = the hours-weighted cross-program split). Stack dau_deduped
--   for a "unique overall DAU by program" chart.
--
--   Hackatime coding — global hackatime heartbeats, attributed to a program by
--                      the user's claimed project alias (user_hackatime_projects).
--   Custom logging   — program-native devlogs, journals, creator posts, and
--                      activity telemetry where those are the daily source.
--   Daily-grain /    — programs whose native source is not hourly Hackatime or
--   activity-only      custom time: macondo (daily_project_activity rollup),
--                      horizons app-native daily activity (from 2026-04-22),
--                      fallout (hardware build timelapses + journals), and
--                      highway (GitHub commit days, no durations). Added in
--                      section 6b; see there for grain/identity/dedup handling.
--                      This makes the model the single activity log for every
--                      Summer 2026 program.
--
-- DEDUP (equal split), inspired by spring:
--   If the same coding hour is claimed by multiple programs — either because the
--   same (hour, user, alias) is claimed by >1 program, OR because a project's
--   repo URL is shared across >1 program on the same day — the raw hours are
--   divided equally among them. credited_hours_logged = raw_hours_logged /
--   split_factor, where split_factor = GREATEST(alias_claim_count, url_program
--   _count). This keeps a single coding session from inflating multiple programs.
--
--   WINDOW-AWARE: unlike spring (which assumes every program is concurrently
--   live), dedup here only counts a program for an hour it is actually in-window
--   for, and URL overlap is scoped to the same calendar day. This is required
--   because the comparison set mixes a finished program (flavortown) with a live
--   one (stardance): a Hackatime alias never "unclaims", so without window-gating
--   a user who claimed the same alias in both would have stardance's current hours
--   wrongly split with — and flavortown wrongly kept alive by — the dead program.
--   The two run windows share no day, so nothing splits between them today; once
--   genuinely concurrent Summer programs are added, real overlaps split correctly.
-- ============================================================================

WITH program_windows AS (
    -- start inclusive, end exclusive (NULL end = ongoing)
    SELECT * FROM (VALUES
        ('flavortown', TIMESTAMP WITH TIME ZONE '2025-12-24 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2026-05-02 00:00:00+00'),
        ('stardance',  TIMESTAMP WITH TIME ZONE '2026-05-31 00:00:00+00',
                       NULL::timestamptz),
        ('stack',      TIMESTAMP WITH TIME ZONE '2026-05-01 00:00:00+00',
                       NULL::timestamptz),
        ('offtrack',   TIMESTAMP WITH TIME ZONE '2026-05-01 00:00:00+00',
                       NULL::timestamptz),
        ('beest',      TIMESTAMP WITH TIME ZONE '2026-04-06 00:00:00+00',
                       NULL::timestamptz),
        ('stasis',     TIMESTAMP WITH TIME ZONE '2026-03-03 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2026-07-01 00:00:00+00'),
        -- Horizons uses Hackatime claims only until its app-native daily
        -- activity rollup begins. Closing this unified-log window also keeps
        -- old Horizons Hackatime claims from splitting live programs after the
        -- daily_user_activity handoff.
        ('horizons',   TIMESTAMP WITH TIME ZONE '2026-02-22 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2026-04-22 00:00:00+00'),
        -- First projects 2026-01-16 (13 created launch day; spring used the
        -- 01-20 public launch, but the earlier wave is real participants).
        -- Still running: YSWS submissions through 2026-06-11, projects still
        -- being created in June. Revisit for a closed window once it ends.
        ('sleepover',  TIMESTAMP WITH TIME ZONE '2026-01-16 00:00:00+00',
                       NULL::timestamptz),
        -- Launched 2025-06-16 (first real devlog wave); devlogs stop after
        -- 2025-10-02, so the window closes 2025-10-03. The closed window also
        -- stops the never-unclaimed SoM aliases from splitting current programs.
        ('summer_of_making', TIMESTAMP WITH TIME ZONE '2025-06-16 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2025-10-03 00:00:00+00'),
        -- First journal entries 2025-09-23 (soft launch); still running.
        ('blueprint',  TIMESTAMP WITH TIME ZONE '2025-09-23 00:00:00+00',
                       NULL::timestamptz),
        -- First Hackatime claims 2026-01-16; the pre-public-launch claims
        -- (Jan 16 – Feb 8) are 65 real beta users + a handful of admins, so the
        -- window opens at the beta, not the first public approvals (2026-02-17).
        -- Still running.
        ('hack_club_the_game', TIMESTAMP WITH TIME ZONE '2026-01-16 00:00:00+00',
                       NULL::timestamptz),
        -- First HackatimeProjectLink rows 2025-05-28 (attributed activity ramps
        -- the same week); last unified-YSWS approval 2025-09-03, and program-db
        -- submissions/reviews collapse after September (33 vs 917 in July). The
        -- island event was 2025-08-08..11. CLOSED window required: the app
        -- stayed up and aliases never unclaim, so attributed Hackatime activity
        -- runs at ~100-170 hrs/week for months after the program ended — dead
        -- claims that would otherwise split hours with live programs.
        ('shipwrecked', TIMESTAMP WITH TIME ZONE '2025-05-28 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2025-09-04 00:00:00+00'),
        -- Siege: first ballots 2025-08-29, first projects 2025-09-01, first
        -- unified-YSWS approval 2025-08-31 (start). Core weekly game ended
        -- 2025-12-20 (last votes); project status changes ran to 2026-02-25
        -- and YSWS approvals to 2026-04-07, so the window closes 2026-04-08.
        -- CLOSED window required: the app's never-unclaimed aliases still
        -- accrue ~750 Hackatime h/month as of 2026-06, which must not credit
        -- to siege or split live programs.
        ('siege',      TIMESTAMP WITH TIME ZONE '2025-08-31 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2026-04-08 00:00:00+00'),
        -- First registrations 2025-05-21 (first unified-YSWS approvals
        -- 2025-05-27). Attributed activity peaks Oct 2025 (~4.0k hrs, the
        -- submission deadline rush), tapers Nov 2025 – Feb 2026 (387 -> 122
        -- hrs/mo, stragglers finishing; last approvals 2026-02-13), then drops
        -- to a ~30-75 hrs/mo dead-claim tail from March on. CLOSED window
        -- required: aliases never unclaim, so an open window would split hours
        -- with live programs forever.
        ('athena_award', TIMESTAMP WITH TIME ZONE '2025-05-21 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2026-03-01 00:00:00+00'),
        -- Milkyway (game-making, milkyway.hackclub.com): first signups and
        -- projects 2025-10-07 (beta trickle; public ramp Oct 9-11, first
        -- unified-YSWS approval 2025-10-28). New projects collapse after
        -- 2026-04-04 (stragglers to May 1), artlogs end 2026-04-08 (10 stray
        -- entries through June), and the last unified-YSWS approval is
        -- 2026-04-30 — so the window closes 2026-05-02. CLOSED window
        -- required: the site is still up (signups continue into June) and
        -- aliases never unclaim.
        ('milkyway',   TIMESTAMP WITH TIME ZONE '2025-10-07 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2026-05-02 00:00:00+00'),
        -- Neighborhood's Airtable Hackatime project rows start 2025-05-02;
        -- attributed Hackatime is concentrated May-Jul 2025 (23.9k hours),
        -- then collapses to a dead-claim tail (577h in Aug, <300h/month
        -- after). Unified-YSWS approvals run 2025-06-17..2025-07-29. CLOSED
        -- window required so never-unclaimed aliases do not split current
        -- programs.
        ('neighborhood', TIMESTAMP WITH TIME ZONE '2025-05-01 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2025-08-01 00:00:00+00'),
        -- Construct app activity begins 2025-12-02 (users/projects) and
        -- devlogs begin 2025-12-04; unified-YSWS approvals run through
        -- 2026-06 and the app is still active, so this window is open.
        ('construct',  TIMESTAMP WITH TIME ZONE '2025-12-02 00:00:00+00',
                       NULL::timestamptz),
        -- Shiba creator posts start 2025-08-18, unified-YSWS approvals run
        -- 2025-09-26..2025-12-01, creator posts fade after 2025-10, and the
        -- final logged-in play heartbeat is 2026-02-12. CLOSED historical
        -- window prevents old game pages/play telemetry from reviving Shiba.
        ('shiba',      TIMESTAMP WITH TIME ZONE '2025-08-18 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2026-02-13 00:00:00+00'),
        -- Midnight project rows start 2025-11-05, approvals run
        -- 2025-11-17..2026-04-21, and the app has no native daily activity
        -- table. CLOSED window prevents never-unclaimed aliases from crediting
        -- old Midnight claims after the program ended.
        ('midnight',   TIMESTAMP WITH TIME ZONE '2025-11-05 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2026-04-22 00:00:00+00'),
        -- Carnival (carnival.hackclub.com, Hackatime-tracked coding YSWS). The
        -- app's project↔alias linkage (project_hackatime_project) only carries a
        -- 2026-05 created_at (a migration artifact), so the window — not the
        -- linkage timestamp — gates time. In-window Hackatime for the linked
        -- aliases ramps 2025-12 (28h/7 users) -> 2026-01 (131h/16 users) and
        -- peaks Feb-May 2026; a handful of pre-program rows on reused alias names
        -- (~25 raw hrs across Aug-Nov 2025) sit below the start. The app is still
        -- active (devlogs/sessions through 2026-06-14), so the window is open.
        ('carnival',   TIMESTAMP WITH TIME ZONE '2025-12-01 00:00:00+00',
                       NULL::timestamptz),
        -- Moonshot (moonshot.hackclub.com, ended). App opened ~2025-10-25 (first
        -- HackatimeProjectLink rows), YSWS approvals 2025-11..12, and app activity
        -- (links/reviews/shop orders) collapses after December (Jan 2026: 8 links,
        -- 3 reviews vs Dec's 198/665). CLOSED window 2026-01-01 required: the app's
        -- aliases never unclaim, so in-window Hackatime keeps a dead-claim tail of
        -- ~250-330 hrs/mo through spring 2026 that must not split live programs.
        ('moonshot',   TIMESTAMP WITH TIME ZONE '2025-10-25 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2026-01-01 00:00:00+00'),
        -- High Seas (winter 2024, ended ~2025-01-31), Airtable-backed. First
        -- ships 2024-10-02 and the in-window Hackatime ramps from October (116
        -- users) to a Nov-Jan peak (~730 users / ~16k hrs/mo), then cliffs 6x
        -- from 2025-01-31 (686 hrs) to 2025-02-01 (113 hrs) at program end.
        -- CLOSED window 2025-02-01 required: aliases never unclaim, so the Feb+
        -- tail (~60-140 users/day) is dead-claim coding that must not split live
        -- programs. Opens 2024-10-01 (the Sep trickle is 7 users/41 hrs).
        ('high_seas',  TIMESTAMP WITH TIME ZONE '2024-10-01 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2025-02-01 00:00:00+00'),
        -- Arcade (Summer 2024, ended ~2024-09). sessions (live ~1-hour Hack
        -- Hour coding sessions) run 2024-06-17..2024-09-25; window opens
        -- 2024-06-16 to capture the pre-launch beta sessions and closes
        -- 2024-09-30 after the last session. No covered program overlaps this
        -- window, so Arcade adds with no dedup impact on existing programs.
        ('arcade',     TIMESTAMP WITH TIME ZONE '2024-06-16 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2024-09-30 00:00:00+00'),
        -- Juice (Jan-May 2025, juice.hackclub.com). Work "stretches" run
        -- 2025-01-24..2025-05-11; window opens 2025-01-24 and closes 2025-05-12
        -- after the last stretch. Overlaps only High Seas (Jan-Feb 2025, which
        -- is Hackatime-aliased) and the very start of neighborhood (May 2025);
        -- Juice has no Hackatime alias so it shares only same-repo-same-day URL
        -- dedup, which is negligible across these distinct programs.
        ('juice',      TIMESTAMP WITH TIME ZONE '2025-01-24 00:00:00+00',
                       TIMESTAMP WITH TIME ZONE '2025-05-12 00:00:00+00')
        -- Hackatime + custom devlog/journal time for coding programs is the
        -- credited-hours core of this model, but it is now the SINGLE activity
        -- log for ALL Summer 2026 programs: the daily-grain and activity-only
        -- sources (macondo, fallout, highway, and Horizons app-native activity
        -- from 2026-04-22 on) are unioned in below via section 6b, so
        -- daily_active_users / daily_hours_logged_by_program / the deduped DAU
        -- all read from here alone. Highway and fallout journal-only days carry
        -- zero credited hours (they are activity-only — see the final filter).
        -- Horizons keeps Hackatime claims here for pre-2026-04-22 history and
        -- switches to app-native user_daily_activity (section 6b) from
        -- 2026-04-22 onward; the run window below closes the Hackatime path at
        -- the handoff so the two never overlap.
    ) AS t(program_name, start_at, end_at_exclusive)
),

bad_aliases AS (
    SELECT alias FROM (VALUES
        (''), ('other'), ('<<last_project>>'), ('projects'), ('.wakatime'), ('.vscode')
    ) AS t(alias)
),

-- ============================================================
-- 1. HACKATIME HOURLY: raw coding hours per (hour, user, alias)
-- ============================================================
hackatime_hourly AS (
    SELECT
        DATE_TRUNC('hour', hpa.activity_time) AS activity_hour,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(hpa.hackatime_first_email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(hpa.hackatime_first_email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(hpa.hackatime_first_email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(hpa.hackatime_first_email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(hpa.project_name)) AS hackatime_alias,
        SUM(hpa.hackatime_hours) AS raw_hours_logged
    FROM {{ ref('hourly_project_activity') }} hpa
    WHERE hpa.hackatime_first_email IS NOT NULL
      AND hpa.project_name IS NOT NULL
      AND hpa.hackatime_hours > 0
      AND hpa.activity_time <= NOW()
    GROUP BY 1, 2, 3
),

-- Shared Hackatime user id -> first email map. Beest, Athena Award, and
-- Construct all use app Slack/Hackatime IDs rather than storing participant
-- email directly in the project-time source.
beest_htid_email AS (
    SELECT DISTINCT hackatime_user_id, hackatime_first_email
    FROM {{ ref('hourly_project_activity') }}
    WHERE hackatime_first_email IS NOT NULL
),

-- ============================================================
-- 2. CUSTOM TIME SOURCES (devlogs)
-- ============================================================
stardance_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', COALESCE(po.created_at, pd.created_at) AT TIME ZONE 'UTC') AS activity_hour,
        'stardance'::text AS program_name,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        ROUND(SUM(pd.duration_seconds)::numeric / 3600.0, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('stardance.post_devlogs; posts=' || COUNT(*)::text) AS source_detail
    FROM {{ source('stardance', 'post_devlogs') }} pd
    JOIN {{ source('stardance', 'posts') }} po
        ON po.postable_type = 'Post::Devlog' AND po.postable_id = pd.id
    JOIN {{ source('stardance', 'users') }} u ON u.id = po.user_id
    LEFT JOIN {{ source('stardance', 'projects') }} proj ON proj.id = po.project_id
    WHERE pd.duration_seconds > 0 AND pd.deleted_at IS NULL
    GROUP BY 1, 2, 3, 4, 5
),

flavortown_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', COALESCE(po.created_at, pd.created_at) AT TIME ZONE 'UTC') AS activity_hour,
        'flavortown'::text AS program_name,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        ROUND(SUM(pd.duration_seconds)::numeric / 3600.0, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('flavortown.post_devlogs; posts=' || COUNT(*)::text) AS source_detail
    FROM {{ source('flavortown', 'post_devlogs') }} pd
    JOIN {{ source('flavortown', 'posts') }} po
        ON po.postable_type = 'Post::Devlog' AND po.postable_id = pd.id
    JOIN {{ source('flavortown', 'users') }} u ON u.id = po.user_id
    LEFT JOIN {{ source('flavortown', 'projects') }} proj ON proj.id = po.project_id
    WHERE pd.duration_seconds > 0 AND pd.deleted_at IS NULL
    GROUP BY 1, 2, 3, 4, 5
),

-- Stack logs custom time via journal_entries (hours_worked), keyed off time_done
stack_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', je.time_done) AS activity_hour,
        'stack'::text AS program_name,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        je.project_name AS project_name,
        NULL::text AS code_url,
        SUM(je.hours_worked)::numeric AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('stack.journal_entries.hours_worked; entries=' || COUNT(*)::text) AS source_detail
    FROM {{ source('stack', 'journal_entries') }} je
    JOIN {{ source('stack', 'users') }} u ON u.id = je.user_id
    WHERE je.hours_worked > 0 AND je.time_done IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- Off-Track uses the same journal_entries schema as Stack
offtrack_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', je.time_done) AS activity_hour,
        'offtrack'::text AS program_name,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        je.project_name AS project_name,
        NULL::text AS code_url,
        SUM(je.hours_worked)::numeric AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('offtrack.journal_entries.hours_worked; entries=' || COUNT(*)::text) AS source_detail
    FROM {{ source('offtrack', 'journal_entries') }} je
    JOIN {{ source('offtrack', 'users') }} u ON u.id = je.user_id
    WHERE je.hours_worked > 0 AND je.time_done IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- Stasis logs custom/manual time as work sessions tied to projects.
stasis_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', ws."createdAt" AT TIME ZONE 'UTC') AS activity_hour,
        'stasis'::text AS program_name,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        proj.title AS project_name,
        NULLIF(BTRIM(proj."githubRepo"), '') AS code_url,
        SUM(ws."hoursClaimed")::numeric AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('stasis.work_session.hoursClaimed; sessions=' || COUNT(*)::text) AS source_detail
    FROM {{ source('stasis', 'work_session') }} ws
    JOIN {{ source('stasis', 'project') }} proj ON proj.id = ws."projectId"
    JOIN {{ source('stasis', 'user') }} u ON u.id = proj."userId"
    WHERE ws."hoursClaimed" > 0
    GROUP BY 1, 2, 3, 4, 5
),

-- Blueprint (hardware/PCB) logs self-reported time as journal entries, keyed
-- off created_at (there is no separate work timestamp). Blueprint has no
-- Hackatime integration (projects.hackatime_project_keys exists but is empty
-- for all 14.5k projects), so journals are its only time source.
--
-- DURATION SEMANTICS: the journal form asks "How many hours did you spend
-- since the last journal?", so an entry banks ALL time since the user's
-- previous post onto the posting date (same lumping as SoM devlogs). The app
-- enforces no server-side validation on duration_seconds (client-side max is
-- 1000h), so the data holds 999h test entries (incl. admin tests on live
-- projects), first-entry "whole project so far" dumps (avg 76h), and same-day
-- repeat-entry inflation (avg 154h/entry; one user: 81 templated 5h entries
-- in 4 hours). Reviewers set hours_override at review time and never sanitize
-- journal durations, so there is no cleaner ground truth to redistribute with.
--
-- Quality controls (audited 2026-06):
--   * banned users excluded — 65 banned users held 4.1% of hours.
--   * each entry is capped at 24h AND each user-day is proportionally rescaled
--     to a 24h total (entry spam reached 400h/user-day with only the per-entry
--     cap). The cap also truncates honest multi-week banked entries to 24h on
--     their posting day — accepted, since real and fabricated >24h claims are
--     indistinguishable here. DAU is unaffected by either cap.
--   * all entries are authored by the project owner (review-linked entries are
--     the owner's milestone submissions, verified by_other=0), and exact-dupe
--     entries are zero, so no further dedup is needed.
blueprint_journal_hourly AS (
    SELECT
        DATE_TRUNC('hour', je.created_at AT TIME ZONE 'UTC') AS activity_hour,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_link), '') AS code_url,
        SUM(LEAST(je.duration_seconds, 24 * 3600))::numeric / 3600.0 AS entry_hours,
        COUNT(*) AS entry_count
    FROM {{ source('blueprint', 'journal_entries') }} je
    JOIN {{ source('blueprint', 'users') }} u ON u.id = je.user_id
    LEFT JOIN {{ source('blueprint', 'projects') }} proj ON proj.id = je.project_id
    WHERE je.duration_seconds > 0
      AND NOT u.is_banned
    GROUP BY 1, 2, 3, 4
),

blueprint_custom_hourly AS (
    SELECT
        activity_hour,
        'blueprint'::text AS program_name,
        user_email,
        project_name,
        code_url,
        ROUND((entry_hours * LEAST(day_total_hours, 24) / day_total_hours)::numeric, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('blueprint.journal_entries.duration_seconds (capped 24h/entry + 24h/user-day); entries=' || entry_count::text) AS source_detail
    FROM (
        SELECT *,
            SUM(entry_hours) OVER (
                PARTITION BY user_email, (activity_hour AT TIME ZONE 'UTC')::date
            ) AS day_total_hours
        FROM blueprint_journal_hourly
    ) x
),

-- Milkyway (game-making) tracks art time as artlogs: self-reported hours
-- attached to a project, posted with proof images and reviewed afterwards
-- (approved_hours). Art time never reaches Hackatime, so artlogs are
-- Milkyway's custom-time source alongside the Hackatime claims below; raw
-- hours are credited (the reviewer pipeline approved 787 of 1,197 raw hours
-- but reviews lag and rejected hours are not adjudicated fraud, matching the
-- stasis/stack precedent of crediting claimed time). Devlogs are NOT counted:
-- devlogs.code_hours banks Hackatime time and devlogs.art_hours banks artlog
-- time accrued since the previous post (SoM double-count rationale), and
-- devlogs only existed 2025-11-17..2026-01-06 anyway.
--
-- Quality controls (audited 2026-06):
--   * banned users excluded — 15 banned users hold 14.1 of 1,120.6 linked art
--     hours (1.3%).
--   * 53 of 1,057 artlogs (5.0%) have no project link and are dropped — the
--     project is the only path to the author. Zero exact-dupe rows, zero
--     non-positive hours.
--   * the app validates entries server-side (max observed 21h, p99 7.9h,
--     none >24h), so the 24h/entry + 24h/user-day caps are no-op guards.
milkyway_artlog_hourly AS (
    SELECT
        DATE_TRUNC('hour', al.created AT TIME ZONE 'UTC') AS activity_hour,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
            THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                 || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
            ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        proj.projectname AS project_name,
        NULLIF(BTRIM(proj.github_url), '') AS code_url,
        SUM(LEAST(al.hours, 24))::numeric AS entry_hours,
        COUNT(*) AS entry_count
    FROM {{ source('airtable_milkyway', 'artlog') }} al
    JOIN {{ source('airtable_milkyway', 'projects') }} proj ON proj.id = al.projects
    JOIN {{ source('airtable_milkyway', 'users') }} u ON u.id = proj."user"
    WHERE al.hours > 0
      AND NOT COALESCE(u.is_banned, false)
    GROUP BY 1, 2, 3, 4
),

milkyway_custom_hourly AS (
    SELECT
        activity_hour,
        'milkyway'::text AS program_name,
        user_email,
        project_name,
        code_url,
        ROUND((entry_hours * LEAST(day_total_hours, 24) / day_total_hours)::numeric, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('milkyway.artlog.hours (capped 24h/entry + 24h/user-day); entries=' || entry_count::text) AS source_detail
    FROM (
        SELECT *,
            SUM(entry_hours) OVER (
                PARTITION BY user_email, (activity_hour AT TIME ZONE 'UTC')::date
            ) AS day_total_hours
        FROM milkyway_artlog_hourly
    ) x
),

-- Construct logs custom time in devlog.timeSpent, measured in minutes. The
-- source app is still live and its inspected production DB had 18,312 devlogs
-- on 2026-06-12 (16,374 non-deleted). Distribution was clean relative to
-- Blueprint-style free input: p50=31m, p90=90m, p99=120m, max=425m. Still, two
-- user-days exceeded 24h (max 1,714m), so the same safety shape is applied:
-- cap each entry at 24h and proportionally rescale each user-day to <=24h.
--
-- Quality controls (audited 2026-06):
--   * user.trust='blue' is required. Non-blue trust users held 451.4 of
--     11,093.8 raw devlog hours (4.1%); those are excluded. hackatimeTrust was
--     blue for almost everyone and is not used as the fraud gate.
--   * session.token and user.idvToken are excluded in the Sling mirror; this
--     model only needs Slack identity, trust, project, and devlog fields.
--   * Slack ID maps to Hackatime user -> email where possible so Construct can
--     participate in cross-program URL/user dedup. If no Hackatime email exists
--     for a Slack ID, a stable construct_user_<id> key preserves DAU/hours but
--     cannot dedup against other programs.
construct_slack_email AS (
    SELECT
        hu.slack_uid,
        MIN(
            CASE
                WHEN POSITION('@' IN LOWER(BTRIM(m.hackatime_first_email))) > 0
                THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '@', 1), '+', 1)
                     || '@' || SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '@', 2)
                ELSE SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '+', 1)
            END
        ) AS user_email
    FROM {{ source('hackatime_raw', 'users') }} hu
    JOIN beest_htid_email m ON m.hackatime_user_id = hu.id
    WHERE hu.slack_uid IS NOT NULL
    GROUP BY 1
),

construct_devlog_hourly AS (
    SELECT
        DATE_TRUNC('hour', d."createdAt" AT TIME ZONE 'UTC') AS activity_hour,
        COALESCE(cse.user_email, 'construct_user_' || u.id::text) AS user_email,
        proj.name AS project_name,
        NULLIF(BTRIM(proj.url), '') AS code_url,
        SUM(LEAST(d."timeSpent", 24 * 60))::numeric / 60.0 AS entry_hours,
        COUNT(*) AS entry_count
    FROM {{ source('construct', 'devlog') }} d
    JOIN {{ source('construct', 'user') }} u ON u.id = d."userId"
    LEFT JOIN {{ source('construct', 'project') }} proj ON proj.id = d."projectId"
    LEFT JOIN construct_slack_email cse ON cse.slack_uid = u."slackId"
    WHERE d."timeSpent" > 0
      AND NOT COALESCE(d.deleted, false)
      AND u.trust = 'blue'
    GROUP BY 1, 2, 3, 4
),

construct_custom_hourly AS (
    SELECT
        activity_hour,
        'construct'::text AS program_name,
        user_email,
        project_name,
        code_url,
        ROUND((entry_hours * LEAST(day_total_hours, 24) / day_total_hours)::numeric, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('construct.devlog.timeSpent minutes (trust=blue; capped 24h/entry + 24h/user-day); entries=' || entry_count::text) AS source_detail
    FROM (
        SELECT *,
            SUM(entry_hours) OVER (
                PARTITION BY user_email, (activity_hour AT TIME ZONE 'UTC')::date
            ) AS day_total_hours
        FROM construct_devlog_hourly
    ) x
),

-- Shiba's app-backed Airtable base is currently available only through the
-- generic airtable_raw_all_bases mirror (base appg245A41MWc6Rej, synced
-- 2026-06-12). It contains both creator posts and logged-in player telemetry.
--
-- Creator posts are the credited-hour source: 4,582 valid post rows / 266
-- users / 11,171.9 raw hours after selecting the greatest positive value from
-- HoursSpent, TimeSpentOnAsset, and InCaseHoursGoAway. The source has no
-- user-level ban flag, but Games.Banned by Fraud Dept is an adjudicated project
-- flag; 94 post rows / 2 users / 262.2h are excluded. Ten entries exceed 24h
-- (max 55.2h) and three user-days remain over 24h after entry capping, so the
-- standard 24h/entry + 24h/user-day cap is applied.
--
-- User Activity is 15-second heartbeat/play telemetry. Only rows with
-- PlayerSlackId are counted; CreatorSlackId identifies the game owner and is
-- not the active user. This adds logged-in play/test DAU (290 users / 61.8h)
-- without attributing anonymous play to project owners.
-- Final modeled validation after fraud exclusion + caps: 408 users / 10,896.8h
-- (10,838.1h creator posts + 58.7h play telemetry).
shiba_users AS (
    SELECT
        LOWER(BTRIM(r.fields->>'slack id')) AS slack_id,
        MIN(
            CASE
                WHEN POSITION('@' IN LOWER(BTRIM(r.fields->>'Email'))) > 0
                THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '@', 1), '+', 1)
                     || '@' || SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '@', 2)
                ELSE SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '+', 1)
            END
        ) AS user_email
    FROM {{ source('airtable_raw_all_bases', 'records') }} r
    WHERE r.base_id = 'appg245A41MWc6Rej'
      AND r.table_id = 'tblfD2WCyqAIOBBbz' -- Users
      AND NULLIF(BTRIM(r.fields->>'slack id'), '') IS NOT NULL
      AND NULLIF(BTRIM(r.fields->>'Email'), '') IS NOT NULL
    GROUP BY 1
),

shiba_games AS (
    SELECT
        r.record_id AS game_id,
        COALESCE((r.fields->>'Banned by Fraud Dept')::boolean, false) AS banned_by_fraud_dept
    FROM {{ source('airtable_raw_all_bases', 'records') }} r
    WHERE r.base_id = 'appg245A41MWc6Rej'
      AND r.table_id = 'tblrmmG1si4Ti9KlT' -- Games
),

shiba_post_rows AS (
    SELECT
        DATE_TRUNC('hour', NULLIF(r.fields->>'Created At', '')::timestamptz AT TIME ZONE 'UTC') AS activity_hour,
        COALESCE(su.user_email, 'shiba_slack_' || LOWER(BTRIM(s.slack_id))) AS user_email,
        COALESCE(
            NULLIF(
                CASE
                    WHEN jsonb_typeof(r.fields->'Game Name') = 'array' THEN r.fields->'Game Name'->>0
                    ELSE r.fields->>'Game Name'
                END,
                ''
            ),
            'Shiba creator post'
        ) AS project_name,
        COALESCE(
            NULLIF(
                CASE
                    WHEN jsonb_typeof(r.fields->'GitHubUrl') = 'array' THEN r.fields->'GitHubUrl'->>0
                    ELSE r.fields->>'GitHubUrl'
                END,
                ''
            ),
            NULLIF(BTRIM(r.fields->>'Link to Github Asset'), '')
        ) AS code_url,
        LEAST(
            GREATEST(
                COALESCE(NULLIF(r.fields->>'HoursSpent', '')::numeric, 0),
                COALESCE(NULLIF(r.fields->>'TimeSpentOnAsset', '')::numeric, 0),
                COALESCE(NULLIF(r.fields->>'InCaseHoursGoAway', '')::numeric, 0)
            ),
            24
        ) AS entry_hours,
        'posts'::text AS activity_source
    FROM {{ source('airtable_raw_all_bases', 'records') }} r
    CROSS JOIN LATERAL (
        SELECT jsonb_array_elements_text(
            CASE
                WHEN jsonb_typeof(r.fields->'slack id') = 'array' THEN r.fields->'slack id'
                WHEN r.fields ? 'slack id' THEN jsonb_build_array(r.fields->'slack id')
                ELSE '[]'::jsonb
            END
        ) AS slack_id
    ) s
    LEFT JOIN LATERAL (
        SELECT jsonb_array_elements_text(
            CASE
                WHEN jsonb_typeof(r.fields->'Game') = 'array' THEN r.fields->'Game'
                WHEN r.fields ? 'Game' THEN jsonb_build_array(r.fields->'Game')
                ELSE '[]'::jsonb
            END
        ) AS game_id
    ) g ON true
    LEFT JOIN shiba_users su ON su.slack_id = LOWER(BTRIM(s.slack_id))
    LEFT JOIN shiba_games sg ON sg.game_id = g.game_id
    WHERE r.base_id = 'appg245A41MWc6Rej'
      AND r.table_id = 'tblrkDBkvpySnSiHB' -- Posts
      AND NULLIF(r.fields->>'Created At', '') IS NOT NULL
      AND GREATEST(
            COALESCE(NULLIF(r.fields->>'HoursSpent', '')::numeric, 0),
            COALESCE(NULLIF(r.fields->>'TimeSpentOnAsset', '')::numeric, 0),
            COALESCE(NULLIF(r.fields->>'InCaseHoursGoAway', '')::numeric, 0)
          ) > 0
      AND NOT COALESCE(sg.banned_by_fraud_dept, false)
),

shiba_play_rows AS (
    SELECT
        DATE_TRUNC('hour', NULLIF(r.fields->>'Timestamp', '')::timestamptz AT TIME ZONE 'UTC') AS activity_hour,
        COALESCE(su.user_email, 'shiba_slack_' || LOWER(BTRIM(s.player_slack))) AS user_email,
        'Shiba play activity'::text AS project_name,
        NULL::text AS code_url,
        LEAST(COALESCE(NULLIF(r.fields->>'Time Spent (seconds)', '')::numeric, 0) / 3600.0, 24) AS entry_hours,
        'play_activity'::text AS activity_source
    FROM {{ source('airtable_raw_all_bases', 'records') }} r
    CROSS JOIN LATERAL (
        SELECT jsonb_array_elements_text(
            CASE
                WHEN jsonb_typeof(r.fields->'PlayerSlackId') = 'array' THEN r.fields->'PlayerSlackId'
                WHEN r.fields ? 'PlayerSlackId' THEN jsonb_build_array(r.fields->'PlayerSlackId')
                ELSE '[]'::jsonb
            END
        ) AS player_slack
    ) s
    LEFT JOIN LATERAL (
        SELECT jsonb_array_elements_text(
            CASE
                WHEN jsonb_typeof(r.fields->'Game') = 'array' THEN r.fields->'Game'
                WHEN r.fields ? 'Game' THEN jsonb_build_array(r.fields->'Game')
                ELSE '[]'::jsonb
            END
        ) AS game_id
    ) g ON true
    LEFT JOIN shiba_users su ON su.slack_id = LOWER(BTRIM(s.player_slack))
    LEFT JOIN shiba_games sg ON sg.game_id = g.game_id
    WHERE r.base_id = 'appg245A41MWc6Rej'
      AND r.table_id = 'tblFe4Jf66a70xhlf' -- User Activity
      AND NULLIF(r.fields->>'Timestamp', '') IS NOT NULL
      AND COALESCE(NULLIF(r.fields->>'Time Spent (seconds)', '')::numeric, 0) > 0
      AND NOT COALESCE(sg.banned_by_fraud_dept, false)
),

shiba_activity_hourly AS (
    SELECT
        activity_hour,
        user_email,
        project_name,
        code_url,
        activity_source,
        SUM(entry_hours)::numeric AS entry_hours,
        COUNT(*) AS entry_count
    FROM (
        SELECT * FROM shiba_post_rows
        UNION ALL
        SELECT * FROM shiba_play_rows
    ) s
    GROUP BY 1, 2, 3, 4, 5
),

shiba_custom_hourly AS (
    SELECT
        activity_hour,
        'shiba'::text AS program_name,
        user_email,
        project_name,
        code_url,
        ROUND((entry_hours * LEAST(day_total_hours, 24) / day_total_hours)::numeric, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('shiba.airtable.' || activity_source || ' (capped 24h/entry + 24h/user-day); entries=' || entry_count::text) AS source_detail
    FROM (
        SELECT *,
            SUM(entry_hours) OVER (
                PARTITION BY user_email, (activity_hour AT TIME ZONE 'UTC')::date
            ) AS day_total_hours
        FROM shiba_activity_hourly
    ) x
),

-- Arcade (Summer 2024) is Airtable-backed: the arcade__summer_24 mirror IS the
-- program db. sessions is Arcade's live activity log — one ~1-hour "Hack Hour"
-- coding session per row, with Hackatime-tracked minutes, created_at (session
-- start) and a reviewer status. Sessions were logged live as users coded, so
-- they are the daily leading indicator and session minutes are the time source.
-- Identity is the participant's normalized email (users.email; sessions link to
-- users by the user record link, which joins 100% of sessions).
--
-- Quality controls (audited 2026-06-14):
--   * 'Rejected'/'Rejected Locked' sessions, session-level fraud = [true], and
--     users carrying any open_fraud_case (Arcade's fraud flags) are excluded —
--     together 16.8% of raw session hours and 107 fully-excluded users.
--   * 'Unreviewed' sessions (the program ended before review finished; ~5.4k
--     users, the largest status by user count) are KEPT as claimed live
--     activity, consistent with crediting claimed time where review lags
--     (stasis/stack/milkyway precedent). Raw minutes are credited, NOT the
--     reviewer-haircut approved_minutes (which is 0 for unreviewed and would
--     erase those users).
--   * no session-day exceeds the shared 24h/user-day cap (a no-op guard here).
--     Kept: 5,476 users / 103.4k raw hours / peak DAU 645.
arcade_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', s.created_at) AS activity_hour,
        'arcade'::text AS program_name,
        u.user_email,
        NULL::text AS project_name,
        NULLIF(BTRIM(s.code_url), '') AS code_url,
        ROUND(SUM(s.minutes)::numeric / 60.0, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('arcade.sessions.minutes; sessions=' || COUNT(*)::text) AS source_detail
    FROM {{ source('airtable_arcade', 'sessions') }} s
    JOIN (
        SELECT record_id,
            CASE WHEN POSITION('@' IN LOWER(BTRIM(email))) > 0
                 THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(email)), '@', 1), '+', 1)
                      || '@' || SPLIT_PART(LOWER(BTRIM(email)), '@', 2)
                 ELSE SPLIT_PART(LOWER(BTRIM(email)), '+', 1)
            END AS user_email,
            open_fraud_case
        FROM {{ source('airtable_arcade', 'users') }}
        WHERE email IS NOT NULL AND BTRIM(email) <> ''
    ) u ON u.record_id = (CASE WHEN jsonb_typeof(s."user") = 'array'
                               THEN s."user"->>0 ELSE s."user"#>>'{}' END)
    WHERE s.minutes > 0
      AND s.status NOT IN ('Rejected', 'Rejected Locked')
      AND NOT COALESCE(s.fraud @> '[true]'::jsonb, false)
      AND u.open_fraud_case IS NULL
    GROUP BY 1, 2, 3, 4, 5
),

-- Juice (Jan-May 2025, juice.hackclub.com) is Airtable-backed: the juice mirror
-- IS the program db. juice_stretches and jungle_stretches are Juice's live work
-- sessions ("stretches") — start_time/end_time and time_worked_seconds tracked
-- live as the user worked (pauses excluded; total_pause_time_seconds is a
-- separate field). These are the daily leading indicator, so stretch seconds are
-- the time source, bucketed by start_time. Identity is the participant's
-- normalized email (email_from_signups_, present on 99.9% of stretches).
--
-- Quality controls (audited 2026-06-14):
--   * is_canceled stretches (canceled / abandoned sessions) are excluded.
--   * Juice has NO ban/fraud flag anywhere in its base, so no fraud exclusion is
--     applied; is_canceled plus the shared 24h/user-day cap (27 user-days
--     exceeded it, shaving 331 of 20,640 raw hours) are the only controls.
--   * jungle_stretches (the in-app "Jungle" mini-game's work sessions) is unioned
--     in — both are real Juice work time and never share a stretch row.
--     Kept: 471 users / ~20.3k hours / peak DAU 114.
juice_custom_hourly AS (
    SELECT
        DATE_TRUNC('hour', st.start_time) AS activity_hour,
        'juice'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(st.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(st.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(st.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(st.email)), '+', 1)
        END AS user_email,
        st.project_name,
        NULL::text AS code_url,
        ROUND(SUM(st.secs)::numeric / 3600.0, 4) AS raw_hours_logged,
        'custom'::text AS logging_method,
        ('juice.' || MIN(st.src) || '.time_worked_seconds; stretches=' || COUNT(*)::text) AS source_detail
    FROM (
        SELECT
            (CASE WHEN jsonb_typeof(email_from_signups_) = 'array'
                  THEN email_from_signups_->>0 ELSE email_from_signups_#>>'{}' END) AS email,
            start_time, time_worked_seconds AS secs, is_canceled,
            'Juice stretch'::text AS project_name, 'juice_stretches'::text AS src
        FROM {{ source('airtable_juice', 'juice_stretches') }}
        UNION ALL
        SELECT
            (CASE WHEN jsonb_typeof(email_from_signups_) = 'array'
                  THEN email_from_signups_->>0 ELSE email_from_signups_#>>'{}' END),
            start_time, time_worked_seconds, is_canceled,
            'Juice jungle stretch'::text, 'jungle_stretches'::text
        FROM {{ source('airtable_juice', 'jungle_stretches') }}
    ) st
    WHERE st.secs > 0
      AND NOT COALESCE(st.is_canceled, false)
      AND NULLIF(BTRIM(st.email), '') IS NOT NULL
      AND st.start_time IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- ============================================================
-- 3. HACKATIME CLAIMS: per-program alias -> project mapping
-- ============================================================
stardance_ht_claims AS (
    SELECT 'stardance'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(hp.name)) AS hackatime_alias,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        hp.created_at AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('stardance', 'user_hackatime_projects') }} hp
    JOIN {{ source('stardance', 'users') }} u ON u.id = hp.user_id
    LEFT JOIN {{ source('stardance', 'projects') }} proj ON proj.id = hp.project_id
    WHERE hp.name IS NOT NULL AND hp.name <> ''
),

flavortown_ht_claims AS (
    SELECT 'flavortown'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(hp.name)) AS hackatime_alias,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        hp.created_at AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('flavortown', 'user_hackatime_projects') }} hp
    JOIN {{ source('flavortown', 'users') }} u ON u.id = hp.user_id
    LEFT JOIN {{ source('flavortown', 'projects') }} proj ON proj.id = hp.project_id
    WHERE hp.name IS NOT NULL AND hp.name <> ''
),

-- Beest links Hackatime by users.hackatime_user_id (NOT email), and stores
-- projects.hackatime_project_name as a JSON array string (e.g. '["my-proj"]').
-- Map the hackatime user id to the Hackatime email so beest joins the shared
-- (user_email, alias) matching path, and unnest the alias array.
beest_ht_claims AS (
    SELECT 'beest'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(m.hackatime_first_email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(alias.alias_text, ' "')) AS hackatime_alias,
        proj.name AS project_name,
        NULLIF(BTRIM(proj.code_url), '') AS code_url,
        proj.created_at AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('beest', 'projects') }} proj
    JOIN {{ source('beest', 'users') }} u ON u.id = proj.user_id
    JOIN beest_htid_email m ON m.hackatime_user_id::text = u.hackatime_user_id
    CROSS JOIN LATERAL (
        SELECT jsonb_array_elements_text(proj.hackatime_project_name::jsonb) AS alias_text
        WHERE proj.hackatime_project_name ~ '^\s*\[.*\]\s*$'
        UNION ALL
        SELECT proj.hackatime_project_name
        WHERE NOT (proj.hackatime_project_name ~ '^\s*\[.*\]\s*$')
    ) AS alias
    WHERE proj.hackatime_project_name IS NOT NULL AND proj.hackatime_project_name <> ''
      AND u.hackatime_user_id IS NOT NULL AND u.hackatime_user_id <> ''
),

stasis_ht_claims AS (
    SELECT 'stasis'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(hp."hackatimeProject")) AS hackatime_alias,
        proj.title AS project_name,
        NULLIF(BTRIM(proj."githubRepo"), '') AS code_url,
        hp."createdAt" AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('stasis', 'hackatime_project') }} hp
    JOIN {{ source('stasis', 'project') }} proj ON proj.id = hp."projectId"
    JOIN {{ source('stasis', 'user') }} u ON u.id = proj."userId"
    WHERE hp."hackatimeProject" IS NOT NULL AND hp."hackatimeProject" <> ''
),

horizons_ht_claims AS (
    -- Horizons has app-native daily user activity from 2026-04-22 onward; that
    -- is used directly in the daily rollups. This claim bridge is retained for
    -- earlier history only. Quality controls (audited 2026-06): exclude Joe
    -- fraud-failed projects (134 claimed projects / 203 alias rows). User-level
    -- is_fraud/is_sus flags were 0 at audit time but remain as future gates.
    SELECT 'horizons'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(alias.alias_text, ' "')) AS hackatime_alias,
        proj.project_title AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        proj.created_at AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('horizons', 'projects') }} proj
    JOIN {{ source('horizons', 'users') }} u ON u.user_id = proj.user_id
    CROSS JOIN LATERAL unnest(proj.now_hackatime_projects::text[]) AS alias(alias_text)
    WHERE proj.now_hackatime_projects IS NOT NULL
      AND proj.now_hackatime_projects <> '{}'
      AND NOT (
          proj.joe_fraud_reviewed_at IS NOT NULL
          AND NOT COALESCE(proj.joe_fraud_passed, false)
      )
      AND NOT COALESCE(u.is_fraud, false)
      AND NOT COALESCE(u.is_sus, false)
),

-- Midnight uses the same K8S app family as Horizons. It has app sessions and
-- approved/banked submission hours, but no program-native daily activity table;
-- daily credited time comes from explicit projects.now_hackatime_projects
-- aliases joined to global Hackatime. submissions.approved_hours is a review
-- total and is not counted as daily activity.
--
-- Quality controls (audited 2026-06): projects.is_fraud excludes 19 projects.
-- users.is_fraud currently excludes 0 users but is kept as a future-proof user
-- fraud gate. Eligible source claims were 2,348 aliases / 812 users; in-window
-- Hackatime matched 611 users / 11,549.7 raw hours, with one Hackatime user-day
-- over 24h (24.65h).
midnight_ht_claims AS (
    SELECT 'midnight'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(alias.alias_text, ' "')) AS hackatime_alias,
        proj.project_title AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        proj.created_at AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('midnight', 'projects') }} proj
    JOIN {{ source('midnight', 'users') }} u ON u.user_id = proj.user_id
    CROSS JOIN LATERAL unnest(proj.now_hackatime_projects::text[]) AS alias(alias_text)
    WHERE proj.now_hackatime_projects IS NOT NULL
      AND proj.now_hackatime_projects <> '{}'
      AND NOT COALESCE(proj.is_fraud, false)
      AND NOT COALESCE(u.is_fraud, false)
),

-- Summer of Making 2025 attaches Hackatime aliases to projects via
-- projects.hackatime_project_keys (a text[] mirrored as text, like horizons).
-- NOTE: SoM also has a hackatime_projects table, but it is a full mirror of
-- every Hackatime project for every user who signed in (no project FK, still
-- syncing today) — using it would credit ALL of a user's coding to SoM, so the
-- explicit per-project keys are used instead. Devlog duration_seconds is NOT
-- counted as custom time: SoM devlogs bank Hackatime time accrued since the
-- previous devlog, so counting both would double count, and devlog-dated time
-- lumps multi-day work onto the posting date. Validated 2026-06: 97.8% of
-- devlog authors match Hackatime by normalized email; the claims path yields
-- 116k hours / 75k user-days vs 146k hours / 31k user-days devlog-dated (the
-- gap is pre-program Neighborhood-migrated time and over-banked devlogs).
--
-- Quality controls (audited 2026-06): banned users are excluded. SoM's fraud
-- wave left 461 banned users holding 28.2% of credited hours (32k of 114k);
-- the app's own devlog record shows the same share, and the fraud team also
-- deleted those users' projects (banned ∩ all-claims-deleted = 31.9k of the
-- 38.9k deleted-claim hours). Non-banned users' deleted projects are kept,
-- consistent with the other programs (deletion alone is not a fraud signal).
summer_of_making_ht_claims AS (
    SELECT 'summer_of_making'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(alias.alias_text, ' "')) AS hackatime_alias,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_link), '') AS code_url,
        proj.created_at AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('summer_of_making_2025', 'projects') }} proj
    JOIN {{ source('summer_of_making_2025', 'users') }} u ON u.id = proj.user_id
    CROSS JOIN LATERAL unnest(proj.hackatime_project_keys::text[]) AS alias(alias_text)
    WHERE proj.hackatime_project_keys IS NOT NULL
      AND proj.hackatime_project_keys <> '{}'
      AND NOT u.is_banned
),

-- Hack Club: The Game links Hackatime by users.hackatime_id (numeric id stored
-- as text, NOT email — 99.6% of claiming users match Hackatime by id vs 95.3%
-- by normalized email), so it reuses the beest htid->email map. Its
-- hackatime_projects table is MIXED: 93.6% of rows are a per-user Hackatime
-- mirror with NULL project_id (the SoM-mirror trap — crediting those would
-- attribute ALL of a user's coding to the program); only rows with a
-- project_id are explicit user claims, and those are what's used here.
-- projects.total_seconds / project_reviews.approved_seconds are review-time
-- banked Hackatime totals, not daily activity, so they are NOT counted as a
-- custom time source (SoM double-count rationale); Hackatime is the only
-- time source.
--
-- Quality controls (audited 2026-06):
--   * NOT u.is_banned follows the SoM/Blueprint precedent, but no user is
--     banned yet (is_banned=false for all 3,033 users; live program, mirror
--     refreshes daily) — currently 0% of hours.
--   * zero claims point at deleted projects, all claim rows belong to the
--     project owner, and dupe (user, project, alias) claims (14) collapse in
--     the all_claims GROUP BY.
hack_club_the_game_ht_claims AS (
    SELECT 'hack_club_the_game'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(m.hackatime_first_email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(hp.name)) AS hackatime_alias,
        proj.title AS project_name,
        NULLIF(BTRIM(proj.repo_link), '') AS code_url,
        hp.created_at AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('hack_club_the_game', 'hackatime_projects') }} hp
    JOIN {{ source('hack_club_the_game', 'projects') }} proj ON proj.id = hp.project_id
    JOIN {{ source('hack_club_the_game', 'users') }} u ON u.id = hp.user_id
    JOIN beest_htid_email m ON m.hackatime_user_id::text = u.hackatime_id
    WHERE hp.project_id IS NOT NULL
      AND hp.name IS NOT NULL AND hp.name <> ''
      AND u.hackatime_id IS NOT NULL AND u.hackatime_id <> ''
      AND NOT u.is_banned
),

-- Sleepover is an Airtable-backed app (no program db): projects carry
-- hackatime_name (a JSON array string like '["proj"]' for 1,437 projects,
-- plain text for 170 — audited 2026-06), and link to registered_users (email)
-- via the DLT junction table. Ported from spring_2026_analytics, which is the
-- reference implementation. Hackatime is the only time source: projects.hours
-- and ysws_project_submission hours are banked/reviewed totals, not daily
-- activity (SoM double-count rationale).
-- Sleepover's own banked project hours count post-program-launch Hackatime,
-- not only hours after the Airtable project row was created: re-audited
-- 2026-06-14, project-grain Hackatime from 2026-01-16 matches many
-- projects.hours rows exactly, while using projects.created_at as the claim
-- gate drops the same source-approved work. Therefore claim_start_ts is the
-- program launch, still filtered by explicit project↔alias linkage.
--
-- Quality controls (audited 2026-06):
--   * linkage is complete — all 1,609 alias-bearing project↔user rows resolve
--     to a registered user with an email; 0 alias projects lack a user link;
--     0 normalized-email collisions across registered_users ids.
--   * identity-join coverage: 600 of 634 claim users (94.6%) match a Hackatime
--     email after normalization (shipwrecked was 95.4%, SoM 97.8%).
--   * the base has no ban/fraud flag. verification_status is an identity-
--     verification state, not a fraud signal; 'ineligible' users hold 2 of
--     1,609 claim projects (~0.1%), so no exclusion is applied.
--   * scale re-audit: claimed aliases carry 10,586 lifetime Hackatime hours;
--     program-start gating counts 9,236 raw hours / 559 users; the old
--     projects.created_at gate counted only 3,345 raw hours / 447 users. At
--     project grain, banked projects.hours total 14,375; duplicated aliases
--     across projects explain why this exceeds the deduped claim-pair total.
-- code_url comes from the user's latest YSWS submission for the same-named
-- project (projects itself has no repo field).
sleepover_code_urls AS (
    SELECT y.userid,
           LOWER(BTRIM(y.project)) AS project_name_key,
           (ARRAY_AGG(NULLIF(BTRIM(y.code_url), '')
                      ORDER BY y.automation_first_submitted_at DESC NULLS LAST,
                               y.id DESC))[1] AS code_url
    FROM {{ source('airtable_sleepover', 'ysws_project_submission') }} y
    WHERE y.userid IS NOT NULL AND y.userid != ''
      AND y.project IS NOT NULL AND y.project != ''
      AND y.code_url IS NOT NULL AND y.code_url != ''
    GROUP BY 1, 2
),

sleepover_ht_claims AS (
    SELECT 'sleepover'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(ru.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(ru.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(ru.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(ru.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(alias.alias_text, ' "')) AS hackatime_alias,
        proj.name AS project_name,
        scu.code_url,
        TIMESTAMP WITH TIME ZONE '2026-01-16 00:00:00+00' AS claim_start_ts
    FROM {{ source('airtable_sleepover', 'projects') }} proj
    JOIN {{ source('airtable_sleepover', 'projects__registered_users') }} pr
        ON pr._dlt_parent_id = proj._dlt_id
    JOIN {{ source('airtable_sleepover', 'registered_users') }} ru
        ON ru.id = pr.value
    LEFT JOIN sleepover_code_urls scu
        ON scu.userid = proj.userid
        AND scu.project_name_key = LOWER(BTRIM(proj.name))
    CROSS JOIN LATERAL (
        SELECT jsonb_array_elements_text(proj.hackatime_name::jsonb) AS alias_text
        WHERE proj.hackatime_name ~ '^\s*\[.*\]\s*$'
        UNION ALL
        SELECT proj.hackatime_name AS alias_text
        WHERE NOT (proj.hackatime_name ~ '^\s*\[.*\]\s*$')
    ) AS alias
    WHERE proj.hackatime_name IS NOT NULL AND proj.hackatime_name != ''
),

-- Shipwrecked 2025 (Prisma schema, camelCase identifiers) attaches Hackatime
-- aliases to projects via HackatimeProjectLink (one row per alias). Hackatime
-- is the program's only real time source: the link's rawHours/hoursOverride
-- are banked/reviewer-adjusted Hackatime totals (counting them would double
-- count, same rationale as SoM devlogs), and User.purchasedProgressHours is
-- shop-bought progress, not time. claim_start_ts comes from the link's
-- createdAt — Project has no timestamp columns at all.
--
-- Quality controls (audited 2026-06): users the program's fraud review marked
-- User.status = 'FraudSuspect' are excluded — 43 such users held 14.1% of
-- in-window attributed hours (1,359 of 9,666). Deleted projects are already
-- absent from the mirror (hard deletes), and per the SoM precedent deletion
-- alone would not exclude anyway. Identity-join quality: 95.4% of claim users
-- (562 of 589) match a Hackatime email after normalization (SoM was 97.8%).
shipwrecked_ht_claims AS (
    SELECT 'shipwrecked'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(l."hackatimeName")) AS hackatime_alias,
        proj.name AS project_name,
        NULLIF(BTRIM(proj."codeUrl"), '') AS code_url,
        l."createdAt" AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('shipwrecked_the_bay', 'HackatimeProjectLink') }} l
    JOIN {{ source('shipwrecked_the_bay', 'Project') }} proj ON proj."projectID" = l."projectID"
    JOIN {{ source('shipwrecked_the_bay', 'User') }} u ON u.id = proj."userId"
    WHERE l."hackatimeName" IS NOT NULL AND BTRIM(l."hackatimeName") <> ''
      AND u.status <> 'FraudSuspect'
),

-- Siege (weekly project game, ran 2025-08-31 to ~2026-04-07) attaches
-- Hackatime aliases to projects via projects.hackatime_projects (a JSON array
-- of alias strings; every non-null value is an array). Hackatime-only
-- attribution: Siege has no self-reported time source — its hackatime_days
-- table is a per-DAY global rollup (no user/project grain, still updating
-- daily after program end), the per-user analog of the SoM hackatime_projects
-- trap, so it is not used.
--
-- Quality controls (audited 2026-06): banned users (users.status = 'banned';
-- 25 of the 436 alias-claiming users) held 774 of 15,221 raw in-window hours
-- (5.1%), and Siege's spot-check verdicts (projects.fraud_status = 'fraud';
-- 39 projects) held another 151 hours (1.0%) — both excluded. 'sus' (4
-- projects) and 'unchecked' are not adjudicated fraud and are kept. Identity
-- join coverage: of 405 eligible claim users, 94.3% have in-window Hackatime
-- activity by normalized email and 87.4% match on email+alias (354 users /
-- 14,295 raw hours credited pre-dedup). users.email has one duplicate pair
-- and no NULLs.
siege_ht_claims AS (
    SELECT 'siege'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(alias.alias_text)) AS hackatime_alias,
        proj.name AS project_name,
        NULLIF(BTRIM(proj.repo_url), '') AS code_url,
        proj.created_at AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('siege', 'projects') }} proj
    JOIN {{ source('siege', 'users') }} u ON u.id = proj.user_id
    CROSS JOIN LATERAL (
        SELECT jsonb_array_elements_text(proj.hackatime_projects::jsonb) AS alias_text
        WHERE jsonb_typeof(proj.hackatime_projects::jsonb) = 'array'
    ) AS alias
    WHERE proj.hackatime_projects IS NOT NULL
      AND u.status <> 'banned'
      AND COALESCE(proj.fraud_status, '') <> 'fraud'
),

-- Athena Award has no app database — its app reads/writes the Airtable base
-- directly, so the airtable_athena_award mirror IS the program db.
-- projects.project_name is the Hackatime alias the user picked in-app from
-- their own Hackatime projects; '_select#' is the untouched dropdown
-- placeholder (2,390 rows), and 'other ysws project' marks projects imported
-- from OTHER YSWS programs whose time belongs to those programs ('other' is
-- already in bad_aliases). projects.hackatime_duration / approved_duration
-- are banked snapshot totals, NOT counted as time (SoM double-count
-- rationale) — Hackatime is the only time source.
--
-- Identity: slack_id -> hackatime.users.slack_uid -> hackatime user id ->
-- first email (reuses beest_htid_email). Measured 2026-06: slack matches
-- 93.7% of duration-bearing claim pairs vs 90.2% by normalized email, and
-- email adds zero pairs beyond slack, so slack is the sole join; the residual
-- unmatched pairs are dominated by the other-YSWS placeholders excluded here.
-- Airtable holds no per-claim created timestamp (the created_at lookup on
-- registered_users is misaligned with its projects list — 93 of 323 array
-- lengths agree), so claim_start_ts is the user's registration timestamp:
-- activity before the user joined the program is never credited.
--
-- Quality controls (audited 2026-06):
--   * registered_users.disregard_submissions is the program's own fraud/spam
--     flag — 6 users holding 416 banked hours (3.1% of 13.6k) are excluded.
--     There is no is_banned column; admins (3 users, 33 banked hrs) are kept,
--     consistent with hack_club_the_game's beta users.
--   * rejected projects (232) are kept — rejection is a review outcome, not a
--     fraud signal (same precedent as keeping non-banned deleted projects).
--   * in-window attributed Hackatime (~13.8k raw hrs) agrees with the app's
--     own banked totals (~13.6k hrs) within ~2%.
athena_award_slack_email AS (
    SELECT DISTINCT hu.slack_uid, m.hackatime_first_email
    FROM {{ source('hackatime_raw', 'users') }} hu
    JOIN beest_htid_email m ON m.hackatime_user_id = hu.id
    WHERE hu.slack_uid IS NOT NULL AND hu.slack_uid <> ''
),

athena_award_ht_claims AS (
    SELECT 'athena_award'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(m.hackatime_first_email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(m.hackatime_first_email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(p.project_name)) AS hackatime_alias,
        p.project_name AS project_name,
        NULLIF(BTRIM(url.code_url), '') AS code_url,
        ru.registered AS claim_start_ts
    FROM {{ source('airtable_athena_award', 'projects') }} p
    JOIN {{ source('airtable_athena_award', 'registered_users') }} ru
        ON ru.record_id = p.registered_user
    JOIN athena_award_slack_email m ON m.slack_uid = p.slack_id
    LEFT JOIN (
        SELECT _dlt_parent_id, MIN(value) AS code_url
        FROM {{ source('airtable_athena_award', 'projects__code_url') }}
        GROUP BY 1
    ) url ON url._dlt_parent_id = p._dlt_id
    WHERE p.project_name IS NOT NULL
      AND LOWER(BTRIM(p.project_name)) NOT IN ('', '_select#', 'other ysws project')
      AND NOT COALESCE(ru.disregard_submissions, false)
),

-- Milkyway has no app database — like athena_award, the app reads/writes its
-- Airtable base directly, so the airtable_milkyway dlt mirror is the program
-- db. projects.hackatime_projects is the alias linkage the user set in-app: a
-- comma-separated list of the user's own Hackatime project names (973 of
-- 2,939 projects have one; 220 list several). claim_start_ts is
-- projects.counting_from, the app's own "count Hackatime from here" marker
-- (set on every project, = created for all but 35). projects.hackatime_hours
-- / total_hours are banked snapshot totals, NOT counted as time (SoM
-- double-count rationale); they also exceed the alias's observable Hackatime
-- (17.1k banked vs 12.6k all-time attributable) — cross-check only.
--
-- Quality controls (audited 2026-06):
--   * users.is_banned excluded — 8 of 433 matched claim users held 180 of
--     11.0k raw in-window attributed hours (1.6%).
--   * identity is normalized email (projects' user link -> users.email):
--     94.8% of claiming users (490 of 517) match Hackatime, in line with
--     shipwrecked (95.4%).
--   * counting_from trims 878 raw hours of pre-claim coding that an
--     unrestricted window join would have credited.
milkyway_ht_claims AS (
    SELECT 'milkyway'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(alias.alias_text)) AS hackatime_alias,
        proj.projectname AS project_name,
        NULLIF(BTRIM(proj.github_url), '') AS code_url,
        COALESCE(proj.counting_from, proj.created) AT TIME ZONE 'UTC' AS claim_start_ts
    FROM {{ source('airtable_milkyway', 'projects') }} proj
    JOIN {{ source('airtable_milkyway', 'users') }} u ON u.id = proj."user"
    CROSS JOIN LATERAL unnest(string_to_array(proj.hackatime_projects, ',')) AS alias(alias_text)
    WHERE proj.hackatime_projects IS NOT NULL
      AND BTRIM(alias.alias_text) <> ''
      AND NOT COALESCE(u.is_banned, false)
),

-- Neighborhood 2025 is Airtable-backed. airtable_neighborhood.hackatime_projects
-- is an explicit alias table linked to a neighbor (1,359/1,368 rows have a
-- neighbor, 1,208 have a GitHub URL), not a full per-user Hackatime mirror.
-- Hackatime is the only daily time source; total_time_hours on the Airtable
-- row is a banked aggregate and is not counted.
--
-- Quality controls (audited 2026-06): no ban/fraud flag exists in the
-- Neighborhood Airtable mirror. The review-like fields are operational:
-- hard_review=true covered 24 alias rows / 1,085 banked hours, and
-- staff_verified=true covered 325 alias rows / 9,331 banked hours; neither is
-- an adjudicated fraud signal. In-window Hackatime had 487 users / 23,735.4
-- hours and no user-day over 24h. Bad aliases (52 rows) are filtered by the
-- shared bad_aliases list.
neighborhood_ht_claims AS (
    SELECT 'neighborhood'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(hp.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(hp.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(hp.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(hp.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(hp.name)) AS hackatime_alias,
        COALESCE(NULLIF(BTRIM(n.project_name), ''), hp.name) AS project_name,
        NULLIF(BTRIM(hp.github_link), '') AS code_url,
        hp.created_at AS claim_start_ts
    FROM {{ source('airtable_neighborhood', 'hackatime_projects') }} hp
    LEFT JOIN {{ source('airtable_neighborhood', 'neighbors') }} n ON n.id = hp.neighbor
    WHERE hp.name IS NOT NULL AND BTRIM(hp.name) <> ''
      AND hp.email IS NOT NULL AND BTRIM(hp.email) <> ''
),

-- Carnival (carnival.hackclub.com) links Hackatime via project_hackatime_project
-- (one row per alias; 137 rows / 131 aliases over 132 projects). Hackatime is the
-- only daily time source: devlog.duration_seconds banks Hackatime time
-- (hackatime_pulled_at is set on every devlog), and project.hackatime_total_seconds
-- / hours_spent_seconds are banked snapshot totals — counting any of them next to
-- the Hackatime claims would double count (SoM rationale). Identity is the user's
-- normalized email (carnival.user.email; hackatime_user_id is set on only 83/570
-- users, so email is the join key).
--
-- claim_start_ts is the program launch (2025-12-01), NOT the linkage row's
-- created_at: every project_hackatime_project.created_at is 2026-05-04+ (the app
-- backfilled the table in May), so gating on it would wrongly drop Dec-Apr coding.
-- The program window does the real gating and trims pre-program coding on reused
-- alias names.
--
-- Quality controls (audited 2026-06-14):
--   * users.is_frozen is Carnival's ban flag — 0 of 570 users are frozen today,
--     but the gate is kept as a future-proof fraud exclusion (hctg/midnight
--     precedent).
--   * identity-join coverage: 59 of 63 alias-claiming users (93.7%) match a
--     Hackatime email after normalization (siege 94.3%, sleepover 94.6%).
--   * scale cross-check: in-window Hackatime for the linked aliases is ~1,071 raw
--     hours / 63 users pre-dedup, consistent with the app's own banked totals
--     (project.hackatime_total_seconds 834h; devlogs 315h) given the claims path
--     captures all in-window Hackatime, not just banked devlog snapshots.
carnival_ht_claims AS (
    SELECT 'carnival'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(php.name)) AS hackatime_alias,
        proj.name AS project_name,
        NULLIF(BTRIM(proj.code_url), '') AS code_url,
        TIMESTAMP WITH TIME ZONE '2025-12-01 00:00:00+00' AS claim_start_ts
    FROM {{ source('carnival', 'project_hackatime_project') }} php
    JOIN {{ source('carnival', 'project') }} proj ON proj.id = php.project_id
    JOIN {{ source('carnival', 'user') }} u ON u.id = proj.creator_id
    WHERE php.name IS NOT NULL AND BTRIM(php.name) <> ''
      AND u.email IS NOT NULL AND BTRIM(u.email) <> ''
      AND NOT COALESCE(u.is_frozen, false)
),

-- Moonshot (moonshot.hackclub.com, ended Dec 2025) is the same Prisma schema as
-- Shipwrecked: HackatimeProjectLink attaches Hackatime aliases to projects (one
-- row per alias), and Project has no timestamp columns. Hackatime is the only
-- time source — the link's rawHours/hoursOverride are banked/reviewer-adjusted
-- Hackatime totals (counting them would double count, SoM rationale), and
-- User.purchasedProgressHours is shop-bought progress, not time. Identity is the
-- user's normalized email (present for all 2,338 users).
--
-- claim_start_ts is the program launch (2025-10-25, first link row), not each
-- link's createdAt: links were created 2025-10-25..2026-03 as people joined, and
-- gating on the link timestamp would drop legitimate in-window coding done before
-- a participant linked. The CLOSED program window does the real gating.
--
-- Quality controls (audited 2026-06-14):
--   * User.status = 'FraudSuspect' is the program's fraud flag (UserStatus enum:
--     Unknown/L1/L2/FraudSuspect) — excluded, mirroring Shipwrecked. 0 users are
--     FraudSuspect today, but the gate is kept as a future-proof exclusion.
--   * identity-join coverage: 336 of 351 alias-claiming users (95.7%) have
--     in-window Hackatime by normalized email (Shipwrecked 95.4%); 322 (91.7%)
--     match on email+alias.
--   * scale: 6,207 raw in-window hours / 322 users / 4,659 user-days pre-dedup.
--     The mirror is a one-time manual backfill (DB normally stopped); see
--     sources.yml for the refresh procedure.
moonshot_ht_claims AS (
    SELECT 'moonshot'::text AS program_name,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1)
        END AS user_email,
        LOWER(BTRIM(l."hackatimeName")) AS hackatime_alias,
        proj.name AS project_name,
        NULLIF(BTRIM(proj."codeUrl"), '') AS code_url,
        TIMESTAMP WITH TIME ZONE '2025-10-25 00:00:00+00' AS claim_start_ts
    FROM {{ source('moonshot', 'HackatimeProjectLink') }} l
    JOIN {{ source('moonshot', 'Project') }} proj ON proj."projectID" = l."projectID"
    JOIN {{ source('moonshot', 'User') }} u ON u.id = proj."userId"
    WHERE l."hackatimeName" IS NOT NULL AND BTRIM(l."hackatimeName") <> ''
      AND u.email IS NOT NULL AND BTRIM(u.email) <> ''
      AND u.status <> 'FraudSuspect'
),

-- High Seas (winter 2024, ended ~2025-01-31) is Airtable-backed: the high_seas
-- mirror IS the program db. ships.wakatime_project_name is the Hackatime alias
-- the participant tied to each ship (12,358 of 12,754 ships carry one; 8,075
-- distinct aliases), and ships link to people via entrant_record_id. Hackatime
-- is the only daily time source — ships.hours/credited_hours/total_hours and
-- people.waka_*_hours_logged are banked snapshot/rollup totals, not daily
-- activity (SoM double-count rationale). Identity is the participant's
-- normalized email (people.email).
--
-- claim_start_ts is the program launch (2024-10-01), NOT ship.created_time: a
-- ship is created when the user SHIPS (after the coding), so gating on it would
-- drop the in-window coding that earned the ship. The CLOSED program window
-- (see program_windows) does the real gating.
--
-- Quality controls (audited 2026-06-14):
--   * entrant_disciplinary_status IN ('banned','frozen','suspected') or
--     entrant_freeze_activity = true is High Seas's fraud/ban gate (a per-person
--     status denormalized onto ships) — 131 of 1,839 alias-claiming participants
--     are fully banned and held 6.8% of in-window attributed hours; excluded.
--   * in-window Hackatime for the kept claims: ~900 users / ~40.3k raw hours /
--     peak DAU ~285, with no user-day over 24h.
high_seas_people AS (
    SELECT record_id,
        CASE WHEN POSITION('@' IN LOWER(BTRIM(email))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(email)), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(email)), '@', 2)
             ELSE SPLIT_PART(LOWER(BTRIM(email)), '+', 1)
        END AS user_email
    FROM {{ source('airtable_high_seas', 'people') }}
    WHERE email IS NOT NULL AND BTRIM(email) <> ''
),

high_seas_ht_claims AS (
    SELECT 'high_seas'::text AS program_name,
        p.user_email,
        LOWER(BTRIM(s.wakatime_project_name)) AS hackatime_alias,
        NULLIF(BTRIM(s.title), '') AS project_name,
        NULLIF(BTRIM(s.repo_url), '') AS code_url,
        TIMESTAMP WITH TIME ZONE '2024-10-01 00:00:00+00' AS claim_start_ts
    FROM {{ source('airtable_high_seas', 'ships') }} s
    JOIN high_seas_people p
        ON p.record_id = (CASE WHEN jsonb_typeof(s.entrant_record_id) = 'array'
                               THEN s.entrant_record_id->>0
                               ELSE s.entrant_record_id#>>'{}' END)
    WHERE s.wakatime_project_name IS NOT NULL AND BTRIM(s.wakatime_project_name) <> ''
      AND NOT COALESCE(
          (CASE WHEN jsonb_typeof(s.entrant_disciplinary_status) = 'array'
                THEN s.entrant_disciplinary_status->>0
                ELSE s.entrant_disciplinary_status#>>'{}' END) IN ('banned', 'frozen', 'suspected')
          OR (CASE WHEN jsonb_typeof(s.entrant_freeze_activity) = 'array'
                   THEN s.entrant_freeze_activity->>0
                   ELSE s.entrant_freeze_activity#>>'{}' END) = 'true',
          false)
),

-- ============================================================
-- 4. MERGE CLAIMS & FILTER BAD ALIASES
-- ============================================================
all_claims_raw AS (
    SELECT * FROM stardance_ht_claims
    UNION ALL SELECT * FROM flavortown_ht_claims
    UNION ALL SELECT * FROM beest_ht_claims
    UNION ALL SELECT * FROM stasis_ht_claims
    UNION ALL SELECT * FROM horizons_ht_claims
    UNION ALL SELECT * FROM sleepover_ht_claims
    UNION ALL SELECT * FROM midnight_ht_claims
    UNION ALL SELECT * FROM summer_of_making_ht_claims
    UNION ALL SELECT * FROM hack_club_the_game_ht_claims
    UNION ALL SELECT * FROM shipwrecked_ht_claims
    UNION ALL SELECT * FROM siege_ht_claims
    UNION ALL SELECT * FROM athena_award_ht_claims
    UNION ALL SELECT * FROM milkyway_ht_claims
    UNION ALL SELECT * FROM neighborhood_ht_claims
    UNION ALL SELECT * FROM carnival_ht_claims
    UNION ALL SELECT * FROM moonshot_ht_claims
    UNION ALL SELECT * FROM high_seas_ht_claims
),

all_claims AS (
    SELECT
        c.program_name,
        c.user_email,
        c.hackatime_alias,
        COALESCE(
            MIN(c.project_name) FILTER (WHERE NULLIF(BTRIM(c.code_url), '') IS NOT NULL),
            MIN(c.project_name)
        ) AS project_name,
        MIN(c.code_url) FILTER (
            WHERE NULLIF(BTRIM(c.code_url), '') IS NOT NULL
        ) AS code_url,
        MIN(c.claim_start_ts) AS claim_start_ts
    FROM all_claims_raw c
    LEFT JOIN bad_aliases b ON b.alias = c.hackatime_alias
    WHERE c.hackatime_alias IS NOT NULL AND c.hackatime_alias != ''
      AND b.alias IS NULL
    GROUP BY 1, 2, 3
),

-- ============================================================
-- 6. MATCH HACKATIME HOURS -> CLAIMS (only after claim_start_ts,
--    and only for hours within the claiming program's run window)
-- ============================================================
hackatime_matches AS (
    SELECT
        hd.activity_hour,
        (hd.activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
        ac.program_name,
        hd.user_email,
        ac.project_name,
        ac.code_url,
        hd.hackatime_alias,
        hd.raw_hours_logged,
        ac.claim_start_ts AS claim_started_at
    FROM hackatime_hourly hd
    JOIN all_claims ac
        ON ac.user_email = hd.user_email
        AND ac.hackatime_alias = hd.hackatime_alias
        AND hd.activity_hour >= DATE_TRUNC('hour', ac.claim_start_ts)
    JOIN program_windows w
        ON w.program_name = ac.program_name
        AND hd.activity_hour >= w.start_at
        AND (w.end_at_exclusive IS NULL OR hd.activity_hour < w.end_at_exclusive)
),

-- Custom hours, bounded to each program's run window.
--
-- 24h/user-day cap on self-reported time (audited 2026-06-12): none of the
-- source apps except blueprint enforce a server-side ceiling on duration
-- fields, so each (program, user, UTC day) is proportionally rescaled to a
-- 24h total here, extending the blueprint precedent to every custom path
-- (blueprint additionally keeps its own per-entry cap upstream; for the
-- program-day totals the two are equivalent — only intra-day project
-- attribution differs). Measured share of custom hours above 24h/user-day
-- at adoption: flavortown 4.7% (4,262 of 90,548h; worst user-day 590h),
-- stack 7.0% (25 of 351h), stasis 2.1% (477 of 22,549h), stardance 1.2%
-- (154 of 12,357h), offtrack and blueprint 0%; milkyway joined later with
-- its own upstream 24h guards and is covered here too. Real multi-day banked
-- entries and fabricated claims are indistinguishable here, so both are
-- truncated to 24h on the posting day (same trade-off blueprint documents).
-- DAU is unaffected by the cap. The cap applies per logging path: a user-day
-- can still exceed 24h when a program counts hackatime AND custom time
-- (different sources, deliberately not netted against each other).
custom_in_window AS (
    SELECT
        activity_hour,
        activity_date,
        program_name, user_email, project_name, code_url,
        CASE WHEN day_total_hours > 24
             THEN ROUND((raw_hours_logged * 24 / day_total_hours)::numeric, 4)
             ELSE raw_hours_logged
        END AS raw_hours_logged,
        logging_method, source_detail
    FROM (
        SELECT
            c.activity_hour,
            (c.activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
            c.program_name, c.user_email, c.project_name, c.code_url,
            c.raw_hours_logged, c.logging_method, c.source_detail,
            SUM(c.raw_hours_logged) OVER (
                PARTITION BY c.program_name, c.user_email,
                             (c.activity_hour AT TIME ZONE 'UTC')::date
            ) AS day_total_hours
        FROM (
            SELECT * FROM stardance_custom_hourly
            UNION ALL SELECT * FROM flavortown_custom_hourly
            UNION ALL SELECT * FROM stack_custom_hourly
            UNION ALL SELECT * FROM offtrack_custom_hourly
            UNION ALL SELECT * FROM stasis_custom_hourly
            UNION ALL SELECT * FROM blueprint_custom_hourly
            UNION ALL SELECT * FROM milkyway_custom_hourly
            UNION ALL SELECT * FROM construct_custom_hourly
            UNION ALL SELECT * FROM shiba_custom_hourly
            UNION ALL SELECT * FROM arcade_custom_hourly
            UNION ALL SELECT * FROM juice_custom_hourly
        ) c
        JOIN program_windows w
            ON w.program_name = c.program_name
            AND c.activity_hour >= w.start_at
            AND (w.end_at_exclusive IS NULL OR c.activity_hour < w.end_at_exclusive)
    ) windowed
),

-- ============================================================
-- 5. URL-BASED CROSS-PROGRAM OVERLAP (same-day, in-window only)
--    Derived from actual in-window activity (not timeless claims), so a repo
--    shared by a finished and a current program never splits across dates.
-- ============================================================
all_project_urls AS (
    SELECT DISTINCT user_email,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(code_url, '\.git$', ''), '/+$', '')) AS norm_url,
        activity_date, program_name
    FROM hackatime_matches WHERE code_url IS NOT NULL AND code_url != ''
    UNION
    SELECT DISTINCT user_email,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(code_url, '\.git$', ''), '/+$', '')) AS norm_url,
        activity_date, program_name
    FROM custom_in_window WHERE code_url IS NOT NULL AND code_url != ''
),

url_overlap AS (
    SELECT user_email, norm_url, activity_date,
        COUNT(DISTINCT program_name) AS num_programs,
        STRING_AGG(DISTINCT program_name, ', ' ORDER BY program_name) AS programs
    FROM all_project_urls
    WHERE norm_url IS NOT NULL AND norm_url != ''
    GROUP BY 1, 2, 3
    HAVING COUNT(DISTINCT program_name) > 1
),

ht_claim_counts AS (
    SELECT activity_hour, user_email, hackatime_alias,
        COUNT(DISTINCT program_name) AS ht_claim_count,
        STRING_AGG(DISTINCT program_name, ', ' ORDER BY program_name) AS ht_claimed_programs
    FROM hackatime_matches
    GROUP BY 1, 2, 3
),

hackatime_split_hourly AS (
    SELECT
        hm.activity_hour,
        hm.program_name,
        hm.user_email,
        hm.project_name,
        hm.code_url,
        'hackatime'::text AS logging_method,
        hm.raw_hours_logged,
        ROUND((hm.raw_hours_logged / GREATEST(hcc.ht_claim_count, COALESCE(uo.num_programs, 1)))::numeric, 4) AS credited_hours_logged,
        GREATEST(hcc.ht_claim_count, COALESCE(uo.num_programs, 1))::smallint AS split_factor,
        CASE
            WHEN hcc.ht_claim_count > 1 AND COALESCE(uo.num_programs, 1) > 1 THEN 'both'
            WHEN hcc.ht_claim_count > 1 THEN 'hackatime_alias'
            WHEN COALESCE(uo.num_programs, 1) > 1 THEN 'code_url'
            ELSE 'none'
        END AS overlap_type,
        CASE
            WHEN GREATEST(hcc.ht_claim_count, COALESCE(uo.num_programs, 1)) > 1
            THEN (
                SELECT ARRAY_AGG(DISTINCT p ORDER BY p)
                FROM unnest(
                    string_to_array(COALESCE(hcc.ht_claimed_programs, ''), ', ')
                    || string_to_array(COALESCE(uo.programs, ''), ', ')
                ) AS p
                WHERE p != ''
            )
        END AS overlapping_programs,
        hm.hackatime_alias,
        NULL::text AS source_detail,
        hm.claim_started_at
    FROM hackatime_matches hm
    JOIN ht_claim_counts hcc
        ON hcc.activity_hour = hm.activity_hour
        AND hcc.user_email = hm.user_email
        AND hcc.hackatime_alias = hm.hackatime_alias
    LEFT JOIN url_overlap uo
        ON hm.code_url IS NOT NULL AND hm.code_url != ''
        AND hm.user_email = uo.user_email
        AND hm.activity_date = uo.activity_date
        AND LOWER(REGEXP_REPLACE(REGEXP_REPLACE(hm.code_url, '\.git$', ''), '/+$', '')) = uo.norm_url
),

-- Apply URL-based splitting to custom sources too
custom_with_url_split AS (
    SELECT
        c.activity_hour,
        c.program_name,
        c.user_email,
        c.project_name,
        c.code_url,
        c.logging_method,
        c.raw_hours_logged,
        ROUND((c.raw_hours_logged / COALESCE(uo.num_programs, 1))::numeric, 4) AS credited_hours_logged,
        COALESCE(uo.num_programs, 1)::smallint AS split_factor,
        CASE WHEN COALESCE(uo.num_programs, 1) > 1 THEN 'code_url' ELSE 'none' END AS overlap_type,
        CASE WHEN COALESCE(uo.num_programs, 1) > 1
             THEN string_to_array(uo.programs, ', ')
        END AS overlapping_programs,
        NULL::text AS hackatime_alias,
        c.source_detail,
        NULL::timestamptz AS claim_started_at
    FROM custom_in_window c
    LEFT JOIN url_overlap uo
        ON c.code_url IS NOT NULL AND c.code_url != ''
        AND c.user_email = uo.user_email
        AND c.activity_date = uo.activity_date
        AND LOWER(REGEXP_REPLACE(REGEXP_REPLACE(c.code_url, '\.git$', ''), '/+$', '')) = uo.norm_url
),

-- ============================================================
-- 6b. DAILY-GRAIN & ACTIVITY-ONLY SOURCES
--     Programs whose native activity source is not hourly Hackatime/custom
--     time. These bypass the hourly alias/URL dedup (they expose no shared
--     alias or repo URL) and the 24h/user-day cap (their upstreams already
--     cap or aggregate), so each row is added pre-split: split_factor=1,
--     overlap_type='none'. Each carries a distinct logging_method token so
--     daily_active_users can recover its DAU methodology label.
--
--     Identity is the normalized participant email wherever the source can
--     bridge to one, so these users dedup against the coding programs in
--     daily_active_users_deduped. macondo/horizons fall back to a stable
--     program:user_id key when no email exists (cannot cross-program dedup,
--     but DAU/hours are preserved); fallout/highway keep NULL for unmatched
--     users (dropped from DAU by COUNT(DISTINCT), matching their prior
--     program-native models).
-- ============================================================

-- Hour spine for daily-grain sources. They only know the calendar day, so rather
-- than spike a person's whole day at 00:00 UTC we spread the attribution evenly
-- across all 24 hours (1/24 of the hours per hourly row). The downstream DAU
-- weights still reconcile — a person-day's rows sum to their hours/day-total no
-- matter how many rows it is split into. raw/credited hours are rounded to 6dp so
-- 24 × (hours/24) recovers the day total to well within the test tolerances.
hours_of_day AS (
    SELECT generate_series(0, 23) AS hr
),

-- Macondo's own per-day rollup (day-grain). DAU is email-keyed (was user_id-keyed
-- in the old program-native model) so a person counts once across programs; hours
-- are unchanged (hackatime+journal seconds), just spread across the 24 hours.
macondo_daily AS (
    SELECT
        dpa.day::date AS activity_day,
        COALESCE(
            CASE
                WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
                    THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                         || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
                ELSE NULLIF(SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1), '')
            END,
            'macondo:' || dpa.user_id
        ) AS user_email,
        SUM(COALESCE(dpa.hackatime_seconds, 0) + COALESCE(dpa.journal_seconds, 0))::numeric / 3600.0 AS day_hours
    FROM {{ source('macondo', 'daily_project_activity') }} dpa
    LEFT JOIN {{ source('macondo', 'users') }} u ON u.id = dpa.user_id
    WHERE dpa.day::text ~ '^\d{4}-\d{2}-\d{2}$'
      AND dpa.day::date >= DATE '2026-03-23'
      AND (dpa.hackatime_seconds > 0 OR dpa.journal_seconds > 0)
    GROUP BY 1, 2
),

macondo_activity AS (
    SELECT
        (md.activity_day::timestamp + (h.hr * INTERVAL '1 hour')) AS activity_hour,
        'macondo'::text AS program_name,
        md.user_email,
        NULL::text AS project_name,
        NULL::text AS code_url,
        'daily_project_activity'::text AS logging_method,
        ROUND((md.day_hours / 24.0)::numeric, 6) AS raw_hours_logged,
        ROUND((md.day_hours / 24.0)::numeric, 6) AS credited_hours_logged,
        1::smallint AS split_factor,
        'none'::text AS overlap_type,
        NULL::text[] AS overlapping_programs,
        NULL::text AS hackatime_alias,
        'macondo.daily_project_activity; hackatime+journal seconds spread across 24h'::text AS source_detail,
        NULL::timestamptz AS claim_started_at
    FROM macondo_daily md
    CROSS JOIN hours_of_day h
),

-- Horizons app-native daily activity, from the 2026-04-22 handoff onward
-- (earlier history flows through horizons_ht_claims / the run window above).
-- Day-grain; DAU is now email-keyed and the day's seconds are spread evenly
-- across 24 hourly rows (see hours_of_day). Folding this in also restores
-- Horizons app-native users to the deduped DAU total, which the old daily-grain
-- models omitted.
horizons_daily AS (
    SELECT
        uda.local_date::date AS activity_day,
        COALESCE(
            CASE
                WHEN POSITION('@' IN LOWER(BTRIM(u.email))) > 0
                    THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(u.email)), '@', 1), '+', 1)
                         || '@' || SPLIT_PART(LOWER(BTRIM(u.email)), '@', 2)
                ELSE NULLIF(SPLIT_PART(LOWER(BTRIM(u.email)), '+', 1), '')
            END,
            'horizons:' || uda.user_id::text
        ) AS user_email,
        SUM(uda.seconds)::numeric / 3600.0 AS day_hours
    FROM {{ source('horizons', 'user_daily_activity') }} uda
    LEFT JOIN {{ source('horizons', 'users') }} u ON u.user_id = uda.user_id
    WHERE uda.local_date::date >= DATE '2026-04-22'
      AND uda.qualified
      AND uda.seconds > 0
    GROUP BY 1, 2
),

horizons_post_activity AS (
    SELECT
        (hd.activity_day::timestamp + (h.hr * INTERVAL '1 hour')) AS activity_hour,
        'horizons'::text AS program_name,
        hd.user_email,
        NULL::text AS project_name,
        NULL::text AS code_url,
        'daily_user_activity'::text AS logging_method,
        ROUND((hd.day_hours / 24.0)::numeric, 6) AS raw_hours_logged,
        ROUND((hd.day_hours / 24.0)::numeric, 6) AS credited_hours_logged,
        1::smallint AS split_factor,
        'none'::text AS overlap_type,
        NULL::text[] AS overlapping_programs,
        NULL::text AS hackatime_alias,
        'horizons.user_daily_activity; qualified seconds spread across 24h'::text AS source_detail,
        NULL::timestamptz AS claim_started_at
    FROM horizons_daily hd
    CROSS JOIN hours_of_day h
),

-- Fallout (hardware): real created_at timestamps, so this is genuinely hourly.
-- DAU = a build timelapse (lookout/lapse, duration>0) OR a non-discarded
-- journal entry (regardless of duration). Hours = timelapse durations +
-- journal burnout_duration_seconds. Journal entries with no burnout duration
-- yield 0-hour rows that exist purely to mark the user active that day (kept
-- by the activity-only branch of the final filter). users join is LEFT so
-- unmatched authors keep their hours (NULL email, dropped from DAU) — matching
-- the prior model, which summed hours without a user join.
fallout_users_norm AS (
    SELECT
        id,
        CASE
            WHEN POSITION('@' IN LOWER(BTRIM(email))) > 0
                THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(email)), '@', 1), '+', 1)
                     || '@' || SPLIT_PART(LOWER(BTRIM(email)), '@', 2)
            ELSE NULLIF(SPLIT_PART(LOWER(BTRIM(email)), '+', 1), '')
        END AS user_email
    FROM {{ source('fallout', 'users') }}
),

fallout_events AS (
    SELECT DATE_TRUNC('hour', l.created_at AT TIME ZONE 'UTC') AS activity_hour,
           u.user_email,
           l.duration::numeric / 3600.0 AS hours
    FROM {{ source('fallout', 'lookout_timelapses') }} l
    LEFT JOIN fallout_users_norm u ON u.id = l.user_id
    WHERE l.duration > 0 AND (l.created_at AT TIME ZONE 'UTC') <= NOW()
    UNION ALL
    SELECT DATE_TRUNC('hour', l.created_at AT TIME ZONE 'UTC'),
           u.user_email,
           l.duration::numeric / 3600.0
    FROM {{ source('fallout', 'lapse_timelapses') }} l
    LEFT JOIN fallout_users_norm u ON u.id = l.user_id
    WHERE l.duration > 0 AND (l.created_at AT TIME ZONE 'UTC') <= NOW()
    UNION ALL
    SELECT DATE_TRUNC('hour', j.created_at AT TIME ZONE 'UTC'),
           u.user_email,
           COALESCE(j.burnout_duration_seconds, 0)::numeric / 3600.0
    FROM {{ source('fallout', 'journal_entries') }} j
    LEFT JOIN fallout_users_norm u ON u.id = j.user_id
    WHERE j.discarded_at IS NULL AND (j.created_at AT TIME ZONE 'UTC') <= NOW()
),

fallout_activity_log AS (
    SELECT
        activity_hour,
        'fallout'::text AS program_name,
        user_email,
        NULL::text AS project_name,
        NULL::text AS code_url,
        'hardware_build'::text AS logging_method,
        ROUND(SUM(hours)::numeric, 4) AS raw_hours_logged,
        ROUND(SUM(hours)::numeric, 4) AS credited_hours_logged,
        1::smallint AS split_factor,
        'none'::text AS overlap_type,
        NULL::text[] AS overlapping_programs,
        NULL::text AS hackatime_alias,
        'fallout.build_timelapses+journals'::text AS source_detail,
        NULL::timestamptz AS claim_started_at
    FROM fallout_events
    WHERE (activity_hour AT TIME ZONE 'UTC')::date >= DATE '2026-03-01'
    GROUP BY 1, 3
),

-- Highway (hardware, Summer 2025): GitHub commit days on submitted repos.
-- No durations exist, so every row carries 0 hours and serves DAU only (kept
-- by the activity-only branch of the final filter; excluded from the hours
-- mart by its HAVING SUM > 0). Commits bucket to their UTC commit-day at
-- 00:00. Quality controls and window match the prior program-native model.
highway_norm AS (
    SELECT
        CASE WHEN POSITION('@' IN LOWER(BTRIM(r.fields->>'Email'))) > 0
             THEN SPLIT_PART(SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '@', 1), '+', 1)
                  || '@' || SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '@', 2)
             ELSE NULLIF(SPLIT_PART(LOWER(BTRIM(r.fields->>'Email')), '+', 1), '')
        END AS user_email,
        LOWER((REGEXP_MATCH(r.fields->>'Github_Url',
                            'github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)'))[1])
        || '/' ||
        REGEXP_REPLACE(
            LOWER((REGEXP_MATCH(r.fields->>'Github_Url',
                                'github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)'))[2]),
            '\.git$', '') AS repo_key
    FROM {{ source('airtable_raw_all_bases', 'records') }} r
    WHERE r.base_id = 'appuDQSHCdCHyOrxw'
      AND r.table_id = 'tbl9QnZ320NTGJHJj'          -- Highway Projects table
      AND COALESCE(r.fields->>'Status', '') <> 'Purged'
      AND r.fields->>'Github_Url' ~* 'github\.com/'
),

highway_activity AS (
    SELECT
        ((c.authored_at AT TIME ZONE 'UTC')::date)::timestamp AS activity_hour,
        'highway'::text AS program_name,
        h.user_email,
        NULL::text AS project_name,
        NULL::text AS code_url,
        'github_commit_days'::text AS logging_method,
        0::numeric AS raw_hours_logged,
        0::numeric AS credited_hours_logged,
        1::smallint AS split_factor,
        'none'::text AS overlap_type,
        NULL::text[] AS overlapping_programs,
        NULL::text AS hackatime_alias,
        'highway.github_commit_days (no durations; DAU only)'::text AS source_detail,
        NULL::timestamptz AS claim_started_at
    FROM highway_norm h
    JOIN {{ source('highway_github', 'repos') }} gr
        ON gr.repo_key = h.repo_key
        AND gr.scrape_status = 'ok'
    JOIN {{ source('highway_github', 'commits') }} c ON c.repo_key = h.repo_key
    WHERE h.user_email IS NOT NULL
      AND (c.authored_at AT TIME ZONE 'UTC') <= NOW()
      AND (c.authored_at AT TIME ZONE 'UTC')::date >= DATE '2025-05-01'
      AND (c.authored_at AT TIME ZONE 'UTC')::date < DATE '2025-11-01'
    GROUP BY 1, 3
),

-- ============================================================
-- 7. FINAL UNION (already bounded to each program's run window upstream)
-- ============================================================
combined AS (
    SELECT * FROM hackatime_split_hourly
    UNION ALL
    SELECT * FROM custom_with_url_split
    UNION ALL
    SELECT * FROM macondo_activity
    UNION ALL
    SELECT * FROM horizons_post_activity
    UNION ALL
    SELECT * FROM fallout_activity_log
    UNION ALL
    SELECT * FROM highway_activity
),

-- The rows that actually land in the table: every time-bearing row plus the
-- activity-only rows (highway commit days, fallout duration-less journals) that
-- mark a user active that day with zero credited hours. The DAU weights below
-- are computed over THIS set so they sum exactly.
final_rows AS (
    SELECT *,
        (activity_hour AT TIME ZONE 'UTC')::date AS activity_date
    FROM combined
    WHERE credited_hours_logged > 0
       OR logging_method IN ('github_commit_days', 'hardware_build')
),

-- ============================================================
-- 8. ROW-LEVEL DAU WEIGHTS (so a stacked chart sums to a headcount)
--
-- A person has many rows per day (multiple hours/projects/programs), so a flat
-- 1-per-row would overcount. These two columns each spread a person's "1" across
-- their rows so SUM() aggregates back to a headcount:
--
--   dau          — the row's share of the person's presence in THIS program that
--                  day: credited_hours / SUM(credited_hours) over
--                  (program, user, day). A person's rows in a program-day sum to
--                  1, so SUM(dau) over (program, day) = that program's distinct
--                  users (== daily_active_users.dau). NOT split across programs.
--   dau_deduped  — the row's share of the person's WHOLE day across all programs:
--                  credited_hours / SUM(credited_hours) over (user, day). A
--                  person's rows across all programs sum to 1, so SUM(dau_deduped)
--                  over (program, day) = the hours-weighted cross-program split,
--                  and over (day) = the unique overall DAU
--                  (== daily_active_users_deduped.dau). STACK THIS for "unique
--                  overall DAU by program".
--
-- Both fall back to an equal split across the person's rows when the relevant
-- denominator is zero (a program-day, or a whole day, that is entirely
-- activity-only / hour-less — e.g. highway commit days). NULL-email rows can't
-- form a person, so both weights are NULL there (and excluded from DAU, matching
-- daily_active_users*). Stored unrounded so the stacks reconcile exactly.
-- ============================================================
prog_user_day AS (
    SELECT program_name, user_email, activity_date,
        SUM(credited_hours_logged) AS prog_hours,
        COUNT(*) AS prog_rows
    FROM final_rows
    WHERE user_email IS NOT NULL
    GROUP BY 1, 2, 3
),

user_day AS (
    SELECT user_email, activity_date,
        SUM(credited_hours_logged) AS day_hours,
        COUNT(DISTINCT program_name) AS day_program_count
    FROM final_rows
    WHERE user_email IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    c.activity_hour,
    c.program_name,
    c.user_email,
    c.project_name,
    c.code_url,
    c.logging_method,
    c.raw_hours_logged,
    c.credited_hours_logged,
    c.split_factor,
    c.overlap_type,
    c.overlapping_programs,
    c.hackatime_alias,
    c.source_detail,
    c.claim_started_at,
    CASE
        WHEN c.user_email IS NULL THEN NULL
        WHEN pud.prog_hours > 0 THEN c.credited_hours_logged / pud.prog_hours
        ELSE 1.0 / pud.prog_rows
    END AS dau,
    CASE
        WHEN c.user_email IS NULL THEN NULL
        WHEN ud.day_hours > 0 THEN c.credited_hours_logged / ud.day_hours
        ELSE (1.0 / ud.day_program_count) / pud.prog_rows
    END AS dau_deduped
FROM final_rows c
LEFT JOIN prog_user_day pud
    ON pud.program_name = c.program_name
   AND pud.user_email = c.user_email
   AND pud.activity_date = c.activity_date
LEFT JOIN user_day ud
    ON ud.user_email = c.user_email
   AND ud.activity_date = c.activity_date
ORDER BY c.activity_hour DESC, c.program_name, c.user_email, c.project_name
