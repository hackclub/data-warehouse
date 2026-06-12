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
--   blueprint  — hardware/PCB program (ongoing, launched ~2025-09-23).
--   hack_club_the_game — gamified ship-anything program (ongoing, beta opened
--                ~2026-01-16, first public approvals 2026-02-17).
--   stack, offtrack, beest, stasis, horizons — public Summer 2026 programs
--                with coding/work-session activity in their own app mirrors.
--
-- Sources per program:
--   Hackatime coding — global hackatime heartbeats, attributed to a program by
--                      the user's claimed project alias (user_hackatime_projects).
--   Custom logging   — post_devlogs.duration_seconds.
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
        ('horizons',   TIMESTAMP WITH TIME ZONE '2026-02-22 00:00:00+00',
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
                       TIMESTAMP WITH TIME ZONE '2026-05-02 00:00:00+00')
        -- This model is the CREDITED-HOURS log (Hackatime + devlog/journal) for
        -- coding programs. Separate, daily-grained programs live in
        -- daily_active_users: fallout (ship-based) and macondo (its own
        -- daily_project_activity rollup).
        -- Horizons' app mirror is currently stale, so this only includes
        -- Hackatime claims already present in the mirror.
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
-- (hack_club_the_game_ht_claims reuses this map for the same reason.)
beest_htid_email AS (
    SELECT DISTINCT hackatime_user_id, hackatime_first_email
    FROM {{ ref('hourly_project_activity') }}
    WHERE hackatime_first_email IS NOT NULL
),

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

-- ============================================================
-- 4. MERGE CLAIMS & FILTER BAD ALIASES
-- ============================================================
all_claims_raw AS (
    SELECT * FROM stardance_ht_claims
    UNION ALL SELECT * FROM flavortown_ht_claims
    UNION ALL SELECT * FROM beest_ht_claims
    UNION ALL SELECT * FROM stasis_ht_claims
    UNION ALL SELECT * FROM horizons_ht_claims
    UNION ALL SELECT * FROM summer_of_making_ht_claims
    UNION ALL SELECT * FROM hack_club_the_game_ht_claims
    UNION ALL SELECT * FROM shipwrecked_ht_claims
    UNION ALL SELECT * FROM siege_ht_claims
    UNION ALL SELECT * FROM athena_award_ht_claims
    UNION ALL SELECT * FROM milkyway_ht_claims
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

-- Custom hours, bounded to each program's run window
custom_in_window AS (
    SELECT
        c.activity_hour,
        (c.activity_hour AT TIME ZONE 'UTC')::date AS activity_date,
        c.program_name, c.user_email, c.project_name, c.code_url,
        c.raw_hours_logged, c.logging_method, c.source_detail
    FROM (
        SELECT * FROM stardance_custom_hourly
        UNION ALL SELECT * FROM flavortown_custom_hourly
        UNION ALL SELECT * FROM stack_custom_hourly
        UNION ALL SELECT * FROM offtrack_custom_hourly
        UNION ALL SELECT * FROM stasis_custom_hourly
        UNION ALL SELECT * FROM blueprint_custom_hourly
        UNION ALL SELECT * FROM milkyway_custom_hourly
    ) c
    JOIN program_windows w
        ON w.program_name = c.program_name
        AND c.activity_hour >= w.start_at
        AND (w.end_at_exclusive IS NULL OR c.activity_hour < w.end_at_exclusive)
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
-- 7. FINAL UNION (already bounded to each program's run window upstream)
-- ============================================================
combined AS (
    SELECT * FROM hackatime_split_hourly
    UNION ALL
    SELECT * FROM custom_with_url_split
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
    c.claim_started_at
FROM combined c
WHERE c.credited_hours_logged > 0
ORDER BY c.activity_hour DESC, c.program_name, c.user_email, c.project_name
