{{ config(
    schema='hcb_ysws_true_spend_analytics',
    materialized='table'
) }}

/*
    YSWS spend program registry — one row per canonical program ROOT HCB org.

    THE MAPPING CONTRACT (do not add heuristics to this file):

      A YSWS program is mapped to HCB by the `hcb` link on its Unified YSWS DB
      (Airtable) record. That linked org AND all of its HCB sub-orgs are the
      program's money. That is the ONLY way an HCB org is matched to a YSWS.

    Consequences, all intentional:
      - No slug branding, name similarity, or money-flow inference. An org that
        looks like it belongs to a program but is neither the linked org nor a
        descendant of it is UNMATCHED, and shows up in ysws_unmatched_orgs so
        the Airtable link or the HCB parent relationship can be fixed. We do
        not paper over the gap here.
      - If Airtable links a sub-org while its parent goes unlinked, the parent
            is unmatched. Fix = point the Airtable link at the parent.
      - Two linked orgs in the same HCB chain are two separate programs;
        ysws_spend_org_tree stops descending at another root, so each org still
        belongs to exactly one program.
      - Personal author/reviewer pots that are sub-orgs of a linked program are
        part of that program (per Zach, 2026-08-18: all sub-orgs means all).

    The one non-Airtable row is HQ marketing, which is NOT a YSWS program and
    never claims to be: bucket = 'marketing', is_ysws_program = false. It is
    tracked here because the true-spend ledger nets marketing-funded program
    budget out of the receiving program (see ysws_spend_ledger's M offsets) and
    the leadership dashboard reads that bucket.

    Grain: root_event_id. Several Airtable rows can point at the same HCB org
    (Jumpstart V1/V2/V3), so names and record ids are aggregated.
*/

WITH airtable_roots AS (
    SELECT
        p.name AS program_name,
        -- Airtable record id: the stable join key for approved projects and
        -- weighted hours. Names get renamed; record ids do not.
        p.id AS program_id,
        regexp_replace(p.hcb, '^https://hcb\.hackclub\.com/([^/?#]+).*$', '\1') AS root_slug,
        'program' AS bucket,
        TRUE AS is_ysws_program,
        'unified_ysws_hcb_link' AS match_source
    FROM {{ source('unified_ysws', 'ysws_programs') }} p
    WHERE p.hcb ~ '^https://hcb\.hackclub\.com/'
),

non_program_roots AS (
    SELECT
        'Marketing (HQ, not a YSWS program)' AS program_name,
        NULL::text AS program_id,
        'ysws-marketing' AS root_slug,
        'marketing' AS bucket,
        FALSE AS is_ysws_program,
        'manual_non_program' AS match_source
),

all_roots AS (
    SELECT * FROM airtable_roots
    UNION ALL
    SELECT * FROM non_program_roots
),

resolved AS (
    SELECT
        o.event_id AS root_event_id,
        o.slug AS root_slug,
        r.program_name,
        r.program_id,
        r.bucket,
        r.is_ysws_program,
        r.match_source
    FROM all_roots r
    JOIN {{ ref('orgs') }} o ON o.slug = r.root_slug
    WHERE NOT o.is_deleted
)

SELECT
    root_event_id,
    root_slug,
    string_agg(DISTINCT program_name, ' / ' ORDER BY program_name) AS program_name,
    -- Constituent Airtable program names; display only.
    array_agg(DISTINCT program_name) AS member_names,
    -- The same programs by Airtable record id: the deterministic join key for
    -- approved projects and weighted hours. Empty for the marketing row.
    COALESCE(
        array_agg(DISTINCT program_id) FILTER (WHERE program_id IS NOT NULL),
        ARRAY[]::text[]
    ) AS member_ids,
    MIN(bucket) AS bucket,
    bool_or(is_ysws_program) AS is_ysws_program,
    string_agg(DISTINCT match_source, '+') AS match_source
FROM resolved
GROUP BY root_event_id, root_slug
