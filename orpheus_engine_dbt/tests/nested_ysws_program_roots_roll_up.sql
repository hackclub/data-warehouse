-- An Airtable program root that is nested below another Airtable program root
-- AND slug-branded as its child (campfire-flagship under campfire) is a
-- satellite: it must live inside the umbrella's spend tree, not be emitted as a
-- second root. Metadata-driven, so future satellites are covered without
-- naming individual events.
--
-- Deliberately NOT asserted for nesting alone: unrelated programs are banked
-- under another program's org (Sleepover under Athena) or under a personal
-- reviewer pot (Ceiling under ysws-budget-tongyu). Those must stay their own
-- roots, and the branded-slug + pot-exclusion conditions below encode that.

WITH RECURSIVE airtable_roots AS (
    SELECT
        e.id AS event_id,
        e.slug,
        e.name AS org_name,
        e.parent_id
    FROM {{ source('unified_ysws', 'ysws_programs') }} p
    JOIN {{ source('hcb', 'events') }} e
        ON e.slug = regexp_replace(
            p.hcb,
            '^https://hcb\.hackclub\.com/([^/?#]+).*$',
            '\1'
        )
    WHERE p.hcb ~ '^https://hcb\.hackclub\.com/'
      AND e.deleted_at IS NULL
),

ancestors AS (
    SELECT event_id AS member_event_id, event_id AS ancestor_event_id, parent_id, 0 AS depth
    FROM airtable_roots

    UNION ALL

    SELECT a.member_event_id, e.id, e.parent_id, a.depth + 1
    FROM ancestors a
    JOIN {{ source('hcb', 'events') }} e ON e.id = a.parent_id
),

-- Only genuine umbrella relationships, matching ysws_spend_programs.
expected AS (
    SELECT DISTINCT ON (a.member_event_id)
        a.member_event_id,
        root.event_id AS canonical_root_event_id
    FROM ancestors a
    JOIN airtable_roots root ON root.event_id = a.ancestor_event_id
    JOIN airtable_roots member ON member.event_id = a.member_event_id
    WHERE a.depth > 0
      AND left(member.slug, length(root.slug) + 1) = root.slug || '-'
      AND root.slug NOT LIKE 'ysws-budget-%'
      AND root.slug NOT LIKE 'ysws-resolution-%'
      AND root.slug NOT LIKE '%-earnings'
      AND root.slug NOT LIKE '%-jemoney'
      AND COALESCE(root.org_name, '') NOT ILIKE '%budget%'
      AND COALESCE(root.org_name, '') NOT ILIKE '%earnings%'
    ORDER BY a.member_event_id, a.depth DESC
)

SELECT e.member_event_id, e.canonical_root_event_id
FROM expected e
-- The satellite must appear inside the umbrella's tree...
LEFT JOIN {{ ref('ysws_spend_org_tree') }} tree
    ON tree.root_event_id = e.canonical_root_event_id
   AND tree.event_id = e.member_event_id
WHERE tree.event_id IS NULL
   -- ...and must not still be its own root.
   OR EXISTS (
       SELECT 1
       FROM {{ ref('ysws_spend_programs') }} p
       WHERE p.root_event_id = e.member_event_id
   )
