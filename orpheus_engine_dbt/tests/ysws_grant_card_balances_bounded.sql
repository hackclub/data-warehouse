/*
    Money still on a program's grant cards cannot exceed what the program put on
    them, and cannot be negative.

    This guards the fix for card_grants_unspent_dollars, which used to expose the
    face value of HCB's "active" grants as if it were the leftover balance and
    overstated it by roughly 6x ($892k vs $142k). The replacement is measured
    from each grant's subledger, so a sign error or a bad join would show up
    here rather than as a plausible-looking number on a public page.
*/

SELECT
    root_slug,
    card_grants_funded_dollars,
    card_grants_remaining_dollars
FROM {{ ref('ysws_spend_by_program') }}
WHERE card_grants_remaining_dollars < 0
   OR card_grants_remaining_dollars > card_grants_funded_dollars + 0.01
