"""Keep purchase purpose while removing payment identities from every export.

Only public-style descriptions and merchant/organization labels are candidates
for publication. Recipient, sender, bank and contact fields never become output.
The report still requires access control; free text is not provably anonymous.
"""

import html
import re
import unicodedata

NAME_HIDDEN = "[name hidden]"
_WORD = re.compile(r"[^\W_]+", re.UNICODE)
_EMAIL = re.compile(r"[^\s<>\[\](),;:]+@[^\s<>\[\](),;:]+\.[^\s<>\[\](),;:]+")
_URL = re.compile(r"(?i)\b(?:https?://|www\.)\S+|mailto:\S+")
_PHONE = re.compile(r"(?<!\w)(?:\+?\d[\d ().-]{5,}\d)(?!\w)")
_ADDRESS = re.compile(
    r"(?i)\b\d{1,6}\s+(?:[\w.'-]+\s+){1,6}"
    r"(?:street|st\.?|road|rd\.?|avenue|ave\.?|lane|ln\.?|drive|dr\.?|"
    r"boulevard|blvd\.?|court|ct\.?|way|place|pl\.?)\b[^\n;]*"
)
_BANK_FIELD = re.compile(
    r"(?i)\b(?:iban|swift|bic|routing(?:\s+number)?|account(?:\s+(?:number|no))?|"
    r"address|phone|passport|ssn)\s*[:#=]\s*[^\n;]+"
)
# These generated HCB memos have a recipient but often no separate recipient
# field (especially returned grants). Preserve the action, never the recipient.
_GRANT_RECIPIENT = re.compile(r"(?i)(\bgrant\s+to\s+).*?(?=\s+for\s+|$)")
_PERSON_FIELDS = (
    "initiated_by_name", "private_recipient_name", "private_sender_name",
    "private_donor_name", "private_user_name",
)


def _words(value):
    return tuple(m.group().casefold() for m in _WORD.finditer(value))


class PaymentRedactor:
    """Token trie avoids a giant regex or scanning every name for every memo."""

    def __init__(self, names=()):
        self.trie = {}
        for value in names:
            if not value:
                continue
            value = unicodedata.normalize("NFKC", html.unescape(str(value)))
            tokens = _words(value)
            # Global one-word display names can also be ordinary purchase words.
            # Match them only on their own transaction (see text()).
            if len(tokens) < 2:
                continue
            self._insert(tokens)
            if len(tokens) > 2:
                self._insert((tokens[0], tokens[-1]))

    def _insert(self, tokens):
        node = self.trie
        for token in tokens:
            node = node.setdefault(token, {})
        node[None] = True

    @staticmethod
    def names_in(txn):
        names = [txn.get(k) for k in _PERSON_FIELDS]
        # A bank-rail counterparty may be a legal recipient name. Never treat a
        # card merchant or known HCB organization as a person's identity.
        if txn.get("transaction_type") not in ("card_transaction", "disbursement", "incoming_disbursement"):
            for field in ("counterparty", "source"):
                value = txn.get(field)
                if value and value != txn.get("public_counterparty") and value != txn.get("public_source"):
                    names.append(value)
        return [str(n) for n in names if n]

    def text(self, value, txn=None):
        if value is None:
            return None
        text = unicodedata.normalize("NFKC", html.unescape(str(value)))
        text = "".join(c for c in text if unicodedata.category(c) != "Cf")
        text = _EMAIL.sub("[email hidden]", text)
        text = _URL.sub("[link hidden]", text)
        text = _BANK_FIELD.sub("[private detail hidden]", text)
        text = _ADDRESS.sub("[address hidden]", text)
        # Do not mistake ISO dates for phone/account numbers.
        def hide_number(match):
            value = match.group()
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                return value
            return "[number hidden]" if sum(c.isdigit() for c in value) >= 7 else value
        text = _PHONE.sub(hide_number, text)
        text = _GRANT_RECIPIENT.sub(lambda m: m.group(1) + NAME_HIDDEN, text)
        local = set()
        for name in self.names_in(txn or {}):
            # Redact first/last/middle components, initials and reordered bank
            # names on this transaction, without damaging other merchants.
            local.update(_words(unicodedata.normalize("NFKC", name)))
        protected = list(re.finditer(r"\[[^\]]*hidden\]", text))
        matches = [m for m in _WORD.finditer(text)
                   if not any(p.start() <= m.start() < p.end() for p in protected)]
        spans = []
        i = 0
        while i < len(matches):
            node = self.trie
            end = i
            for j in range(i, len(matches)):
                token = matches[j].group().casefold()
                if token not in node:
                    break
                node = node[token]
                if None in node:
                    end = j + 1
            if end == i and matches[i].group().casefold() in local:
                end = i + 1
            if end > i:
                spans.append((matches[i].start(), matches[end - 1].end()))
                i = end
            else:
                i += 1
        for start, end in reversed(spans):
            text = text[:start] + NAME_HIDDEN + text[end:]
        return re.sub(r"(?:\[name hidden\][\s,.'-]*){2,}", NAME_HIDDEN + " ", text).strip()
