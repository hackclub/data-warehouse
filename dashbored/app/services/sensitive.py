import re

SENSITIVE_PATTERNS = [
    "token", "secret", "password", "ciphertext", "bidx", "encrypted",
    "private_key", "api_key", "secret_key", "otp", "digest", "salt",
    "reset_password", "sign_in_ip",
]

_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def is_sensitive(name: str) -> bool:
    """Match case- and separator-insensitively so snake_case and camelCase
    schemas flag the same columns: sign_in_ip, signInIp and SignInIP all hit.
    Words stay separated by an underscore so short patterns cannot straddle a
    boundary the way "otp" does in not_processed."""
    words = re.findall(r"[a-z0-9]+", _CAMEL_BOUNDARY.sub("_", name).lower())
    return any(p in "_".join(words) for p in SENSITIVE_PATTERNS)
