import os
import subprocess


class EncryptionError(Exception):
    """The connection URL could not be encrypted; the message is user-facing.

    This has to be loud. The return value goes straight into a PR body on a
    public repo, so a placeholder string here would ship a PR that looks
    complete and carries no credential, and nobody finds out until an operator
    tries to install it.
    """


def recipients() -> list[str]:
    """Every age public key allowed to decrypt, newest config first.

    age takes multiple recipients and any one of the matching private keys can
    decrypt, so a second keyholder is just another -r — no shared key, no
    escrow. Removing someone only affects PRs opened after the change; blobs
    already published stay readable by whoever was a recipient at the time.
    """
    raw = os.environ.get("AGE_PUBLIC_KEYS") or os.environ.get("AGE_PUBLIC_KEY") or ""
    return [key for key in raw.replace(",", " ").split() if key]


def encrypt(plaintext: str) -> str:
    keys = recipients()
    if not keys:
        raise EncryptionError(
            "No age recipients are configured, so the connection URL cannot be "
            "encrypted into the PR. Set AGE_PUBLIC_KEYS before onboarding."
        )

    args = ["age", "--encrypt", "--armor"]
    for key in keys:
        args += ["--recipient", key]

    try:
        result = subprocess.run(
            args, input=plaintext, capture_output=True, text=True, timeout=10
        )
    except FileNotFoundError:
        raise EncryptionError("The age binary is missing from this deployment.")
    except subprocess.TimeoutExpired:
        raise EncryptionError("Encrypting the connection URL timed out.")

    if result.returncode != 0:
        raise EncryptionError(f"Encrypting the connection URL failed: {result.stderr.strip()}")

    encrypted = result.stdout.strip()
    if not encrypted:
        raise EncryptionError("age produced no output for the connection URL.")
    return encrypted
