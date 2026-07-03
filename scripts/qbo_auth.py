"""One-time OAuth2 bootstrap for the QuickBooks Online sync.

QuickBooks Online only issues API tokens via OAuth2, and Intuit requires
production redirect URIs to be HTTPS (localhost is rejected), so this uses
a copy/paste flow -- the redirect target never needs to actually exist:

1. On https://developer.intuit.com create (or reuse) an app with the
   QuickBooks Online Accounting scope, and register the redirect URI below
   (see DEFAULT_REDIRECT_URI, or pass --redirect-uri) under the app's
   *production* keys.
2. Put QBO_CLIENT_ID / QBO_CLIENT_SECRET (production keys) in the .env file.
3. Run `uv run python scripts/qbo_auth.py url`, open the printed link, sign
   in as an admin of the QuickBooks company, and authorize it.
4. The browser lands on the redirect URI -- a 404 page is expected and fine.
   Copy the FULL URL from the address bar (it carries ?code=...&realmId=...)
   and run `uv run python scripts/qbo_auth.py exchange '<that url>'`.

On success, QBO_REFRESH_TOKEN and QBO_REALM_ID are written into the env
file and the connection is verified with a CompanyInfo API call. Tokens are
only ever printed masked. The refresh token is valid for 100 days and
rotates over time; the sync is responsible for persisting rotations.

If running from a git worktree, point --env-file at the parent repo's .env.
"""

import argparse
import secrets
import sys
import urllib.parse
from pathlib import Path

import requests
from dotenv import dotenv_values

AUTHORIZE_URL = "https://appcenter.intuit.com/connect/oauth2"
TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
SCOPE = "com.intuit.quickbooks.accounting"
DEFAULT_REDIRECT_URI = "https://hackclub.com/qbo-oauth-callback"
API_BASES = {
    "production": "https://quickbooks.api.intuit.com",
    "sandbox": "https://sandbox-quickbooks.api.intuit.com",
}
MINOR_VERSION = "75"


def mask(value):
    return f"{value[:4]}...{value[-4:]}" if len(value) > 12 else "***"


def read_client_creds(env_file):
    values = dotenv_values(env_file) if env_file.exists() else {}
    client_id = values.get("QBO_CLIENT_ID")
    client_secret = values.get("QBO_CLIENT_SECRET")
    if not client_id or not client_secret:
        sys.exit(
            f"QBO_CLIENT_ID / QBO_CLIENT_SECRET not found in {env_file}. "
            "Add the production keys from your app on https://developer.intuit.com first."
        )
    return client_id, client_secret


def upsert_env_var(env_file, key, value):
    lines = env_file.read_text().splitlines() if env_file.exists() else []
    entry = f"{key}={value}"
    replaced = False
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = entry
            replaced = True
    if not replaced:
        lines.append(entry)
    env_file.write_text("\n".join(lines) + "\n")


def state_file_for(env_file):
    return env_file.parent / ".qbo_oauth_state"


def cmd_url(args):
    client_id, _ = read_client_creds(args.env_file)
    state = secrets.token_urlsafe(16)
    state_file_for(args.env_file).write_text(state)
    params = {
        "client_id": client_id,
        "response_type": "code",
        "scope": SCOPE,
        "redirect_uri": args.redirect_uri,
        "state": state,
    }
    print("Open this URL in a browser, sign in, and authorize the company:\n")
    print(f"{AUTHORIZE_URL}?{urllib.parse.urlencode(params)}\n")
    print(
        "The app must have this exact redirect URI registered under its "
        f"production keys:\n  {args.redirect_uri}\n"
    )
    print(
        "After authorizing you'll land on that URI (404 is fine). Copy the full\n"
        "URL from the address bar, then run:\n"
        "  uv run python scripts/qbo_auth.py exchange '<pasted url>'"
    )


def cmd_exchange(args):
    client_id, client_secret = read_client_creds(args.env_file)

    parts = urllib.parse.urlsplit(args.redirect_url.strip().strip("'\""))
    query = urllib.parse.parse_qs(parts.query)
    code = query.get("code", [None])[0]
    realm_id = query.get("realmId", [None])[0]
    returned_state = query.get("state", [None])[0]
    if not code or not realm_id:
        sys.exit(
            "Could not find code= and realmId= in that URL. Paste the complete "
            "URL from the address bar after authorizing (quote it in the shell)."
        )

    state_file = state_file_for(args.env_file)
    if state_file.exists():
        if returned_state != state_file.read_text().strip():
            print("WARNING: state mismatch vs the last `url` run; continuing anyway.")
    else:
        print("WARNING: no saved state found (did you run the `url` step?); continuing.")

    resp = requests.post(
        TOKEN_URL,
        auth=(client_id, client_secret),
        headers={"Accept": "application/json"},
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": args.redirect_uri,
        },
        timeout=30,
    )
    if not resp.ok:
        sys.exit(
            f"Token exchange failed ({resp.status_code}): {resp.text}\n"
            "Auth codes are single-use and expire in ~10 minutes -- rerun the "
            "`url` step if needed. Also confirm the redirect URI here exactly "
            "matches the one registered on the app."
        )
    tokens = resp.json()
    refresh_token = tokens["refresh_token"]
    access_token = tokens["access_token"]
    refresh_days = int(tokens.get("x_refresh_token_expires_in", 0)) // 86400

    api_base = API_BASES[args.environment]
    company_name = "(could not verify)"
    info = requests.get(
        f"{api_base}/v3/company/{realm_id}/companyinfo/{realm_id}",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        params={"minorversion": MINOR_VERSION},
        timeout=30,
    )
    if info.ok:
        company_name = info.json()["CompanyInfo"]["CompanyName"]
    else:
        print(f"WARNING: CompanyInfo verification failed ({info.status_code}): {info.text}")

    upsert_env_var(args.env_file, "QBO_REFRESH_TOKEN", refresh_token)
    upsert_env_var(args.env_file, "QBO_REALM_ID", realm_id)
    state_file.unlink(missing_ok=True)

    print("\nConnected to QuickBooks Online:")
    print(f"  company:        {company_name}")
    print(f"  realm id:       {realm_id}")
    print(f"  refresh token:  {mask(refresh_token)} (valid ~{refresh_days} days, rotates)")
    print(f"\nWrote QBO_REFRESH_TOKEN and QBO_REALM_ID to {args.env_file}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(__file__).resolve().parents[1] / ".env",
        help="Path to the .env file to read client creds from and write tokens to",
    )
    parser.add_argument(
        "--redirect-uri",
        default=DEFAULT_REDIRECT_URI,
        help="Redirect URI registered on the Intuit app (must match exactly)",
    )
    parser.add_argument(
        "--environment",
        choices=sorted(API_BASES),
        default="production",
        help="Which QuickBooks API base to verify the connection against",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("url", help="Print the authorization URL to open in a browser")
    exchange = sub.add_parser("exchange", help="Exchange the pasted redirect URL for tokens")
    exchange.add_argument("redirect_url", help="Full URL from the address bar after authorizing")
    args = parser.parse_args()
    if args.command == "url":
        cmd_url(args)
    else:
        cmd_exchange(args)


if __name__ == "__main__":
    main()
