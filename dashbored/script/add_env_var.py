#!/usr/bin/env python3
"""
Pull a dashbored PR, decrypt the connection URL, and slam it into Coolify.

Usage:
    script/add_env_var.py 57

Interactive by design: it prints the PR author and makes you type their login
before anything reaches production, so it needs a terminal on stdin.

Needs only requests and python-dotenv, plus these in .env (dashbored/.env or
parent .env):
    COOLIFY_API_URL       — e.g. https://coolify.example.com
    COOLIFY_API_TOKEN     — bearer token (Settings → API Tokens in Coolify)
    COOLIFY_APP_UUID      — UUID of the dagster application in Coolify
    AGE_KEY_FILE          — path to the age private key file

The PR body is untrusted: the upstream repo is public and the age recipient is a
public key, so anyone can open a PR carrying a name/value pair of their choosing.
The name has to look like the env var the wizard generates, and the operator has
to name the PR author before anything is written to production.
"""

import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import requests
from dotenv import dotenv_values

DASHBORED = Path(__file__).resolve().parent.parent
PR_CREATOR = DASHBORED / "app" / "services" / "github_pr_creator.py"


def upstream_repo():
    """Read UPSTREAM_REPO out of github_pr_creator.py without importing it.

    Importing the module would drag in flask, libcst and yaml for one string.
    """
    for node in ast.parse(PR_CREATOR.read_text()).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "UPSTREAM_REPO" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    print(f"UPSTREAM_REPO is not assigned in {PR_CREATOR}", file=sys.stderr)
    sys.exit(1)


UPSTREAM_REPO = upstream_repo()

ENV_VAR_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*_DATABASE_URL$")
ENV_VAR_MAX_LEN = 64


def load_env():
    """Load .env files, dashbored first then parent repo."""
    env = {}
    for p in [DASHBORED / ".env", DASHBORED.parent / ".env"]:
        if p.exists():
            for k, v in dotenv_values(p).items():
                if v is not None:
                    env.setdefault(k, v)
    return env


def prompt(message):
    if not sys.stdin.isatty():
        print("stdin is not a terminal, and this script asks before it writes to", file=sys.stderr)
        print("production. run it from a terminal.", file=sys.stderr)
        sys.exit(1)
    return input(message)


def require_env(env, key):
    val = env.get(key) or os.environ.get(key)
    if not val:
        print(f"missing {key} in .env or environment", file=sys.stderr)
        sys.exit(1)
    return val


def fetch_pr(pr_number):
    result = subprocess.run(
        ["gh", "pr", "view", str(pr_number), "--repo", UPSTREAM_REPO,
         "--json", "author,title,url,body"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"gh pr view failed: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def parse_pr(body):
    env_match = re.search(r"env var: `([^`]+)`", body)
    if not env_match:
        print("couldn't find env var name in PR body", file=sys.stderr)
        sys.exit(1)
    env_var_name = env_match.group(1)

    if len(env_var_name) > ENV_VAR_MAX_LEN or not ENV_VAR_RE.match(env_var_name):
        print(f"refusing env var name from PR body: {env_var_name!r}", file=sys.stderr)
        print("dashbored only ever generates <PROGRAM>_DATABASE_URL", file=sys.stderr)
        sys.exit(1)

    encrypted_match = re.search(
        r"```\s*\n(-----BEGIN AGE ENCRYPTED FILE-----\n.*?-----END AGE ENCRYPTED FILE-----)\s*\n```",
        body, re.DOTALL,
    )
    if not encrypted_match:
        print("couldn't find age-encrypted block in PR body", file=sys.stderr)
        print("(is this an airtable PR? those don't have connection URLs)", file=sys.stderr)
        sys.exit(1)

    return env_var_name, encrypted_match.group(1)


def confirm_author(pr, env_var_name):
    author = pr.get("author", {}).get("login", "")
    print(pr["title"])
    print(f"  {pr['url']}")
    print(f"  author:  @{author or '???'}")
    print(f"  env var: {env_var_name}")
    print()
    print(f"anyone can open a PR on {UPSTREAM_REPO} and encrypt to the public age key.")
    print(f"only continue if @{author} is meant to have a secret in production.")
    typed = prompt("type the author's login to continue: ").strip().lstrip("@")
    if not author or typed != author:
        print("aborted", file=sys.stderr)
        sys.exit(1)


def decrypt_age(ciphertext, key_file):
    result = subprocess.run(
        ["age", "--decrypt", "--identity", key_file],
        input=ciphertext, capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"age decrypt failed: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def add_to_coolify(api_url, token, app_uuid, key, value):
    return requests.patch(
        f"{api_url.rstrip('/')}/api/v1/applications/{app_uuid}/envs/bulk",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        json={
            "data": [{
                "key": key,
                "value": value,
                "is_preview": False,
                "is_literal": True,
                "is_shown_once": True,
            }],
        },
        timeout=30,
    )


def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print(f"usage: {sys.argv[0]} <pr-number>", file=sys.stderr)
        sys.exit(1)

    pr_number = sys.argv[1]
    env = load_env()

    coolify_url = require_env(env, "COOLIFY_API_URL")
    coolify_token = require_env(env, "COOLIFY_API_TOKEN")
    app_uuid = require_env(env, "COOLIFY_APP_UUID")
    key_file = require_env(env, "AGE_KEY_FILE")

    if not Path(key_file).exists():
        print(f"AGE_KEY_FILE not found: {key_file}", file=sys.stderr)
        sys.exit(1)

    print(f"fetching {UPSTREAM_REPO}#{pr_number}...")
    pr = fetch_pr(pr_number)

    env_var_name, encrypted_url = parse_pr(pr["body"])
    confirm_author(pr, env_var_name)

    print("decrypting connection URL...")
    plaintext_url = decrypt_age(encrypted_url, key_file)
    print(f"decrypted ({len(plaintext_url)} chars)")

    # sanity check — should look like a connection string
    if not any(plaintext_url.startswith(s) for s in ("postgres://", "postgresql://", "mysql://", "http")):
        print(f"warning: decrypted value doesn't look like a connection URL:", file=sys.stderr)
        print(f"  {plaintext_url[:40]}...", file=sys.stderr)
        resp = prompt("continue anyway? [y/N] ")
        if resp.lower() != "y":
            sys.exit(1)

    print(f"adding {env_var_name} to Coolify app {app_uuid[:8]}...")
    r = add_to_coolify(coolify_url, coolify_token, app_uuid, env_var_name, plaintext_url)

    if r.ok:
        print(f"done! {env_var_name} is set. redeploy to pick it up.")
    else:
        print(f"coolify returned {r.status_code}:", file=sys.stderr)
        print(r.text, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
