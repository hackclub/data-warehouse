import time

import requests
from flask import Blueprint, redirect, url_for, session, flash, current_app, render_template
from authlib.integrations.flask_client import OAuth

bp = Blueprint("auth", __name__)
oauth = OAuth()

GITHUB_TIMEOUT = 10
GAP_YEARS_TIMEOUT = 3
GAP_YEARS_RECHECK_SECONDS = 900
GAP_YEARS_RETRY_SECONDS = 60


def init_oauth(app):
    oauth.init_app(app)

    oauth.register(
        name="hackclub",
        server_metadata_url=f"{app.config['HACKCLUB_AUTH_URL']}/.well-known/openid-configuration",
        client_id=app.config["HACKCLUB_CLIENT_ID"],
        client_secret=app.config["HACKCLUB_CLIENT_SECRET"],
        client_kwargs={"scope": "openid profile email"},
    )

    oauth.register(
        name="github",
        authorize_url="https://github.com/login/oauth/authorize",
        access_token_url="https://github.com/login/oauth/access_token",
        client_id=app.config["GITHUB_CLIENT_ID"],
        client_secret=app.config["GITHUB_CLIENT_SECRET"],
        client_kwargs={"scope": "repo read:org"},
    )

    if app.config.get("AIRTABLE_CLIENT_ID"):
        oauth.register(
            name="airtable",
            authorize_url="https://airtable.com/oauth2/v1/authorize",
            access_token_url="https://airtable.com/oauth2/v1/token",
            client_id=app.config["AIRTABLE_CLIENT_ID"],
            client_secret=app.config["AIRTABLE_CLIENT_SECRET"],
            client_kwargs={
                "scope": "data.records:read schema.bases:read",
                "code_challenge_method": "S256",
                "token_endpoint_auth_method": "client_secret_basic",
            },
        )


@bp.record_once
def on_register(state):
    init_oauth(state.app)


@bp.route("/login")
def login():
    if session.get("user") and session.get("github_token"):
        return redirect(url_for("wizard.setup"))
    return render_template("login.html")


@bp.route("/auth/hackclub")
def hackclub_auth():
    redirect_uri = f"{current_app.config['APP_URL']}/auth/hackclub/callback"
    return oauth.hackclub.authorize_redirect(redirect_uri)


@bp.route("/auth/hackclub/callback")
def hackclub_callback():
    try:
        token = oauth.hackclub.authorize_access_token()
    except Exception as e:
        flash(f"Authentication failed: {e}", "alert")
        return redirect(url_for("auth.login"))

    userinfo = token.get("userinfo", {})
    if not userinfo:
        userinfo = oauth.hackclub.userinfo()

    email = userinfo.get("email", "")
    name = userinfo.get("name", email)

    session["user"] = {
        "email": email,
        "name": name,
        "hca_id": userinfo.get("sub"),
        "hackclub_email": email.endswith("@hackclub.com"),
    }

    flash("Authenticated with Hack Club. Now connect GitHub.", "notice")
    return redirect(url_for("auth.login"))


@bp.route("/auth/github")
def github_auth():
    redirect_uri = f"{current_app.config['APP_URL']}/auth/github/callback"
    return oauth.github.authorize_redirect(redirect_uri)


@bp.route("/auth/github/callback")
def github_callback():
    try:
        token = oauth.github.authorize_access_token()
    except Exception as e:
        flash(f"Authentication failed: {e}", "alert")
        return redirect(url_for("auth.login"))

    gh_token = token.get("access_token")
    if not gh_token:
        flash(f"GitHub declined the connection: {token.get('error_description') or token.get('error') or 'no access token returned'}", "alert")
        return redirect(url_for("auth.login"))

    try:
        r = requests.get(
            "https://api.github.com/user",
            headers={"Authorization": f"Bearer {gh_token}"},
            timeout=GITHUB_TIMEOUT,
        )
        r.raise_for_status()
        gh_user = r.json()
    except requests.RequestException as e:
        flash(f"Could not read your GitHub profile: {e}", "alert")
        return redirect(url_for("auth.login"))

    username = gh_user.get("login", "")

    session["github_token"] = gh_token
    user = session.get("user", {})
    user["github_username"] = username
    user["github_avatar"] = gh_user.get("avatar_url", "")
    user["gap_year_member"] = _check_gap_years(gh_token, username) is True
    user["gap_year_recheck_at"] = time.time() + GAP_YEARS_RECHECK_SECONDS
    session["user"] = user

    if _authorized():
        _regenerate_session()
        flash("Welcome, citizen. You are cleared for access.", "notice")
        return redirect(url_for("wizard.setup"))
    else:
        session.clear()
        flash(
            "Access denied. You must have a @hackclub.com email or be a member of the gap-years team.",
            "alert",
        )
        return redirect(url_for("auth.login"))


@bp.route("/auth/airtable")
def airtable_auth():
    if not current_app.config.get("AIRTABLE_CLIENT_ID"):
        flash("Airtable integration is not configured.", "alert")
        return redirect(url_for("wizard.setup"))

    redirect_uri = f"{current_app.config['APP_URL']}/auth/airtable/callback"
    return oauth.airtable.authorize_redirect(redirect_uri)


@bp.route("/auth/airtable/callback")
def airtable_callback():
    try:
        token = oauth.airtable.authorize_access_token()
    except Exception as e:
        flash(f"Airtable authentication failed: {e}", "alert")
        return redirect(url_for("wizard.setup"))

    if not token.get("access_token"):
        flash(f"Airtable declined the connection: {token.get('error_description') or token.get('error') or 'no access token returned'}", "alert")
        return redirect(url_for("wizard.setup"))

    session["airtable_token"] = token["access_token"]
    if token.get("refresh_token"):
        session["airtable_refresh_token"] = token["refresh_token"]

    flash("Connected to Airtable. Now pick your base.", "notice")
    wid = session.pop("wizard_wid", "")
    if wid:
        return redirect(url_for("wizard.airtable_bases", wid=wid))
    return redirect(url_for("wizard.airtable_bases"))


def refresh_airtable_token():
    """Try to refresh the airtable access token using the stored refresh token.

    Returns the new access token on success, None on failure. Callers should
    redirect to airtable OAuth if this returns None.
    """
    refresh_token = session.get("airtable_refresh_token")
    client_id = current_app.config.get("AIRTABLE_CLIENT_ID")
    client_secret = current_app.config.get("AIRTABLE_CLIENT_SECRET")
    if not refresh_token or not client_id:
        return None

    try:
        r = requests.post(
            "https://airtable.com/oauth2/v1/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
            },
            auth=(client_id, client_secret),
            timeout=10,
        )
        if r.status_code != 200:
            return None
        token = r.json()
        session["airtable_token"] = token["access_token"]
        if token.get("refresh_token"):
            session["airtable_refresh_token"] = token["refresh_token"]
        session.modified = True
        return token["access_token"]
    except Exception:
        return None


@bp.route("/logout", methods=["POST"])
def logout():
    session.clear()
    flash("Signed out. The computer will miss you.", "notice")
    return redirect(url_for("auth.login"))


def _authorized():
    """Is the current session still allowed in?

    Called on every request, not just at login: gap-years membership is
    re-checked against GitHub once the cached answer goes stale, so someone
    removed from the team loses access without having to log out. That check
    sits in front of every request for gap-year users, so it is on a short
    timeout and the next attempt is scheduled even when GitHub does not answer
    — an outage must not mean a fresh 3s dial on every single request.
    """
    user = session.get("user")
    if not user:
        return False
    if user.get("hackclub_email"):
        return True

    token = session.get("github_token")
    if token and time.time() >= user.get("gap_year_recheck_at", 0):
        member = _check_gap_years(token, user.get("github_username", ""), timeout=GAP_YEARS_TIMEOUT)
        if member is not None:
            user["gap_year_member"] = member
        wait = GAP_YEARS_RECHECK_SECONDS if member is not None else GAP_YEARS_RETRY_SECONDS
        user["gap_year_recheck_at"] = time.time() + wait
        session["user"] = user

    return bool(user.get("gap_year_member"))


def _regenerate_session():
    """Mint a fresh session id so a pre-login cookie cannot be replayed."""
    current_app.session_interface.regenerate(session)


def _check_gap_years(token, username, timeout=GITHUB_TIMEOUT):
    """True/False for membership, None if GitHub could not answer."""
    if not username:
        return False
    try:
        r = requests.get(
            f"https://api.github.com/orgs/hackclub/teams/gap-years/memberships/{username}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )
    except requests.RequestException:
        return None
    if r.status_code == 404:
        return False
    if r.status_code in (401, 403):
        return False
    if r.status_code != 200:
        return None
    return r.json().get("state") == "active"
