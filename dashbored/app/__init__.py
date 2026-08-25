import os
import secrets
import tempfile
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit
from flask import Flask
from flask_session import Session


def _repo_root():
    """The orpheus-engine checkout used to validate generated code, or None.

    REPO_ROOT wins if it points at a real checkout; otherwise fall back to the
    directory this package was installed under. In the Docker image neither
    holds orpheus_engine/, and None is the honest answer.
    """
    candidates = [os.environ.get("REPO_ROOT"), Path(__file__).resolve().parent.parent.parent]
    for candidate in candidates:
        if candidate and (Path(candidate) / "orpheus_engine").is_dir():
            return Path(candidate)
    return None


def _loopback(app_url):
    """Are we serving ourselves on localhost? Then this is somebody's laptop.

    An OAuth deployment cannot work on a loopback APP_URL, so this is a safe
    stand-in for "development" — it decides whether the wizard is allowed to
    dial private database hosts, which in production would be an SSRF.
    """
    host = urlsplit(app_url or "").hostname or ""
    return host in ("localhost", "127.0.0.1", "::1") or host.endswith(".localhost")


def _flag(name, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def create_app():
    from dotenv import load_dotenv

    env_file = Path(__file__).resolve().parent.parent / ".env.development"
    if env_file.exists():
        load_dotenv(env_file)

    app = Flask(__name__, static_folder="static", static_url_path="/static")

    secret = os.environ.get("SECRET_KEY")
    if not secret and not app.debug:
        raise RuntimeError("SECRET_KEY must be set in production")
    app.config["SECRET_KEY"] = secret or f"dev-{secrets.token_hex(32)}"

    session_dir = os.environ.get("SESSION_FILE_DIR", os.path.join(tempfile.gettempdir(), "dashbored_sessions"))
    app.config["SESSION_TYPE"] = "filesystem"
    app.config["SESSION_FILE_DIR"] = session_dir
    app.config["SESSION_PERMANENT"] = False
    # The session is read-modify-write per request with no locking, and the
    # wizard runs long requests (Airtable paging, DAU preview) concurrently with
    # form posts on 8 gthread threads. Refreshing on every request means a
    # read-only one re-upserts the snapshot it loaded and silently reverts the
    # write that landed while it was running.
    app.config["SESSION_REFRESH_EACH_REQUEST"] = False
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=12)
    if not app.debug:
        app.config["SESSION_COOKIE_SECURE"] = True

    app.config["APP_URL"] = os.environ.get("APP_URL", "http://localhost:5000")
    app.config["HACKCLUB_CLIENT_ID"] = os.environ.get("HACKCLUB_CLIENT_ID", "")
    app.config["HACKCLUB_CLIENT_SECRET"] = os.environ.get("HACKCLUB_CLIENT_SECRET", "")
    app.config["HACKCLUB_AUTH_URL"] = os.environ.get(
        "HACKCLUB_AUTH_URL", "https://auth.hackclub.com"
    )
    app.config["GITHUB_CLIENT_ID"] = os.environ.get("GITHUB_CLIENT_ID", "")
    app.config["GITHUB_CLIENT_SECRET"] = os.environ.get("GITHUB_CLIENT_SECRET", "")
    app.config["WAREHOUSE_READONLY_URL"] = os.environ.get("WAREHOUSE_READONLY_URL", "")
    app.config["AGE_PUBLIC_KEY"] = os.environ.get("AGE_PUBLIC_KEY", "")
    app.config["AIRTABLE_CLIENT_ID"] = os.environ.get("AIRTABLE_CLIENT_ID", "")
    app.config["AIRTABLE_CLIENT_SECRET"] = os.environ.get("AIRTABLE_CLIENT_SECRET", "")

    app.config["REPO_ROOT"] = _repo_root()
    app.config["ALLOW_PRIVATE_DB_HOSTS"] = _flag(
        "ALLOW_PRIVATE_DB_HOSTS", app.debug or _loopback(app.config["APP_URL"])
    )

    Session(app)

    from . import auth, wizard, api

    app.register_blueprint(auth.bp)
    app.register_blueprint(wizard.bp, url_prefix="/wizard")
    app.register_blueprint(api.bp, url_prefix="/api")

    from .vite import vite_tags
    from flask import url_for as flask_url_for, request as flask_request

    def wurl(endpoint, **kwargs):
        wid = kwargs.pop("wid", None) or flask_request.args.get("wid", "")
        if wid:
            kwargs["wid"] = wid
        return flask_url_for(endpoint, **kwargs)

    app.jinja_env.globals["vite_tags"] = vite_tags
    app.jinja_env.globals["wurl"] = wurl

    @app.route("/")
    def index():
        from flask import redirect, url_for

        return redirect(url_for("auth.login"))

    return app
