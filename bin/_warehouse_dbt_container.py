"""The dbt profile, and the program bin/warehouse-dbt --elevated runs in the app container.

Standard library only. The local route imports this file. The elevated route sends
this file's text to the container's Python on stdin, with a call to container_main
added at the end, so nothing but the container name appears on a command line.
"""

import base64
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from urllib.parse import parse_qs, unquote, urlparse

APP_ROOT = "/app"

SALT_VAR = "HACK_CLUBBERS_LOCATION_SALT"
PASSWORD_VAR = "DBT_ENV_SECRET_WAREHOUSE_PASSWORD"

# Only references to the environment, so the password is never written to disk.
# DBT_ENV_SECRET_ makes dbt scrub the value from its logs. The profile name, target
# and schema must match orpheus_engine/defs/dbt/definitions.py: the serving schemas
# are this schema plus each model's custom schema, so another value would build
# tables somewhere nothing reads them.
PROFILE_YAML = """\
orpheus_engine_dbt:
  target: prod
  outputs:
    prod:
      type: postgres
      host: "{{ env_var('WAREHOUSE_DBT_HOST') }}"
      port: "{{ env_var('WAREHOUSE_DBT_PORT') | as_number }}"
      user: "{{ env_var('WAREHOUSE_DBT_USER') }}"
      password: "{{ env_var('DBT_ENV_SECRET_WAREHOUSE_PASSWORD') }}"
      dbname: "{{ env_var('WAREHOUSE_DBT_DBNAME') }}"
      sslmode: "{{ env_var('WAREHOUSE_DBT_SSLMODE') }}"
      application_name: "{{ env_var('WAREHOUSE_DBT_APP_NAME') }}"
      schema: public
      threads: 4
      connect_timeout: 15
"""


def write_profile(directory):
    path = os.path.join(directory, "profiles.yml")
    with open(path, "w") as handle:
        handle.write(PROFILE_YAML)
    return path


def dbt_env(url, salt, application_name):
    """The variables dbt needs for this login, added to the current environment."""
    parsed = urlparse(url)
    if not parsed.hostname or not parsed.username or len(parsed.path) < 2:
        raise ValueError("WAREHOUSE_COOLIFY_URL needs a host, a user and a database")
    env = dict(os.environ)
    env.update(
        {
            "WAREHOUSE_DBT_HOST": parsed.hostname,
            "WAREHOUSE_DBT_PORT": str(parsed.port or 5432),
            "WAREHOUSE_DBT_USER": unquote(parsed.username),
            PASSWORD_VAR: unquote(parsed.password or ""),
            "WAREHOUSE_DBT_DBNAME": parsed.path[1:],
            "WAREHOUSE_DBT_SSLMODE": parse_qs(parsed.query).get("sslmode", ["prefer"])[0],
            "WAREHOUSE_DBT_APP_NAME": application_name,
            SALT_VAR: salt,
            # dbt sends usage data by default.
            "DO_NOT_TRACK": "1",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def _container_salt():
    """The salt the scheduled job uses here. Never generated: a new one moves offsets."""
    salt = os.environ.get(SALT_VAR)
    if salt:
        return salt
    try:
        with open(os.path.join(APP_ROOT, ".env")) as handle:
            for line in handle:
                if line.startswith(SALT_VAR + "="):
                    return line.split("=", 1)[1].strip().strip("'\"") or None
    except OSError:
        pass
    return None


def container_main(payload_b64, tree_b64):
    payload = json.loads(base64.b64decode(payload_b64))
    url = os.environ.get("WAREHOUSE_COOLIFY_URL")
    if not url:
        print("warehouse-dbt: this container has no WAREHOUSE_COOLIFY_URL", file=sys.stderr)
        return 2
    salt = _container_salt()
    if not salt:
        print(f"warehouse-dbt: this container has no {SALT_VAR}", file=sys.stderr)
        return 2

    work_dir = tempfile.mkdtemp(prefix="warehouse-dbt-")
    try:
        project_dir = os.path.join(work_dir, "project")
        with tarfile.open(fileobj=io.BytesIO(base64.b64decode(tree_b64))) as tar:
            tar.extractall(project_dir, filter="data")
        write_profile(work_dir)
        command = [os.path.join(APP_ROOT, ".venv", "bin", "dbt"), payload["command"]]
        command += ["--project-dir", project_dir, "--profiles-dir", work_dir]
        command += payload["args"]
        env = dbt_env(url, salt, "elevated-dbt:" + payload["reason"])
        return subprocess.run(
            command, cwd=work_dir, env=env, stdin=subprocess.DEVNULL
        ).returncode
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
