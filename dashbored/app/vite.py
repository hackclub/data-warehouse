import json
from pathlib import Path
from flask import current_app, url_for
from markupsafe import Markup

_manifest_cache = None

DEV_SERVER = "http://localhost:5173"


def vite_tags():
    global _manifest_cache

    if current_app.debug:
        return Markup(
            f'<script type="module" src="{DEV_SERVER}/@vite/client"></script>\n'
            f'<script type="module" src="{DEV_SERVER}/frontend/entrypoints/application.js"></script>'
        )

    if _manifest_cache is None:
        manifest_path = Path(current_app.static_folder) / "vite" / ".vite" / "manifest.json"
        if not manifest_path.exists():
            return Markup("<!-- vite manifest not found -->")
        _manifest_cache = json.loads(manifest_path.read_text())

    entry = _manifest_cache.get("frontend/entrypoints/application.js", {})
    tags = []
    for css in entry.get("css", []):
        href = url_for("static", filename=f"vite/{css}")
        tags.append(f'<link rel="stylesheet" href="{href}">')
    if entry.get("file"):
        src = url_for("static", filename="vite/" + entry["file"])
        tags.append(f'<script type="module" src="{src}"></script>')
    return Markup("\n".join(tags))
