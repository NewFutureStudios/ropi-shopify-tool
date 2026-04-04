"""
Ropi Shopify Tool — Web Interface
Lokaal:  python app.py  →  http://localhost:5000
Vercel:  automatisch via vercel.json
"""

import os
import re
import time
import base64
import secrets
import threading
import uuid
import json as _json_module
import requests
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, request, jsonify, session, redirect, Response
from dotenv import load_dotenv

SNAPSHOTS_DIR = Path(__file__).parent / "snapshots"
SNAPSHOTS_DIR.mkdir(exist_ok=True)

load_dotenv()

GITHUB_TOKEN          = os.getenv("GITHUB_TOKEN", "")
GITHUB_SNAPSHOTS_REPO = os.getenv("GITHUB_SNAPSHOTS_REPO", "NewFutureStudios/ropi-snapshots")
ROPI_API_KEY          = os.getenv("ROPI_API_KEY", "")  # voor remote control via Claude app

# ── Server-side sync jobs ──────────────────────────────────────────────────────
_sync_jobs = {}  # job_id -> dict (draait in background thread, onafhankelijk van browser)


def _github_push_snapshot(filename, content_str):
    """Push een snapshot JSON naar de private GitHub repo via de GitHub Contents API."""
    if not GITHUB_TOKEN:
        return False, "Geen GITHUB_TOKEN ingesteld"
    url = f"https://api.github.com/repos/{GITHUB_SNAPSHOTS_REPO}/contents/snapshots/{filename}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    encoded = base64.b64encode(content_str.encode()).decode()
    # Check of bestand al bestaat (voor SHA bij update)
    sha = None
    existing = requests.get(url, headers=headers, timeout=10)
    if existing.status_code == 200:
        sha = existing.json().get("sha")

    payload = {
        "message": f"snapshot: {filename}",
        "content": encoded,
    }
    if sha:
        payload["sha"] = sha

    resp = requests.put(url, headers=headers, json=payload, timeout=30)
    if resp.status_code in (200, 201):
        return True, resp.json().get("content", {}).get("html_url", "")
    return False, resp.text


def _github_list_snapshots():
    """Haal de lijst van snapshot bestanden op uit de GitHub repo."""
    if not GITHUB_TOKEN:
        return []
    url = f"https://api.github.com/repos/{GITHUB_SNAPSHOTS_REPO}/contents/snapshots"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code == 200:
        return [f["name"] for f in resp.json() if f["name"].endswith(".json")]
    return []


def _github_download_snapshot(filename):
    """Download en parseer een snapshot JSON uit de GitHub repo."""
    if not GITHUB_TOKEN:
        return None
    url = f"https://api.github.com/repos/{GITHUB_SNAPSHOTS_REPO}/contents/snapshots/{filename}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.status_code != 200:
        return None
    content = base64.b64decode(resp.json()["content"]).decode()
    return _json_module.loads(content)

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "ropi-local-dev-secret-change-in-production")

SHOPIFY_API_VERSION   = "2026-04"
SHOPIFY_CLIENT_ID     = os.getenv("SHOPIFY_CLIENT_ID", "")
SHOPIFY_CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET", "")
SHOPIFY_SCOPES        = "read_products,write_products"


# ── Shopify helpers ───────────────────────────────────────────────────────────

def shopify_headers(token):
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}


def get_shopify_products_page(store_url, token, page_url=None):
    """Haal één pagina producten op (max 250). Geeft (products, next_url) terug."""
    if page_url:
        url = page_url
        params = {}
    else:
        url = f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/products.json"
        params = {"limit": 250, "fields": "id,title,variants,images,image"}
    resp = requests.get(url, headers=shopify_headers(token), params=params, timeout=9)
    resp.raise_for_status()
    data = resp.json()
    products = data.get("products", [])
    next_url = None
    link = resp.headers.get("Link", "")
    if 'rel="next"' in link:
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>")
                break
    return products, next_url


def get_existing_image_srcs(store_url, token, product_id):
    url = f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id}/images.json"
    resp = requests.get(url, headers=shopify_headers(token), timeout=15)
    resp.raise_for_status()
    images = resp.json().get("images", [])
    return {img["src"].split("?")[0] for img in images}


def add_image_to_shopify(store_url, token, product_id, image_url, position, fallback_url=None):
    """Voeg afbeelding toe aan Shopify. Probeert highres URL eerst, valt terug op original bij 422."""
    api_url = f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id}/images.json"
    for url_to_try in filter(None, [image_url, fallback_url if fallback_url != image_url else None]):
        payload = {"image": {"src": url_to_try, "position": position}}
        resp = requests.post(api_url, headers=shopify_headers(token), json=payload, timeout=15)
        if resp.status_code == 422:
            continue   # probeer fallback
        resp.raise_for_status()
        return True
    return False


# ── Auth helper ───────────────────────────────────────────────────────────────

def _auth_shopify():
    """Geeft (store_url, shopify_token) terug via sessie of X-Ropi-Key header."""
    api_key = request.headers.get("X-Ropi-Key", "")
    if ROPI_API_KEY and api_key == ROPI_API_KEY:
        return (
            os.getenv("SHOPIFY_STORE_URL", session.get("store_url", "")),
            os.getenv("SHOPIFY_ACCESS_TOKEN", session.get("shopify_token", "")),
        )
    return session.get("store_url", ""), session.get("shopify_token")


# ── Server-side sync worker ────────────────────────────────────────────────────

def _sync_worker(job_id, store_url, shopify_token, bol_client_id, bol_client_secret, dry_run, test_mode=False):
    """Draait als daemon thread — volledig onafhankelijk van de browser."""
    job = _sync_jobs[job_id]

    def jlog(level, icon, msg):
        job["logs"].append({"level": level, "icon": icon, "msg": msg})

    # ── Stap 1: Bol.com token ophalen ─────────────────────────────────────────
    try:
        bol_token, expires_in = get_bol_token(bol_client_id, bol_client_secret)
        bol_token_expires = time.time() + expires_in - 120
        jlog("ok", "🔑", f"Bol.com token verkregen (geldig {expires_in // 60} min)")
    except Exception as e:
        job["status"] = "error"
        job["error"] = f"Bol.com login mislukt: {e}"
        return

    # ── Stap 2: Shopify producten laden ───────────────────────────────────────
    job["phase"] = "loading"
    jlog("info", "🛍", "Shopify producten laden…")
    products = []
    page_url = None
    page_num = 0
    try:
        while True:
            page_num += 1
            raw, next_url = get_shopify_products_page(store_url, shopify_token, page_url)
            for p in raw:
                ean = None
                for v in p.get("variants", []):
                    bc = (v.get("barcode") or "").strip()
                    if bc:
                        ean = bc
                        break
                img = p.get("image")
                products.append({
                    "id":    p["id"],
                    "title": p.get("title", ""),
                    "ean":   ean,
                    "thumb": img["src"] if img else None,
                })
            if not next_url:
                break
            page_url = next_url
            if page_num % 5 == 0:
                jlog("info", "🛍", f"{len(products)} producten geladen…")
    except Exception as e:
        job["status"] = "error"
        job["error"] = f"Shopify producten laden mislukt: {e}"
        return

    # Test-modus: alleen eerste 3 producten verwerken
    if test_mode:
        products = products[:3]
        jlog("info", "⚡", f"Test-modus: alleen eerste {len(products)} producten")

    job["total"]    = len(products)
    job["products"] = products
    job["phase"]    = "syncing"
    job["status"]   = "running"
    jlog("ok", "🛍", f"{len(products)} producten geladen — sync gestart")

    # ── Stap 3: Per product syncen ────────────────────────────────────────────
    for i, p in enumerate(products):
        if job.get("stop_requested"):
            job["status"] = "stopped"
            jlog("warn", "⏹", "Sync gestopt door gebruiker")
            return

        job["progress"]        = i
        job["current_product"] = p["title"]

        # Auto-refresh Bol.com token vóór het verloopt
        if time.time() > bol_token_expires:
            try:
                bol_token, expires_in = get_bol_token(bol_client_id, bol_client_secret)
                bol_token_expires = time.time() + expires_in - 120
                jlog("ok", "🔑", "Bol.com token automatisch vernieuwd")
            except Exception as e:
                jlog("warn", "🔑", f"Token vernieuwen mislukt: {e}")

        if not p["ean"]:
            job["stats"]["nobol"] += 1
            job["nobol_list"].append({"title": p["title"], "ean": "(leeg)", "reden": "Geen EAN in Shopify"})
            job["results"][str(p["id"])] = {"status": "no_ean", "added": 0, "skipped": 0}
            continue

        if dry_run:
            # Dry run: geen Shopify writes → alleen Bol.com rate limit telt
            time.sleep(0.8)
        else:
            # Echte sync: rustpauze elke 50 producten + 3 sec per product
            if i > 0 and i % 50 == 0:
                jlog("info", "⏸", f"Rustpauze na {i} producten (20 sec)…")
                for _ in range(20):
                    if job.get("stop_requested"):
                        break
                    time.sleep(1)
            time.sleep(3)

        try:
            bol_images = get_bol_images(p["ean"], bol_token)
        except Exception as e:
            job["stats"]["errors"] += 1
            job["results"][str(p["id"])] = {"status": "error", "added": 0, "skipped": 0, "message": str(e)}
            jlog("error", "✗", f"{p['title']} — {e}")
            continue

        if not bol_images:
            job["stats"]["nobol"] += 1
            job["nobol_list"].append({"title": p["title"], "ean": p["ean"], "reden": "Geen afbeeldingen op Bol.com"})
            job["results"][str(p["id"])] = {"status": "no_bol", "added": 0, "skipped": 0}
            continue

        try:
            existing = get_existing_image_srcs(store_url, shopify_token, p["id"])
        except Exception as e:
            job["stats"]["errors"] += 1
            job["results"][str(p["id"])] = {"status": "error", "added": 0, "skipped": 0, "message": str(e)}
            continue

        added         = 0
        skipped       = 0
        next_position = max(len(existing) + 1, 2)
        errors_p      = []

        for img in bol_images:
            highres  = img["highres"]
            original = img["original"]
            img_filename = original.split("/")[-1].split("?")[0]
            if any(img_filename in ex for ex in existing):
                skipped += 1
                continue
            if not dry_run:
                try:
                    if add_image_to_shopify(store_url, shopify_token, p["id"], highres, next_position, fallback_url=original):
                        added += 1
                        next_position += 1
                    time.sleep(0.3)
                except Exception as e:
                    errors_p.append(str(e))
            else:
                added += 1

        job["stats"]["added"]   += added
        job["stats"]["skipped"] += skipped

        if errors_p and added == 0 and skipped == 0:
            job["stats"]["errors"] += 1
            job["results"][str(p["id"])] = {"status": "error", "added": 0, "skipped": skipped, "message": errors_p[0]}
            jlog("error", "✗", f"{p['title']} — {errors_p[0]}")
        elif added > 0:
            job["results"][str(p["id"])] = {"status": "success", "added": added, "skipped": skipped}
        else:
            job["results"][str(p["id"])] = {"status": "unchanged", "added": 0, "skipped": skipped}

    # ── Klaar ─────────────────────────────────────────────────────────────────
    job["progress"]    = len(products)
    job["status"]      = "done"
    job["finished_at"] = datetime.utcnow().isoformat()
    s = job["stats"]
    jlog("ok", "🎉", f"Klaar — {s['added']} toegevoegd, {s['skipped']} al aanwezig, {s['nobol']} niet op Bol, {s['errors']} fouten")


# ── Bol.com helpers ───────────────────────────────────────────────────────────

import json as _json

BOL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def get_bol_token(client_id, client_secret):
    resp = requests.post(
        "https://login.bol.com/token",
        params={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"], data.get("expires_in", 3600)


def _to_highres(url):
    """Haal de hoogste beschikbare resolutie op van een Bol.com CDN URL.

    De API-URL heeft het formaat:
        https://media.s-bol.com/{hash1}/{hash2}/500x700.jpg
    De dimensie (500x700) IS de bestandsnaam — niet een pad-segment.

    We vervangen die door 2000x2000: Bol.com CDN ondersteunt dynamische resize,
    en bij 2000 is de kans groot dat we de volledige originele upload krijgen.
    Als de originele upload kleiner is geeft het CDN de originele maat terug.
    """
    url = re.sub(r'\?.*$', '', url)  # verwijder query params eerst

    # API-patroon: /hash/500x700.jpg  →  /hash/1200x1200.jpg
    # 1200x1200 is het hoogste formaat dat Bol.com CDN genereert (bevestigd via paginabron)
    url = re.sub(
        r'/(\d+)x(\d+)\.(jpg|jpeg|png|webp)$',
        r'/1200x1200.\3',
        url,
        flags=re.IGNORECASE,
    )
    # HTML-scrape patroon: /500x700/bestand.jpg  →  /1200x1200/bestand.jpg
    url = re.sub(r'/\d+x\d+/', '/1200x1200/', url)

    return url


def _extract_images_from_html(html):
    """Haal hoogste-resolutie afbeeldingen uit een Bol.com pagina."""
    images = []

    # Methode 1: media gallery JSON in paginastate (meest compleet, alle productfoto's)
    for m in re.findall(r'"mediaGalleryItems"\s*:\s*(\[[^\]]+\])', html):
        try:
            items = _json.loads(m)
            for item in items:
                src = item.get("src") or item.get("url") or item.get("image")
                if src and "bol.com" in src:
                    images.append(_to_highres(src))
        except Exception:
            pass

    # Methode 2: JSON-LD Product schema
    if not images:
        for m in re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL):
            try:
                ld = _json.loads(m.strip())
                for item in (ld if isinstance(ld, list) else [ld]):
                    if item.get("@type") == "Product":
                        imgs = item.get("image", [])
                        if isinstance(imgs, str):
                            imgs = [imgs]
                        images.extend([_to_highres(i) for i in imgs])
            except Exception:
                pass

    # Methode 3: alle s-bol.com / img.s-bol.com afbeeldingen in de HTML
    if not images:
        found = re.findall(r'https://(?:img\.s-bol\.com|media\.s-bol\.com|s-bol\.com)/[^\s"\'<>]+\.(?:jpg|jpeg|png|webp)', html)
        images = list(dict.fromkeys([_to_highres(u) for u in found]))  # uniek, volgorde behouden

    # Methode 4: og:image fallback
    if not images:
        og = re.search(r'<meta property="og:image" content="([^"]+)"', html)
        if og:
            images = [_to_highres(og.group(1))]

    return list(dict.fromkeys(images))  # dedupliceer


def _bol_get(url, retries=2):
    """GET naar Bol.com met retry bij 429/503."""
    for attempt in range(retries + 1):
        resp = requests.get(url, headers=BOL_HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code == 429 or resp.status_code == 503:
            time.sleep(3 * (attempt + 1))
            continue
        return resp
    return resp


def get_bol_images(ean, bol_token):
    """Haal ALLE Bol.com afbeeldingen op via de Retailer API v10 assets endpoint.

    usage=IMAGE geeft zowel de PRIMARY als alle ADDITIONAL afbeeldingen terug.
    assets worden gesorteerd op 'order' (carousel-volgorde op de productpagina).
    Per asset pakken we altijd de hoogste resolutie variant.
    """
    headers = {
        "Authorization": f"Bearer {bol_token}",
        "Accept": "application/vnd.retailer.v10+json",
    }
    # Retry bij 429/503/502/invalid JSON met exponential backoff
    for attempt in range(5):
        try:
            resp = requests.get(
                f"https://api.bol.com/retailer/products/{ean}/assets",
                headers=headers,
                params={"usage": "ADDITIONAL"},  # ← geeft alleen extra foto's terug, niet de PRIMARY
                timeout=20,
            )
        except requests.exceptions.RequestException as e:
            if attempt < 4:
                time.sleep(5 * (attempt + 1))
                continue
            raise Exception(f"Bol.com verbindingsfout: {e}")

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 15)) if attempt == 0 else (2 ** attempt) * 8
            time.sleep(wait)
            continue
        if resp.status_code in (502, 503, 504):
            time.sleep(5 * (attempt + 1))
            continue
        if resp.status_code == 401:
            raise Exception("BOL_TOKEN_EXPIRED")
        if resp.status_code == 404:
            return []
        resp.raise_for_status()

        # Vang non-JSON responses op (bijv. HTML throttle-pagina)
        try:
            data = resp.json()
        except ValueError:
            snippet = resp.text[:200].replace("\n", " ")
            if attempt < 4:
                time.sleep(8 * (attempt + 1))
                continue
            raise Exception(f"Bol.com gaf geen geldig JSON (status {resp.status_code}): {snippet}")

        # Sorteer op carousel-volgorde zodat de afbeeldingen in de juiste volgorde in Shopify komen
        assets = sorted(data.get("assets", []), key=lambda a: a.get("order", 999))

        images = []
        for asset in assets:
            # Sla niet-afbeeldingen over én de PRIMARY (die staat al als hoofdfoto in Shopify)
            usage = asset.get("usage", "ADDITIONAL")
            if usage.upper() in ("PRIMARY", "VIDEO", "DOCUMENT", "SPIN_360"):
                continue
            variants = sorted(asset.get("variants", []), key=lambda v: v.get("width", 0), reverse=True)
            if variants:
                original = variants[0]["url"]
                images.append({"original": original, "highres": _to_highres(original)})
        return images
    raise Exception("Bol.com rate limit: te veel verzoeken, probeer later opnieuw")


def get_bol_images_raw(ean, bol_token):
    """Retourneert de volledige raw API response voor debug doeleinden (met usage=IMAGE)."""
    headers = {
        "Authorization": f"Bearer {bol_token}",
        "Accept": "application/vnd.retailer.v10+json",
    }
    resp = requests.get(
        f"https://api.bol.com/retailer/products/{ean}/assets",
        headers=headers,
        params={"usage": "ADDITIONAL"},
        timeout=15,
    )
    return resp.status_code, resp.json() if resp.headers.get("content-type", "").startswith("application/") else resp.text


# ── OAuth routes ──────────────────────────────────────────────────────────────

@app.route("/auth/login", methods=["POST"])
def auth_login():
    """Stap 1: genereer OAuth URL en stuur die terug naar de frontend."""
    data       = request.json
    store_url  = data.get("store_url", "").strip().rstrip("/").replace("https://", "").replace("http://", "")
    client_id  = data.get("client_id", "").strip() or SHOPIFY_CLIENT_ID
    client_secret = data.get("client_secret", "").strip() or SHOPIFY_CLIENT_SECRET

    if not store_url:
        return jsonify({"error": "Vul je store URL in"}), 400
    if not client_id:
        return jsonify({"error": "Vul je Shopify Client ID in"}), 400
    if not client_secret:
        return jsonify({"error": "Vul je Shopify Client Secret in"}), 400

    state = secrets.token_hex(16)
    session["oauth_state"]     = state
    session["store_url"]       = store_url
    session["client_id"]       = client_id
    session["client_secret"]   = client_secret

    redirect_uri = request.host_url.rstrip("/") + "/auth/callback"

    auth_url = (
        f"https://{store_url}/admin/oauth/authorize"
        f"?client_id={client_id}"
        f"&scope={SHOPIFY_SCOPES}"
        f"&redirect_uri={redirect_uri}"
        f"&state={state}"
    )
    return jsonify({"auth_url": auth_url})


@app.route("/auth/callback")
def auth_callback():
    """Stap 2: Shopify redirect — wissel code in voor access token."""
    code  = request.args.get("code", "")
    state = request.args.get("state", "")
    shop  = request.args.get("shop", "")

    if state != session.get("oauth_state"):
        return render_template("error.html", message="Ongeldige OAuth state. Probeer opnieuw."), 400

    store_url     = session.get("store_url") or shop
    client_id     = session.get("client_id")     or SHOPIFY_CLIENT_ID
    client_secret = session.get("client_secret") or SHOPIFY_CLIENT_SECRET

    try:
        resp = requests.post(
            f"https://{store_url}/admin/oauth/access_token",
            json={"client_id": client_id, "client_secret": client_secret, "code": code},
            timeout=15,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
    except Exception as e:
        return render_template("error.html", message=f"Token ophalen mislukt: {e}"), 500

    session["shopify_token"] = token
    session["store_url"]     = store_url
    session.pop("oauth_state", None)

    return redirect("/")


@app.route("/auth/status")
def auth_status():
    """Geeft terug of er een actieve Shopify sessie is."""
    token     = session.get("shopify_token")
    store_url = session.get("store_url", "")
    return jsonify({"connected": bool(token), "store_url": store_url})


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    session.pop("shopify_token", None)
    session.pop("store_url", None)
    return jsonify({"ok": True})


# ── Pagina routes ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    defaults = {
        "bol_client_id":     os.getenv("BOL_CLIENT_ID", ""),
        "bol_client_secret": os.getenv("BOL_CLIENT_SECRET", ""),
    }
    return render_template("index.html", defaults=defaults)


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/test-credentials", methods=["POST"])
def test_credentials():
    token     = session.get("shopify_token")
    store_url = session.get("store_url", "")
    data      = request.json or {}
    bol_id     = data.get("bol_client_id", "").strip()
    bol_secret = data.get("bol_client_secret", "").strip()
    results    = {}

    if token and store_url:
        try:
            resp = requests.get(
                f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/shop.json",
                headers=shopify_headers(token), timeout=10,
            )
            resp.raise_for_status()
            results["shopify"] = {"ok": True, "name": resp.json().get("shop", {}).get("name", store_url)}
        except Exception as e:
            results["shopify"] = {"ok": False, "error": str(e)}
    else:
        results["shopify"] = {"ok": False, "error": "Nog niet verbonden met Shopify"}

    if bol_id and bol_secret:
        try:
            get_bol_token(bol_id, bol_secret)
            results["bol"] = {"ok": True}
        except Exception as e:
            results["bol"] = {"ok": False, "error": str(e)}
    else:
        results["bol"] = {"ok": False, "error": "Vul Bol.com credentials in"}

    return jsonify(results)


@app.route("/api/bol-token", methods=["POST"])
def api_bol_token():
    data       = request.json or {}
    bol_id     = data.get("bol_client_id", "").strip()
    bol_secret = data.get("bol_client_secret", "").strip()
    if not bol_id or not bol_secret:
        return jsonify({"error": "Ontbrekende Bol.com credentials"}), 400
    try:
        token, expires_in = get_bol_token(bol_id, bol_secret)
        return jsonify({"token": token, "expires_in": expires_in})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/products", methods=["POST"])
def api_products():
    """Haal één pagina producten op. Stuur next_page_url mee voor volgende aanroep."""
    token     = session.get("shopify_token")
    store_url = session.get("store_url", "")
    if not token or not store_url:
        return jsonify({"error": "Niet verbonden met Shopify"}), 401

    data      = request.json or {}
    test_mode = data.get("test_mode", False)
    page_url  = data.get("page_url", None)   # None = eerste pagina

    try:
        raw, next_url = get_shopify_products_page(store_url, token, page_url)
    except Exception as e:
        return jsonify({"error": str(e), "detail": "Shopify products fetch mislukt"}), 500

    products = []
    for p in raw:
        ean = None
        for v in p.get("variants", []):
            bc = (v.get("barcode") or "").strip()
            if bc:
                ean = bc
                break
        img = p.get("image")
        products.append({
            "id":    p["id"],
            "title": p.get("title", ""),
            "ean":   ean,
            "thumb": img["src"] if img else None,
        })

    # In test-modus: geen paginering nodig, gewoon eerste 3
    if test_mode:
        products = products[:3]
        next_url = None

    return jsonify({"products": products, "next_page_url": next_url})


@app.route("/api/sync-product", methods=["POST"])
def api_sync_product():
    token     = session.get("shopify_token")
    store_url = session.get("store_url", "")
    if not token or not store_url:
        return jsonify({"error": "Niet verbonden met Shopify"}), 401

    data          = request.json
    bol_token     = data.get("bol_token", "").strip()
    product_id    = data.get("product_id")
    ean           = data.get("ean", "").strip()
    dry_run       = data.get("dry_run", False)
    product_index = int(data.get("product_index", 0))

    if not ean:
        return jsonify({"status": "no_ean", "added": 0, "skipped": 0})

    # Elke 50 producten een langere rustpauze zodat de rate limit bucket kan herstellen
    if product_index > 0 and product_index % 50 == 0:
        time.sleep(20)

    time.sleep(3)  # 3 sec pauze per product — voorkomt Bol.com rate limit
    try:
        bol_images = get_bol_images(ean, bol_token)
    except Exception as e:
        if "BOL_TOKEN_EXPIRED" in str(e):
            return jsonify({"status": "token_expired", "added": 0, "skipped": 0, "message": "Bol.com token verlopen"}), 401
        return jsonify({"status": "error", "added": 0, "skipped": 0, "message": str(e)}), 500

    if not bol_images:
        return jsonify({"status": "no_bol", "added": 0, "skipped": 0, "total_bol": 0})

    try:
        existing = get_existing_image_srcs(store_url, token, product_id)
    except Exception as e:
        return jsonify({"status": "error", "added": 0, "skipped": 0, "message": str(e)}), 500

    added   = 0
    skipped = 0
    # Start altijd op positie 2 — positie 1 is de hoofdafbeelding, die nooit aanraken
    next_position = max(len(existing) + 1, 2)
    errors  = []

    for img in bol_images:
        highres  = img["highres"]
        original = img["original"]
        # Gebruik originele URL voor duplicate-check (bestandsnaam is altijd hetzelfde)
        img_filename = original.split("/")[-1].split("?")[0]
        if any(img_filename in ex for ex in existing):
            skipped += 1
            continue
        if not dry_run:
            try:
                if add_image_to_shopify(store_url, token, product_id, highres, next_position, fallback_url=original):
                    added += 1
                    next_position += 1
                time.sleep(0.3)
            except Exception as e:
                errors.append(str(e))
        else:
            added += 1

    if errors and added == 0 and skipped == 0:
        return jsonify({"status": "error", "added": 0, "skipped": 0, "message": errors[0]})

    status = "success" if added > 0 else "unchanged"
    return jsonify({
        "status":    status,
        "added":     added,
        "skipped":   skipped,
        "total_bol": len(bol_images),
        "errors":    errors,
    })


# ── Server-side sync endpoints ────────────────────────────────────────────────

@app.route("/api/sync/start", methods=["POST"])
def api_sync_start():
    store_url, shopify_token = _auth_shopify()
    if not store_url or not shopify_token:
        return jsonify({"error": "Niet verbonden met Shopify"}), 401

    data       = request.json or {}
    bol_id     = data.get("bol_client_id", "").strip()
    bol_secret = data.get("bol_client_secret", "").strip()
    dry_run    = data.get("dry_run", False)
    test_mode  = data.get("test_mode", False)

    if not bol_id or not bol_secret:
        return jsonify({"error": "Bol.com Client ID en Secret zijn verplicht"}), 400

    # Als er al een actieve sync is, geef het job_id terug zodat de browser er aan kan koppelen
    for jid, job in _sync_jobs.items():
        if job.get("status") in ("loading", "running"):
            return jsonify({"job_id": jid, "status": "already_running", "message": "Sync was al actief — herverbonden"}), 200

    job_id = str(uuid.uuid4())[:8]
    _sync_jobs[job_id] = {
        "status":          "loading",
        "phase":           "loading",
        "progress":        0,
        "total":           0,
        "current_product": "",
        "stats":           {"added": 0, "skipped": 0, "nobol": 0, "errors": 0},
        "logs":            [],
        "results":         {},
        "products":        [],
        "nobol_list":      [],
        "started_at":      datetime.utcnow().isoformat(),
        "finished_at":     None,
        "dry_run":         dry_run,
        "test_mode":       test_mode,
        "store_url":       store_url,
    }

    threading.Thread(
        target=_sync_worker,
        args=(job_id, store_url, shopify_token, bol_id, bol_secret, dry_run, test_mode),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_id, "status": "started"})


@app.route("/api/sync/status")
def api_sync_status_ep():
    job_id = request.args.get("job_id", "")

    # Geen job_id meegegeven → meest recente job teruggeven
    if not job_id or job_id not in _sync_jobs:
        if not _sync_jobs:
            return jsonify({"status": "idle"})
        job_id = max(_sync_jobs, key=lambda j: _sync_jobs[j].get("started_at", ""))

    job         = _sync_jobs[job_id]
    since_log   = int(request.args.get("since_log", 0))
    since_res   = int(request.args.get("since_result", 0))
    result_list = list(job["results"].items())

    return jsonify({
        "job_id":          job_id,
        "status":          job["status"],
        "phase":           job.get("phase", ""),
        "progress":        job["progress"],
        "total":           job["total"],
        "current_product": job.get("current_product", ""),
        "stats":           job["stats"],
        "new_logs":        job["logs"][since_log:],
        "new_results":     dict(result_list[since_res:]),
        "log_cursor":      len(job["logs"]),
        "result_cursor":   len(result_list),
        "started_at":      job.get("started_at"),
        "finished_at":     job.get("finished_at"),
        "dry_run":         job.get("dry_run", False),
        "error":           job.get("error"),
        "products_count":  len(job.get("products", [])),
        "nobol_count":     len(job.get("nobol_list", [])),
    })


@app.route("/api/sync/products")
def api_sync_products_ep():
    job_id = request.args.get("job_id", "")
    if not job_id or job_id not in _sync_jobs:
        return jsonify({"products": []})
    return jsonify({"products": _sync_jobs[job_id].get("products", [])})


@app.route("/api/sync/stop", methods=["POST"])
def api_sync_stop():
    data   = request.json or {}
    job_id = data.get("job_id", "")
    if job_id in _sync_jobs:
        _sync_jobs[job_id]["stop_requested"] = True
        return jsonify({"ok": True})
    return jsonify({"error": "Job niet gevonden"}), 404


@app.route("/api/sync/export-nobol")
def api_sync_export_nobol():
    job_id = request.args.get("job_id", "")
    if not job_id or job_id not in _sync_jobs:
        return jsonify({"error": "Job niet gevonden"}), 404
    nobol = _sync_jobs[job_id].get("nobol_list", [])
    lines = ["Titel,EAN,Reden"]
    for item in nobol:
        title = item["title"].replace('"', '""')
        lines.append(f'"{title}","{item["ean"]}","{item["reden"]}"')
    return Response(
        "\n".join(lines),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=niet-op-bol.csv"},
    )


@app.route("/api/debug-ean", methods=["POST"])
def api_debug_ean():
    """Toont de volledige raw Bol.com API response voor een EAN — handig om te checken hoeveel assets er zijn."""
    token     = session.get("shopify_token")
    store_url = session.get("store_url", "")
    if not token or not store_url:
        return jsonify({"error": "Niet verbonden met Shopify"}), 401

    data       = request.json or {}
    ean        = data.get("ean", "").strip()
    bol_token  = data.get("bol_token", "").strip()

    if not ean:
        return jsonify({"error": "Vul een EAN in"}), 400
    if not bol_token:
        return jsonify({"error": "Geen Bol.com token meegegeven"}), 400

    try:
        status_code, raw = get_bol_images_raw(ean, bol_token)
        parsed = get_bol_images(ean, bol_token) if status_code == 200 else []
        return jsonify({
            "ean": ean,
            "http_status": status_code,
            "raw_response": raw,
            "parsed_image_urls": [{"original": i["original"], "highres": i["highres"]} for i in parsed],
            "image_count": len(parsed),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/test-ean", methods=["POST"])
def api_test_ean():
    """
    Test de sync voor één specifieke EAN:
    1. Haal Bol.com afbeeldingen op
    2. Zoek het product in Shopify op via EAN (barcode)
    3. Maak optioneel een nieuw testproduct aan als er geen match is
    4. Synchroniseer de afbeeldingen (tenzij dry_run)
    5. Geef een uitgebreid resultaat terug incl. thumbnails
    """
    token     = session.get("shopify_token")
    store_url = session.get("store_url", "")
    if not token or not store_url:
        return jsonify({"error": "Niet verbonden met Shopify"}), 401

    data        = request.json or {}
    ean         = data.get("ean", "").strip()
    bol_token   = data.get("bol_token", "").strip()
    dry_run     = data.get("dry_run", False)
    create_test = data.get("create_test_product", False)

    if not ean:
        return jsonify({"error": "Vul een EAN-code in"}), 400
    if not bol_token:
        return jsonify({"error": "Geen Bol.com token — vul Bol.com credentials in en test de verbinding"}), 400

    # ── Stap 1: Bol.com afbeeldingen ophalen ──────────────────────────────────
    try:
        bol_images = get_bol_images(ean, bol_token)
    except Exception as e:
        return jsonify({"error": f"Bol.com fout: {e}"}), 500

    if not bol_images:
        return jsonify({
            "status": "not_found_on_bol",
            "message": f"Geen afbeeldingen gevonden op Bol.com voor EAN {ean}. "
                       "Controleer of dit product op Bol.com staat.",
        })

    # ── Stap 2: Zoek product in Shopify via EAN (barcode) ────────────────────
    shopify_product = None
    search_url = f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/products.json"
    try:
        # Shopify heeft geen directe EAN-zoekfunctie; we zoeken via barcode-filter
        resp = requests.get(
            search_url,
            headers=shopify_headers(token),
            params={"limit": 250, "fields": "id,title,variants,images,image"},
            timeout=15,
        )
        resp.raise_for_status()
        for p in resp.json().get("products", []):
            for v in p.get("variants", []):
                if (v.get("barcode") or "").strip() == ean:
                    shopify_product = p
                    break
            if shopify_product:
                break

        # Doorzoek ook extra pagina's als niet gevonden
        link = resp.headers.get("Link", "")
        while not shopify_product and 'rel="next"' in link:
            next_url = None
            for part in link.split(","):
                if 'rel="next"' in part:
                    next_url = part.split(";")[0].strip().strip("<>")
                    break
            if not next_url:
                break
            resp = requests.get(next_url, headers=shopify_headers(token), timeout=15)
            resp.raise_for_status()
            for p in resp.json().get("products", []):
                for v in p.get("variants", []):
                    if (v.get("barcode") or "").strip() == ean:
                        shopify_product = p
                        break
                if shopify_product:
                    break
            link = resp.headers.get("Link", "")
    except Exception as e:
        return jsonify({"error": f"Shopify zoekfout: {e}"}), 500

    # ── Stap 3: Testproduct aanmaken als gewenst en niet gevonden ─────────────
    created_product = False
    if not shopify_product and create_test:
        try:
            payload = {
                "product": {
                    "title": f"🧪 TEST TEST TEST — EAN {ean}",
                    "status": "draft",
                    "variants": [{"barcode": ean, "price": "0.00"}],
                    "images": [{"src": bol_images[0]["highres"]}],  # hoofdafbeelding = eerste Bol.com foto
                }
            }
            resp = requests.post(
                f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/products.json",
                headers=shopify_headers(token),
                json=payload,
                timeout=15,
            )
            resp.raise_for_status()
            shopify_product = resp.json().get("product")
            created_product = True
            time.sleep(0.5)
        except Exception as e:
            return jsonify({"error": f"Testproduct aanmaken mislukt: {e}"}), 500

    if not shopify_product:
        return jsonify({
            "status": "not_in_shopify",
            "bol_images": [i["original"] for i in bol_images],
            "bol_image_count": len(bol_images),
            "message": f"EAN {ean} niet gevonden in Shopify. Zet 'Testproduct aanmaken' aan om een nieuw product te maken.",
        })

    product_id    = shopify_product["id"]
    product_title = shopify_product.get("title", "")
    product_url   = f"https://{store_url}/admin/products/{product_id}"

    # ── Stap 4: Afbeeldingen vergelijken en syncen ────────────────────────────
    try:
        existing = get_existing_image_srcs(store_url, token, product_id)
    except Exception as e:
        return jsonify({"error": f"Shopify afbeeldingen ophalen mislukt: {e}"}), 500

    added   = 0
    skipped = 0
    failed  = []
    next_position = max(len(existing) + 1, 2)  # nooit positie 1 overschrijven

    for img in bol_images:
        highres  = img["highres"]
        original = img["original"]
        img_filename = original.split("/")[-1].split("?")[0]
        if any(img_filename in ex for ex in existing):
            skipped += 1
            continue
        if not dry_run:
            try:
                if add_image_to_shopify(store_url, token, product_id, highres, next_position, fallback_url=original):
                    added += 1
                    next_position += 1
                time.sleep(0.3)
            except Exception as e:
                failed.append({"url": highres, "error": str(e)})
        else:
            added += 1

    return jsonify({
        "status":           "success",
        "ean":              ean,
        "product_id":       product_id,
        "product_title":    product_title,
        "product_url":      product_url,
        "created_product":  created_product,
        "dry_run":          dry_run,
        "bol_image_count":  len(bol_images),
        "bol_images":       [i["original"] for i in bol_images],   # originele URLs voor preview
        "added":            added,
        "skipped":          skipped,
        "already_present":  len(existing),
        "failed":           failed,
    })


# ── Snapshot routes ───────────────────────────────────────────────────────────

@app.route("/api/snapshot/create", methods=["POST"])
def snapshot_create():
    """Maak een volledige snapshot van alle huidige Shopify productafbeeldingen."""
    token     = session.get("shopify_token")
    store_url = session.get("store_url", "")
    if not token or not store_url:
        return jsonify({"error": "Niet verbonden met Shopify"}), 401

    products_snapshot = []
    page_url = None
    total    = 0

    while True:
        try:
            raw, next_url = get_shopify_products_page(store_url, token, page_url)
        except Exception as e:
            return jsonify({"error": f"Shopify fout: {e}"}), 500

        for p in raw:
            images = []
            for img in p.get("images", []):
                images.append({
                    "id":       img["id"],
                    "position": img.get("position", 1),
                    "src":      img["src"].split("?")[0],   # zonder resize params
                })
            products_snapshot.append({
                "id":     p["id"],
                "title":  p.get("title", ""),
                "images": images,
            })
            total += 1

        if not next_url:
            break
        page_url = next_url

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename  = f"snapshot_{timestamp}.json"
    filepath  = SNAPSHOTS_DIR / filename

    data = {
        "created":    datetime.now().isoformat(),
        "store_url":  store_url,
        "total":      total,
        "products":   products_snapshot,
    }
    content_str = _json_module.dumps(data, ensure_ascii=False, indent=2)
    filepath.write_text(content_str)

    # Push naar private GitHub repo als backup
    github_ok, github_info = _github_push_snapshot(filename, content_str)

    return jsonify({
        "ok":         True,
        "filename":   filename,
        "total":      total,
        "created":    data["created"],
        "github_ok":  github_ok,
        "github_url": github_info if github_ok else None,
        "github_err": github_info if not github_ok else None,
    })


@app.route("/api/snapshot/list", methods=["GET"])
def snapshot_list():
    """Geeft een lijst van beschikbare snapshots terug (lokaal + GitHub)."""
    snapshots = {}

    # Lokale snapshots
    for f in sorted(SNAPSHOTS_DIR.glob("snapshot_*.json"), reverse=True):
        try:
            meta = _json_module.loads(f.read_text())
            snapshots[f.name] = {
                "filename":  f.name,
                "created":   meta.get("created", ""),
                "total":     meta.get("total", 0),
                "store_url": meta.get("store_url", ""),
                "local":     True,
                "github":    False,
            }
        except Exception:
            pass

    # GitHub snapshots (vul aan wat lokaal mist)
    for name in _github_list_snapshots():
        if name in snapshots:
            snapshots[name]["github"] = True
        else:
            # Alleen in GitHub, niet lokaal — haal metadata op
            try:
                data = _github_download_snapshot(name)
                if data:
                    snapshots[name] = {
                        "filename":  name,
                        "created":   data.get("created", ""),
                        "total":     data.get("total", 0),
                        "store_url": data.get("store_url", ""),
                        "local":     False,
                        "github":    True,
                    }
            except Exception:
                pass

    result = sorted(snapshots.values(), key=lambda s: s["created"], reverse=True)
    return jsonify({"snapshots": result})


@app.route("/api/snapshot/restore", methods=["POST"])
def snapshot_restore():
    """
    Herstel één product vanuit een snapshot.
    Verwijdert afbeeldingen die er ná de snapshot bij zijn gekomen,
    voegt afbeeldingen terug die zijn verdwenen.
    Wordt per product aangeroepen (zelfde patroon als sync-product).
    """
    token     = session.get("shopify_token")
    store_url = session.get("store_url", "")
    if not token or not store_url:
        return jsonify({"error": "Niet verbonden met Shopify"}), 401

    data       = request.json or {}
    filename   = data.get("filename", "").strip()
    product_id = data.get("product_id")
    dry_run    = data.get("dry_run", False)

    if not filename or not product_id:
        return jsonify({"error": "Ontbrekende parameters"}), 400

    filepath = SNAPSHOTS_DIR / filename

    # Probeer lokaal, anders van GitHub downloaden
    if filepath.exists():
        snapshot = _json_module.loads(filepath.read_text())
    else:
        snapshot = _github_download_snapshot(filename)
        if snapshot is None:
            return jsonify({"error": "Snapshot niet gevonden (lokaal noch GitHub)"}), 404
        # Sla lokaal op voor snellere volgende aanroepen
        filepath.write_text(_json_module.dumps(snapshot, ensure_ascii=False, indent=2))
    snap_product = next((p for p in snapshot["products"] if p["id"] == product_id), None)
    if snap_product is None:
        return jsonify({"status": "not_in_snapshot"})

    snap_srcs = {img["src"].split("?")[0] for img in snap_product["images"]}

    # Huidige afbeeldingen in Shopify
    url = f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id}/images.json"
    try:
        resp = requests.get(url, headers=shopify_headers(token), timeout=15)
        resp.raise_for_status()
        current_images = resp.json().get("images", [])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    current_srcs = {img["src"].split("?")[0]: img["id"] for img in current_images}

    removed = 0
    restored = 0
    errors = []

    if not dry_run:
        # Verwijder afbeeldingen die er ná de snapshot bijgekomen zijn
        for src, img_id in current_srcs.items():
            if src not in snap_srcs:
                try:
                    del_url = f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id}/images/{img_id}.json"
                    requests.delete(del_url, headers=shopify_headers(token), timeout=15)
                    removed += 1
                    time.sleep(0.2)
                except Exception as e:
                    errors.append(str(e))

        # Voeg afbeeldingen terug die verdwenen zijn
        for img in snap_product["images"]:
            src = img["src"].split("?")[0]
            if src not in current_srcs:
                try:
                    add_image_to_shopify(store_url, token, product_id, src, img["position"])
                    restored += 1
                    time.sleep(0.2)
                except Exception as e:
                    errors.append(str(e))
    else:
        # Dry run: tel alleen
        removed  = sum(1 for s in current_srcs if s not in snap_srcs)
        restored = sum(1 for img in snap_product["images"] if img["src"].split("?")[0] not in current_srcs)

    status = "ok" if not errors else "partial"
    return jsonify({
        "status":   status,
        "removed":  removed,
        "restored": restored,
        "errors":   errors,
    })


if __name__ == "__main__":
    print("\n🚀 Ropi Shopify Tool draait op http://localhost:5000\n")
    app.run(debug=False, port=5000, threaded=True)
