"""
Ropi Shopify Tool — Web Interface
Lokaal:  python app.py  →  http://localhost:5000
Vercel:  automatisch via vercel.json
"""

import os
import time
import secrets
import requests
from flask import Flask, render_template, request, jsonify, session, redirect
from dotenv import load_dotenv

load_dotenv()

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


def add_image_to_shopify(store_url, token, product_id, image_url, position):
    url = f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id}/images.json"
    payload = {"image": {"src": image_url, "position": position}}
    resp = requests.post(url, headers=shopify_headers(token), json=payload, timeout=15)
    if resp.status_code == 422:
        return False
    resp.raise_for_status()
    return True


# ── Bol.com helpers ───────────────────────────────────────────────────────────

import re
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
    """Zet een Bol.com afbeeldings-URL om naar de hoogste resolutie variant."""
    # Bol.com patroon: .../images/xxx/400x400/... of .../thumb/... → vervang door 1600x1600
    url = re.sub(r'/\d+x\d+/', '/1600x1600/', url)
    url = re.sub(r'_\d+x\d+\.', '_1600x1600.', url)
    # Verwijder Bol.com resize query params
    url = re.sub(r'\?.*$', '', url)
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
    # Retry bij 429 (rate limit) met exponential backoff
    for attempt in range(4):
        resp = requests.get(
            f"https://api.bol.com/retailer/products/{ean}/assets",
            headers=headers,
            params={"usage": "IMAGE"},   # ← geeft PRIMARY + alle ADDITIONAL terug
            timeout=15,
        )
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 10)) if attempt == 0 else (2 ** attempt) * 5
            time.sleep(wait)
            continue
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()

        # Sorteer op carousel-volgorde zodat de afbeeldingen in de juiste volgorde in Shopify komen
        assets = sorted(data.get("assets", []), key=lambda a: a.get("order", 999))

        images = []
        for asset in assets:
            # Sla niet-afbeeldingen over (video, document, 360-spin, etc.)
            usage = asset.get("usage", "IMAGE")
            if usage not in ("IMAGE", "PRIMARY", "ADDITIONAL", "image", "primary", "additional", ""):
                continue
            variants = sorted(asset.get("variants", []), key=lambda v: v.get("width", 0), reverse=True)
            if variants:
                images.append(variants[0]["url"])
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
        params={"usage": "IMAGE"},
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

    data       = request.json
    bol_token  = data.get("bol_token", "").strip()
    product_id = data.get("product_id")
    ean        = data.get("ean", "").strip()
    dry_run    = data.get("dry_run", False)

    if not ean:
        return jsonify({"status": "no_ean", "added": 0, "skipped": 0})

    time.sleep(1)  # 1 sec pauze per product — voorkomt Bol.com rate limit
    try:
        bol_images = get_bol_images(ean, bol_token)
    except Exception as e:
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

    for img_url in bol_images:
        img_filename = img_url.split("/")[-1].split("?")[0]
        if any(img_filename in ex for ex in existing):
            skipped += 1
            continue
        if not dry_run:
            try:
                if add_image_to_shopify(store_url, token, product_id, img_url, next_position):
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
            "parsed_image_urls": parsed,
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
                    "images": [{"src": bol_images[0]}],  # hoofdafbeelding = eerste Bol.com foto
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
            "bol_images": bol_images,
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

    for img_url in bol_images:
        img_filename = img_url.split("/")[-1].split("?")[0]
        if any(img_filename in ex for ex in existing):
            skipped += 1
            continue
        if not dry_run:
            try:
                if add_image_to_shopify(store_url, token, product_id, img_url, next_position):
                    added += 1
                    next_position += 1
                time.sleep(0.3)
            except Exception as e:
                failed.append({"url": img_url, "error": str(e)})
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
        "bol_images":       bol_images,        # alle Bol.com URLs (voor preview)
        "added":            added,
        "skipped":          skipped,
        "already_present":  len(existing),
        "failed":           failed,
    })


if __name__ == "__main__":
    print("\n🚀 Ropi Shopify Tool draait op http://localhost:5000\n")
    app.run(debug=False, port=5000, threaded=True)
