"""
Ropi Shopify Tool — Web Interface
Lokaal:  python app.py  →  http://localhost:5000
Vercel:  automatisch via vercel.json
"""

import os
import time
import requests
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

SHOPIFY_API_VERSION = "2026-04"

# ── Shopify ──────────────────────────────────────────────────────────────────

def shopify_headers(token):
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}


def get_all_shopify_products(store_url, token):
    products = []
    url = f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/products.json"
    params = {"limit": 250, "fields": "id,title,variants,images,image"}
    while url:
        resp = requests.get(url, headers=shopify_headers(token), params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        products.extend(data.get("products", []))
        link = resp.headers.get("Link", "")
        url = None
        params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    break
    return products


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


# ── Bol.com ──────────────────────────────────────────────────────────────────

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


def get_bol_images(ean, bol_token):
    headers = {
        "Authorization": f"Bearer {bol_token}",
        "Accept": "application/vnd.retailer.v10+json",
    }
    resp = requests.get(
        f"https://api.bol.com/retailer/products/{ean}",
        headers=headers,
        timeout=15,
    )
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    data = resp.json()
    images = []
    for asset in data.get("assets", []):
        if asset.get("type") == "IMAGE":
            variants = sorted(asset.get("variants", []), key=lambda v: v.get("width", 0), reverse=True)
            if variants:
                images.append(variants[0]["url"])
    return images


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    defaults = {
        "store_url":       os.getenv("SHOPIFY_STORE_URL", ""),
        "token":           os.getenv("SHOPIFY_ACCESS_TOKEN", ""),
        "bol_client_id":   os.getenv("BOL_CLIENT_ID", ""),
        "bol_client_secret": os.getenv("BOL_CLIENT_SECRET", ""),
    }
    return render_template("index.html", defaults=defaults)


@app.route("/api/test-credentials", methods=["POST"])
def test_credentials():
    data = request.json
    store_url  = data.get("store_url", "").strip().rstrip("/")
    token      = data.get("token", "").strip()
    bol_id     = data.get("bol_client_id", "").strip()
    bol_secret = data.get("bol_client_secret", "").strip()
    results = {}

    try:
        resp = requests.get(
            f"https://{store_url}/admin/api/{SHOPIFY_API_VERSION}/shop.json",
            headers=shopify_headers(token), timeout=10,
        )
        resp.raise_for_status()
        results["shopify"] = {"ok": True, "name": resp.json().get("shop", {}).get("name", store_url)}
    except Exception as e:
        results["shopify"] = {"ok": False, "error": str(e)}

    try:
        get_bol_token(bol_id, bol_secret)
        results["bol"] = {"ok": True}
    except Exception as e:
        results["bol"] = {"ok": False, "error": str(e)}

    return jsonify(results)


@app.route("/api/bol-token", methods=["POST"])
def api_bol_token():
    """Haalt eenmalig een Bol.com OAuth token op. De frontend cached dit voor de hele sync."""
    data       = request.json
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
    """Geeft alle Shopify producten terug als platte lijst."""
    data       = request.json
    store_url  = data.get("store_url", "").strip().rstrip("/")
    token      = data.get("token", "").strip()
    test_mode  = data.get("test_mode", False)

    if not store_url or not token:
        return jsonify({"error": "Ontbrekende Shopify credentials"}), 400

    try:
        raw = get_all_shopify_products(store_url, token)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    products = []
    for p in raw:
        ean = None
        for v in p.get("variants", []):
            bc = v.get("barcode", "").strip()
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

    if test_mode:
        products = products[:3]

    return jsonify({"products": products})


@app.route("/api/sync-product", methods=["POST"])
def api_sync_product():
    """Synchroniseert de afbeeldingen van één product. Stateless — credentials meegestuurd vanuit de browser."""
    data        = request.json
    store_url   = data.get("store_url", "").strip().rstrip("/")
    token       = data.get("token", "").strip()
    bol_token   = data.get("bol_token", "").strip()   # pre-fetched door de frontend
    product_id  = data.get("product_id")
    ean         = data.get("ean", "").strip()
    dry_run     = data.get("dry_run", False)

    if not ean:
        return jsonify({"status": "no_ean", "added": 0, "skipped": 0})

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
    current_count = len(existing)
    errors  = []

    for img_url in bol_images:
        img_filename = img_url.split("/")[-1].split("?")[0]
        if any(img_filename in ex for ex in existing):
            skipped += 1
            continue
        if not dry_run:
            try:
                position = current_count + added + 2
                if add_image_to_shopify(store_url, token, product_id, img_url, position):
                    added += 1
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


if __name__ == "__main__":
    print("\n🚀 Ropi Shopify Tool draait op http://localhost:5000\n")
    app.run(debug=False, port=5000, threaded=True)
