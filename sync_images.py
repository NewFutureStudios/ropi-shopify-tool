"""
Ropi Shopify Tool - Bol.com naar Shopify afbeeldingen sync
----------------------------------------------------------
Haalt ontbrekende productafbeeldingen op van Bol.com via EAN
en plaatst ze terug in Shopify.

Gebruik:
  python sync_images.py            # verwerk alle producten
  python sync_images.py --test     # test met 1 product
  python sync_images.py --ean 1234567890123  # test specifieke EAN
"""

import os
import sys
import time
import argparse
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Credentials ──────────────────────────────────────────────────────────────

SHOPIFY_STORE_URL   = os.getenv("SHOPIFY_STORE_URL", "").rstrip("/")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
BOL_CLIENT_ID       = os.getenv("BOL_CLIENT_ID", "")
BOL_CLIENT_SECRET   = os.getenv("BOL_CLIENT_SECRET", "")

SHOPIFY_API_VERSION = "2024-01"

# ── Shopify helpers ───────────────────────────────────────────────────────────

def shopify_headers():
    return {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }


def get_all_shopify_products():
    """Haalt alle producten op uit Shopify (paginated)."""
    products = []
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}/products.json"
    params = {"limit": 250, "fields": "id,title,variants,images"}

    while url:
        resp = requests.get(url, headers=shopify_headers(), params=params)
        resp.raise_for_status()
        data = resp.json()
        products.extend(data.get("products", []))

        # Shopify paginering via Link header
        link = resp.headers.get("Link", "")
        url = None
        params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    break

        time.sleep(0.5)  # Shopify rate limit: max 2 req/sec

    return products


def get_existing_image_srcs(product_id):
    """Geeft een set van bestaande afbeeldings-URLs voor een product."""
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id}/images.json"
    resp = requests.get(url, headers=shopify_headers())
    resp.raise_for_status()
    images = resp.json().get("images", [])
    return {img["src"].split("?")[0] for img in images}  # strip query params


def add_image_to_shopify(product_id, image_url, position):
    """Voegt één afbeelding toe aan een Shopify product."""
    url = f"https://{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}/products/{product_id}/images.json"
    payload = {"image": {"src": image_url, "position": position}}
    resp = requests.post(url, headers=shopify_headers(), json=payload)
    if resp.status_code == 422:
        print(f"      ⚠️  Overgeslagen (422): {image_url}")
        return False
    resp.raise_for_status()
    return True


# ── Bol.com helpers ───────────────────────────────────────────────────────────

_bol_token = None
_bol_token_expiry = 0


def get_bol_token():
    """Haalt een OAuth2 access token op van Bol.com (cached)."""
    global _bol_token, _bol_token_expiry
    if _bol_token and time.time() < _bol_token_expiry - 60:
        return _bol_token

    resp = requests.post(
        "https://login.bol.com/token",
        params={"grant_type": "client_credentials"},
        auth=(BOL_CLIENT_ID, BOL_CLIENT_SECRET),
    )
    resp.raise_for_status()
    data = resp.json()
    _bol_token = data["access_token"]
    _bol_token_expiry = time.time() + data.get("expires_in", 3600)
    return _bol_token


def bol_headers():
    return {
        "Authorization": f"Bearer {get_bol_token()}",
        "Accept": "application/vnd.retailer.v10+json",
    }


def get_bol_images(ean):
    """
    Haalt alle afbeeldings-URLs op voor een EAN via de Bol.com Retailer API.
    Geeft een lijst van URL-strings terug, of een lege lijst als er niets gevonden is.
    """
    url = f"https://api.bol.com/retailer/products/{ean}"
    resp = requests.get(url, headers=bol_headers())

    if resp.status_code == 404:
        return []
    if resp.status_code == 429:
        print("      ⏳ Bol.com rate limit, wacht 10 sec...")
        time.sleep(10)
        return get_bol_images(ean)

    resp.raise_for_status()
    data = resp.json()

    # Bol.com product response bevat 'assets' met type IMAGE
    images = []
    for asset in data.get("assets", []):
        if asset.get("type") == "IMAGE":
            # Kies de grootste beschikbare variant
            variants = asset.get("variants", [])
            # Sorteer op breedte (grootste eerst)
            variants_sorted = sorted(variants, key=lambda v: v.get("width", 0), reverse=True)
            if variants_sorted:
                images.append(variants_sorted[0]["url"])

    return images


# ── Core sync logica ──────────────────────────────────────────────────────────

def extract_ean(product):
    """Haalt de EAN/barcode op uit het eerste variant van een Shopify product."""
    for variant in product.get("variants", []):
        barcode = variant.get("barcode", "").strip()
        if barcode:
            return barcode
    return None


def sync_product(product, dry_run=False):
    """Synchroniseert afbeeldingen voor één product. Geeft (toegevoegd, overgeslagen) terug."""
    ean = extract_ean(product)
    product_id = product["id"]
    title = product.get("title", "Onbekend")

    if not ean:
        print(f"  ⏭️  [{product_id}] '{title}' — geen EAN, overgeslagen")
        return 0, 0

    print(f"  🔍 [{product_id}] '{title}' | EAN: {ean}")

    # Haal bol.com afbeeldingen op
    bol_images = get_bol_images(ean)
    if not bol_images:
        print(f"      ❌ Geen afbeeldingen gevonden op Bol.com")
        return 0, 0

    print(f"      📦 {len(bol_images)} afbeelding(en) gevonden op Bol.com")

    # Haal bestaande Shopify afbeeldingen op (voor duplicate check)
    existing = get_existing_image_srcs(product_id)
    current_count = len(existing)

    added = 0
    skipped = 0

    for img_url in bol_images:
        # Duplicate check op bestandsnaam (laatste deel van URL)
        img_filename = img_url.split("/")[-1].split("?")[0]
        already_exists = any(img_filename in ex for ex in existing)

        if already_exists:
            print(f"      ✅ Al aanwezig: {img_filename}")
            skipped += 1
            continue

        if dry_run:
            print(f"      [DRY RUN] Zou toevoegen: {img_url}")
            added += 1
        else:
            position = current_count + added + 2  # +2 want positie 1 is de hoofdafbeelding
            success = add_image_to_shopify(product_id, img_url, position)
            if success:
                print(f"      ➕ Toegevoegd: {img_filename}")
                added += 1
            time.sleep(0.5)

    return added, skipped


# ── Entrypoint ────────────────────────────────────────────────────────────────

def validate_credentials():
    missing = []
    if not SHOPIFY_STORE_URL:
        missing.append("SHOPIFY_STORE_URL")
    if not SHOPIFY_ACCESS_TOKEN:
        missing.append("SHOPIFY_ACCESS_TOKEN")
    if not BOL_CLIENT_ID:
        missing.append("BOL_CLIENT_ID")
    if not BOL_CLIENT_SECRET:
        missing.append("BOL_CLIENT_SECRET")
    if missing:
        print(f"❌ Ontbrekende credentials in .env: {', '.join(missing)}")
        print("   Kopieer .env.example naar .env en vul je gegevens in.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Sync Bol.com afbeeldingen naar Shopify")
    parser.add_argument("--test", action="store_true", help="Verwerk alleen het eerste product")
    parser.add_argument("--ean", type=str, help="Test met een specifieke EAN")
    parser.add_argument("--dry-run", action="store_true", help="Simuleer zonder iets te uploaden")
    args = parser.parse_args()

    validate_credentials()

    print("=" * 60)
    print("  Ropi Shopify ↔ Bol.com Afbeeldingen Sync")
    print("=" * 60)

    if args.dry_run:
        print("  ⚠️  DRY RUN MODE — er wordt niets daadwerkelijk geüpload")
        print()

    # Test modus: directe EAN invoer
    if args.ean:
        print(f"  Test modus: EAN {args.ean}")
        images = get_bol_images(args.ean)
        if images:
            print(f"  ✅ {len(images)} afbeelding(en) gevonden:")
            for img in images:
                print(f"     {img}")
        else:
            print("  ❌ Geen afbeeldingen gevonden voor deze EAN")
        return

    # Haal alle Shopify producten op
    print("\n📥 Shopify producten ophalen...")
    products = get_all_shopify_products()
    print(f"   {len(products)} producten gevonden\n")

    if args.test:
        products = products[:1]
        print(f"  ⚡ Test modus: alleen het eerste product wordt verwerkt\n")

    total_added = 0
    total_skipped = 0
    total_no_ean = 0

    for i, product in enumerate(products, 1):
        print(f"[{i}/{len(products)}]", end=" ")
        added, skipped = sync_product(product, dry_run=args.dry_run)
        total_added += added
        total_skipped += skipped
        if not extract_ean(product):
            total_no_ean += 1
        time.sleep(0.3)

    print("\n" + "=" * 60)
    print("  Samenvatting")
    print("=" * 60)
    print(f"  ➕ Afbeeldingen toegevoegd : {total_added}")
    print(f"  ✅ Al aanwezig (overgeslagen): {total_skipped}")
    print(f"  ⏭️  Producten zonder EAN    : {total_no_ean}")
    print("=" * 60)


if __name__ == "__main__":
    main()
