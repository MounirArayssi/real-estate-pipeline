import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

# --- API Config -------
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = "realty-in-us.p.rapidapi.com"
API_URL = f"https://{RAPIDAPI_HOST}/properties/v3/list"

HEADERS = {
    "Content-Type": "application/json",
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key" : RAPIDAPI_KEY,
}

# --- DB Config ------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "real_estate"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
}

# --- Target Zip codes ------
ZIP_CODES = ["90004", "90012", "90015", "90028", "90036"]


def fetch_listings(postal_code, status=None, limit=15):
    """Fetch listings from API for a given zip code"""

    if status is None:
        status = ["for_sale"]

    payload = {
        "limit": limit,
        "offset": 0,
        "postal_code":postal_code,
        "status" : status,
        "sort": {"direction": "desc", "field": "list_date"},

    }

    print(f"    Fetching {postal_code} (limit={limit}...")
    response = requests.post(API_URL, json=payload, headers=HEADERS, timeout=30)
    response.raise_for_status()

    data = response.json()

    results = data.get("data", {}).get("home_search", {}).get("results", [])
    total = data.get("data", {}).get("home_search", {}).get("total", 0)

    print(f"    Got {len(results)}cof {total} total listings")

    return results

def parse_listing(raw):
    """Extract fields from a single API result into a flat dict."""
    location = raw.get("location", {})
    address = location.get("address", {})
    coord = address.get("coordinate", {}) or {}
    desc = raw.get("description", {}) or {}
    flags = raw.get("flags", {}) or {}

    # Parse list_date safely
    list_date_raw = raw.get("list_date")
    list_date = None
    if list_date_raw:
        try:
            list_date = datetime.fromisoformat(
                list_date_raw.replace("Z", "+00:00")
            ).date()
        except (ValueError, TypeError):
            list_date = None

    return {
        "property_id": raw.get("property_id"),
        "listing_id": raw.get("listing_id"),
        "status": raw.get("status"),
        "address": address.get("line"),
        "city": address.get("city"),
        "state": address.get("state_code"),
        "zip": address.get("postal_code"),
        "price": raw.get("list_price"),
        "beds": desc.get("beds"),
        "baths": desc.get("baths"),
        "sqft": desc.get("sqft"),
        "lot_sqft": desc.get("lot_sqft"),
        "property_type": desc.get("type"),
        "property_subtype": desc.get("sub_type"),
        "list_date": list_date,
        "lat": coord.get("lat"),
        "lon": coord.get("lon"),
        "photo_count": raw.get("photo_count"),
        "is_new_listing": flags.get("is_new_listing"),
        "is_foreclosure": flags.get("is_foreclosure"),
        "is_price_reduced": flags.get("is_price_reduced"),
        "price_reduced_amount": raw.get("price_reduced_amount"),
        "last_sold_price": raw.get("last_sold_price"),
        "last_sold_date": raw.get("last_sold_date"),
        "photo_url": (raw.get("primary_photo") or {}).get("href"),
        "detail_url": raw.get("href"),
    }


def load_to_db(listings):
    """Insert parsed listings into PostgreSQL using upsert."""
    if not listings:
        print("  No listings to load.")
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    sql = """
        INSERT INTO listings (
            property_id, listing_id, status, address, city, state, zip,
            price, beds, baths, sqft, lot_sqft,
            property_type, property_subtype, list_date,
            lat, lon, photo_count,
            is_new_listing, is_foreclosure, is_price_reduced,
            price_reduced_amount, last_sold_price, last_sold_date,
            photo_url, detail_url
        ) VALUES %s
        ON CONFLICT (property_id) DO UPDATE SET
            price = EXCLUDED.price,
            status = EXCLUDED.status,
            is_price_reduced = EXCLUDED.is_price_reduced,
            price_reduced_amount = EXCLUDED.price_reduced_amount,
            photo_count = EXCLUDED.photo_count,
            scraped_at = CURRENT_TIMESTAMP
    """

    values = [
        (
            l["property_id"], l["listing_id"], l["status"],
            l["address"], l["city"], l["state"], l["zip"],
            l["price"], l["beds"], l["baths"], l["sqft"], l["lot_sqft"],
            l["property_type"], l["property_subtype"], l["list_date"],
            l["lat"], l["lon"], l["photo_count"],
            l["is_new_listing"], l["is_foreclosure"], l["is_price_reduced"],
            l["price_reduced_amount"], l["last_sold_price"], l["last_sold_date"],
            l["photo_url"], l["detail_url"],
        )
        for l in listings
    ]

    execute_values(cur, sql, values)
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    return inserted


def run():
    """Main pipeline: fetch → parse → load for each zip code."""
    print(f"{'='*50}")
    print(f"Real Estate Pipeline Run - {datetime.now()}")
    print(f"{'='*50}")

    total_fetched = 0
    total_loaded = 0

    for zip_code in ZIP_CODES:
        try:
            raw_listings = fetch_listings(zip_code, limit=15)
            parsed = [parse_listing(r) for r in raw_listings]
            # Filter out any with missing property_id
            seen = set()
            unique = []
            for p in parsed:
                if p["property_id"] and p["property_id"] not in seen:
                    seen.add(p["property_id"])
                    unique.append(p)
            parsed = unique
            loaded = load_to_db(parsed)

            total_fetched += len(parsed)
            total_loaded += loaded
            print(f"  ✓ {zip_code}: {len(parsed)} fetched, {loaded} upserted\n")

        except requests.exceptions.RequestException as e:
            print(f"  ✗ {zip_code}: API error - {e}\n")
        except psycopg2.Error as e:
            print(f"  ✗ {zip_code}: DB error - {e}\n")

    print(f"{'='*50}")
    print(f"Done! {total_fetched} fetched, {total_loaded} upserted")
    print(f"{'='*50}")


if __name__ == "__main__":
    run()
