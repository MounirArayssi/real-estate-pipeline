import os
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "real_estate"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
}

def run_transforms():
    """Add computed columns and create analytical views."""

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # - 1. Add price_per_sqft column if not exists ---
    logger.info("Adding price_per_sqft column...")
    cur.execute("""
        ALTER TABLE listings
        ADD COLUMN IF NOT EXISTS price_per_sqft NUMERIC(10,2);
    """)

    cur.execute("""
        UPDATE listings
        SET price_per_sqft = ROUND(price::numeric / NULLIF(sqft, 0), 2)
        WHERE sqft IS NOT NULL AND sqft > 0;
    """)
    logger.info(f"Updated {cur.rowcount} rows with price_per_sqft")

    # ── 2. Create zip code summary view ─────────────────────
    logger.info("Creating zip_summary view...")
    cur.execute("DROP VIEW IF EXISTS zip_summary;")
    cur.execute("""
        CREATE VIEW zip_summary AS
        SELECT
            zip,
            city,
            COUNT(*) as total_listings,
            AVG(price)::int as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)::int as median_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(price_per_sqft)::numeric(10,2) as avg_price_per_sqft,
            AVG(sqft)::int as avg_sqft,
            ROUND(AVG(beds), 1) as avg_beds,
            ROUND(AVG(baths), 1) as avg_baths
        FROM listings
        WHERE price IS NOT NULL
        GROUP BY zip, city
        ORDER BY avg_price DESC;
    """)

    # ── 3. Create property type breakdown view ──────────────
    logger.info("Creating property_type_stats view...")
    cur.execute("DROP VIEW IF EXISTS property_type_stats;")
    cur.execute("""
        CREATE VIEW property_type_stats AS
        SELECT
            property_type,
            property_subtype,
            COUNT(*) as count,
            AVG(price)::int as avg_price,
            AVG(sqft)::int as avg_sqft,
            AVG(price_per_sqft)::numeric(10,2) as avg_price_per_sqft
        FROM listings
        WHERE price IS NOT NULL
        GROUP BY property_type, property_subtype
        ORDER BY count DESC;
    """)

    # ── 4. Create price range distribution view ─────────────
    logger.info("Creating price_distribution view...")
    cur.execute("DROP VIEW IF EXISTS price_distribution;")
    cur.execute("""
        CREATE VIEW price_distribution AS
        SELECT
            CASE
                WHEN price < 500000 THEN 'Under 500K'
                WHEN price < 1000000 THEN '500K - 1M'
                WHEN price < 2000000 THEN '1M - 2M'
                WHEN price < 5000000 THEN '2M - 5M'
                ELSE '5M+'
            END as price_range,
            COUNT(*) as count,
            AVG(sqft)::int as avg_sqft,
            AVG(beds)::numeric(3,1) as avg_beds
        FROM listings
        WHERE price IS NOT NULL
        GROUP BY price_range
        ORDER BY MIN(price);
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info("[OK] All transforms complete")


if __name__ == "__main__":
    run_transforms()