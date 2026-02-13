# Real Estate Data Pipeline

An automated data pipeline that extracts real estate listings from the Realty in US API (via RapidAPI), transforms the data with computed analytics, and loads it into PostgreSQL — with CI/CD via GitHub Actions.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Realty API   │────▶│  scraper.py  │────▶│  PostgreSQL   │────▶│ transforms.py│
│  (RapidAPI)  │     │  Extract &   │     │  Raw Storage  │     │  Views &     │
│              │     │  Load        │     │              │     │  Analytics   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                            │                                         │
                            ▼                                         ▼
                     ┌──────────────┐                          ┌──────────────┐
                     │ pipeline.log │                          │ zip_summary  │
                     │ Structured   │                          │ price_dist   │
                     │ Logging      │                          │ type_stats   │
                     └──────────────┘                          └──────────────┘
```

## Tech Stack

- **Python 3.12+** — Core pipeline logic
- **PostgreSQL 16+** — Data warehouse
- **Realty in US API** — Real estate listings data (via RapidAPI)
- **psycopg2** — PostgreSQL adapter with bulk upsert
- **GitHub Actions** — CI/CD with PostgreSQL service container
- **pytest** — Unit testing

## Features

- **Automated extraction** from real estate API across multiple zip codes
- **Upsert logic** — inserts new listings, updates changed prices/statuses on re-runs
- **Computed columns** — price per square foot derived from raw data
- **Analytical views** — zip code summaries, property type breakdowns, price distributions
- **Structured logging** — dual output to console and `pipeline.log`
- **Deduplication** — handles duplicate property IDs within API responses
- **Scheduling** — configured for daily automated runs via Windows Task Scheduler

## Project Structure

```
real-estate-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions CI pipeline
├── src/
│   ├── __init__.py
│   ├── scraper.py              # API extraction and PostgreSQL loading
│   └── transforms.py           # Computed columns and analytical views
├── test/
│   └── test_scraper.py         # Unit tests for data parsing
├── .env.example                # Environment variable template
├── .gitignore
├── requirements.txt
└── README.md
```

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 16+
- RapidAPI account with [Realty in US](https://rapidapi.com/apidojo/api/realty-in-us) subscription (free tier)

### 1. Clone and install

```bash
git clone https://github.com/MounirArayssi/real-estate-pipeline.git
cd real-estate-pipeline
pip install -r requirements.txt
```

### 2. Set up PostgreSQL

```sql
CREATE DATABASE real_estate;
\c real_estate

CREATE TABLE listings (
    id SERIAL PRIMARY KEY,
    property_id VARCHAR(50) UNIQUE NOT NULL,
    listing_id VARCHAR(50),
    status VARCHAR(30),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(2),
    zip VARCHAR(10),
    price INTEGER,
    beds INTEGER,
    baths NUMERIC(3,1),
    sqft INTEGER,
    lot_sqft INTEGER,
    property_type VARCHAR(50),
    property_subtype VARCHAR(50),
    list_date DATE,
    lat NUMERIC(10,6),
    lon NUMERIC(10,6),
    photo_count INTEGER,
    is_new_listing BOOLEAN,
    is_foreclosure BOOLEAN,
    is_price_reduced BOOLEAN,
    price_reduced_amount INTEGER,
    last_sold_price INTEGER,
    last_sold_date DATE,
    photo_url TEXT,
    detail_url TEXT,
    source VARCHAR(50) DEFAULT 'realty_in_us',
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_listings_zip ON listings(zip);
CREATE INDEX idx_listings_city ON listings(city);
CREATE INDEX idx_listings_price ON listings(price);
CREATE INDEX idx_listings_list_date ON listings(list_date);
CREATE INDEX idx_listings_status ON listings(status);
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=real_estate
DB_USER=postgres
DB_PASSWORD=your_password
RAPIDAPI_KEY=your_rapidapi_key
```

### 4. Run the pipeline

```bash
# Extract and load listings
python src/scraper.py

# Run transforms (price_per_sqft, analytical views)
python src/transforms.py
```

### 5. Query the results

```sql
-- Zip code market summary
SELECT * FROM zip_summary;

-- Price distribution across market
SELECT * FROM price_distribution;

-- Property type breakdown
SELECT * FROM property_type_stats;
```

## Scheduling

To automate daily runs:

**Windows (Task Scheduler):**
1. Create a `.bat` file that activates your venv and runs both scripts
2. Add a Basic Task in Task Scheduler with a daily trigger

**Linux/Mac (cron):**
```bash
0 8 * * * cd /path/to/real-estate-pipeline && python src/scraper.py && python src/transforms.py
```

## Testing

```bash
python -m pytest test/ -v
```

Tests cover:
- Field extraction from nested API responses
- Date parsing and formatting
- Handling of missing/null data
- Edge cases (no photo, minimal data)

## CI/CD

Every push to `dev` or `main` triggers a GitHub Actions workflow that:
1. Spins up a PostgreSQL 16 service container
2. Installs Python dependencies
3. Runs the full test suite

## Sample Output

```
2026-02-12 22:07:43,103 [INFO] ==================================================
2026-02-12 22:07:43,104 [INFO] Real Estate Pipeline Run
2026-02-12 22:07:43,104 [INFO] ==================================================
2026-02-12 22:07:44,506 [INFO] [OK] 90004: 15 fetched, 15 upserted
2026-02-12 22:07:45,939 [INFO] [OK] 90012: 15 fetched, 15 upserted
2026-02-12 22:07:48,358 [INFO] [OK] 90015: 15 fetched, 15 upserted
2026-02-12 22:07:49,613 [INFO] [OK] 90028: 13 fetched, 13 upserted
2026-02-12 22:07:50,787 [INFO] [OK] 90036: 15 fetched, 15 upserted
2026-02-12 22:07:50,788 [INFO] Done! 73 fetched, 73 upserted
```

## Roadmap

- [ ] Streamlit dashboard with interactive map and charts
- [ ] Price change tracking between pipeline runs
- [ ] Historical snapshots table for trend analysis
- [ ] Docker containerization
- [ ] Expand to additional cities and markets
- [ ] Error alerting via email/Slack

## License

MIT
