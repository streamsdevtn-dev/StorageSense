-- db/schema.sql
-- Prototyping schema for StorageSense National using SQLite

-- 1. Base Geography Table (ZIP codes and Counties)
CREATE TABLE IF NOT EXISTS geography (
    zip_code TEXT PRIMARY KEY,
    county_fips TEXT,
    state_abbr TEXT,
    city TEXT,
    lat REAL,
    lon REAL
);

-- 2. Tier 1 Macro Layer (Agent A: Housing & Supply)
CREATE TABLE IF NOT EXISTS housing_macro (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE,
    mortgage_spread REAL,       -- Freddie Mac 30yr vs effective rate
    building_permits_index REAL,-- US Census permits proxy
    existing_home_sales REAL,   -- NAR / FRED existing home sales
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS local_building_permits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    zip_code TEXT,
    permit_date DATE,
    units INTEGER,              -- Number of housing units permitted
    type TEXT,                  -- Multifamily or Single Family
    FOREIGN KEY (zip_code) REFERENCES geography (zip_code)
);

-- 3. Tier 2 Behavioral Pulse (Agent B)
CREATE TABLE IF NOT EXISTS behavioral_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    zip_code TEXT,
    date DATE,
    usps_net_migration INTEGER, -- Net change of address
    google_search_index REAL,   -- Relative search volume for "storage units near me"
    fema_disaster_active BOOLEAN DEFAULT 0,
    FOREIGN KEY (zip_code) REFERENCES geography (zip_code)
);

-- 4. Tier 3 Structural Anchor (Agent C)
CREATE TABLE IF NOT EXISTS structural_anchors (
    zip_code TEXT PRIMARY KEY,
    boomer_population_pct REAL, -- % of population aged 55+
    median_home_value REAL,     -- Zillow index
    irs_migration_wealth_idx REAL,-- IRS wealth proxy for inbound residents
    last_updated DATE,
    FOREIGN KEY (zip_code) REFERENCES geography (zip_code)
);

-- 5. Score Output (Engine)
CREATE TABLE IF NOT EXISTS demand_scores (
    zip_code TEXT PRIMARY KEY,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    liquidity_score_l REAL,
    movement_score_m REAL,
    structural_score_s REAL,
    competition_score_c REAL,
    final_score REAL,
    FOREIGN KEY (zip_code) REFERENCES geography (zip_code)
);
