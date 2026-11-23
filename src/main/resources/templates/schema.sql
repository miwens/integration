CREATE TABLE IF NOT EXISTS spares_history (
    id                SERIAL PRIMARY KEY,
    spare_code        VARCHAR(100) UNIQUE NOT NULL,
    spare_name        TEXT,
    spare_description TEXT,
    spare_type        VARCHAR(200),
    spare_status      VARCHAR(100),
    price             NUMERIC(10,2),
    quantity          INTEGER,
    updated_at        TIMESTAMP,
    last_seen_at      TIMESTAMP DEFAULT NOW(),
    is_archived       BOOLEAN DEFAULT FALSE,
    archived_at       TIMESTAMP
);
