-- EcommDB Schema

CREATE TABLE IF NOT EXISTS categories (
    id          INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id              SERIAL PRIMARY KEY,
    asin            TEXT UNIQUE NOT NULL,
    title           TEXT,
    img_url         TEXT,
    product_url     TEXT,
    stars           NUMERIC(3,1),
    reviews         INTEGER,
    price           NUMERIC(10,2),
    list_price      NUMERIC(10,2),
    category_id     INTEGER REFERENCES categories(id),
    is_best_seller  BOOLEAN,
    bought_in_last_month INTEGER
);

CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    email           TEXT UNIQUE,
    phone_number    TEXT,
    region          TEXT,
    country         TEXT,
    hashed_password TEXT,
    is_deleted      BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS events (
    id                  BIGSERIAL PRIMARY KEY,
    visitor_id          TEXT,
    user_id             INTEGER,
    timestamp           TIMESTAMPTZ,
    event_type          TEXT,
    product_id          INTEGER,
    order_id_for_refund TEXT,
    is_recommended      BOOLEAN DEFAULT FALSE
);
