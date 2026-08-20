import argparse
import os
import string
import time
from datetime import datetime

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------
# Expanded Reference Data aligned with ARCH.md Section 3.2
# --------------------------------------------------------------------------
EVENT_CONFIG = {
    # 1. user-events topic
    "view": "user-events",
    "add_to_cart": "user-events",
    "delete_from_cart": "user-events",
    "quantity": "user-events",
    "transaction": "user-events",
    "refund_request": "user-events",
    
    # 2. recommendation-actions topic
    "recommendation_shown": "recommendation-actions",
    "recommendation_clicked": "recommendation-actions",
    "recommendation_accepted": "recommendation-actions",
    "recommendation_rejected": "recommendation-actions",
    
    # 3. notification-events topic
    "discount_offer_sent": "notification-events",
    "reminder_sent": "notification-events",
    "offer_accepted": "notification-events",
    "offer_declined": "notification-events"
}

ALL_EVENTS = list(EVENT_CONFIG.keys())
# Realistic weight distribution favoring browsing behavior
ALL_WEIGHTS = [
    0.42, 0.14, 0.05, 0.04, 0.04, 0.01,  # user-events (70%)
    0.15, 0.06, 0.02, 0.02,              # recommendation-actions (25%)
    0.02, 0.01, 0.01, 0.01               # notification-events (5%)
]

RECOMMENDATION_VIEWS = ["home", "product_detail", "final_cart", "notification"]

REGIONS_COUNTRIES = [
    ("North America", "USA"), ("North America", "Canada"), ("North America", "Mexico"),
    ("Europe", "UK"), ("Europe", "Germany"), ("Europe", "France"), ("Europe", "Spain"), ("Europe", "Italy"),
    ("Asia", "China"), ("Asia", "India"), ("Asia", "Japan"), ("Asia", "South Korea"), ("Asia", "Singapore"), ("Asia", "UAE"),
    ("South America", "Brazil"), ("South America", "Argentina"),
    ("Africa", "Nigeria"), ("Africa", "South Africa"),
    ("Oceania", "Australia"), ("Oceania", "New Zealand"),
]

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna", "Ishaan", "Rohan", 
    "Ananya", "Diya", "Saanvi", "Aadhya", "Kavya", "Priya", "Anika", "Riya", "Meera", "Neha", 
    "Rahul", "Amit", "Sanjay", "Deepak", "Pooja", "Sunita", "Lakshmi", "Divya", "Karan", "Arnav",
    "John", "Jane", "Emma", "Liam", "Olivia", "Noah", "Sophia", "Lucas", "Mia", "James", 
    "Emily", "Michael", "Sarah", "David", "Laura", "Mei", "Wei", "Yuki", "Hiro", "Jin", 
    "Ling", "Haruto", "Sakura", "Min-jun", "Ji-woo", "Ahmed", "Fatima", "Ali", "Layla", "Omar", 
    "Yasmin", "Hassan", "Zainab", "Carlos", "Sofia", "Diego", "Valentina", "Mateo", "Camila", "Santiago", 
    "Isabella", "Chidi", "Ngozi", "Kwame", "Amara", "Tendai", "Zola", "Ivan", "Nadia", "Lukas", 
    "Elena", "Marco", "Anna", "Pierre", "Claire"
]

LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Patel", "Reddy", "Iyer", "Nair", "Menon", "Rao", "Chopra", 
    "Malhotra", "Kapoor", "Joshi", "Desai", "Mehta", "Agarwal", "Bhatt", "Kulkarni", "Pillai", "Chatterjee",
    "Smith", "Brown", "Wilson", "Taylor", "Anderson", "Thomas", "Moore", "Martin", "Chen", "Wang", 
    "Tanaka", "Suzuki", "Kim", "Park", "Nakamura", "Li", "Khan", "Al-Farsi", "Hassan", "Ibrahim", 
    "Said", "El-Masry", "Silva", "Garcia", "Lopez", "Martinez", "Rodriguez", "Fernandez", "Okafor", "Mensah", 
    "Ndlovu", "Adeyemi", "Muller", "Rossi", "Novak", "Petrov", "Kowalski", "Dubois"
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]

PRODUCT_TYPES = {
    "computers": ["Mouse", "Keyboard", "Monitor", "Laptop Stand", "Webcam"],
    "kitchen": ["Toaster", "Juicer", "Blender", "Coffee Maker", "Air Fryer"],
    "furniture": ["Office Chair", "Standing Desk", "Bookshelf", "Sofa", "Bed Frame"],
    "electronics": ["Bluetooth Speaker", "Smartwatch", "Headphones", "Power Bank", "Tablet"],
    "fitness": ["Yoga Mat", "Dumbbell Set", "Resistance Bands", "Treadmill"],
    "books": ["Novel", "Cookbook", "Biography", "Sci-Fi Anthology"],
    "clothing": ["Running Shoes", "Denim Jacket", "Wool Sweater", "T-Shirt Pack"],
    "toys": ["Building Blocks", "RC Car", "Puzzle Set", "Action Figure"],
    "beauty": ["Face Serum", "Hair Dryer", "Electric Razor", "Makeup Kit"],
    "others": ["Umbrella", "Backpack", "Water Bottle", "Desk Lamp"]
}
CATEGORIES = list(PRODUCT_TYPES.keys())

START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 6, 30)
TOTAL_SECONDS_RANGE = int((END_DATE - START_DATE).total_seconds())

USER_ID_NULL_RATE = 0.20
PRODUCT_ID_NULL_RATE = 0.05
IS_RECOMMENDED_RATE = 0.15
USER_DELETED_RATE = 0.05

ALNUM = np.array(list(string.ascii_uppercase + string.digits))

def generate_users(n_users: int, rng: np.random.Generator) -> pd.DataFrame:
    ids = np.arange(1, n_users + 1)
    first = rng.choice(FIRST_NAMES, size=n_users)
    last = rng.choice(LAST_NAMES, size=n_users)
    region_country_idx = rng.integers(0, len(REGIONS_COUNTRIES), size=n_users)
    regions = np.array([REGIONS_COUNTRIES[i][0] for i in region_country_idx])
    countries = np.array([REGIONS_COUNTRIES[i][1] for i in region_country_idx])
    domains = rng.choice(EMAIL_DOMAINS, size=n_users)

    emails = np.array([f"{f.lower()}.{l.lower()}{i}@{d}" for f, l, i, d in zip(first, last, ids, domains)])
    phone_digits = rng.integers(0, 10, size=(n_users, 11))
    phones = np.array(["+" + "".join(row.astype(str)) for row in phone_digits])

    pw_chars = np.array(list(string.ascii_letters + string.digits))
    pw_idx = rng.integers(0, len(pw_chars), size=(n_users, 22))
    hashed_passwords = np.array(["$2b$12$" + "".join(pw_chars[row]) for row in pw_idx])
    is_deleted = rng.random(n_users) < USER_DELETED_RATE

    return pd.DataFrame({
        "id": ids, "first_name": first, "last_name": last, "email": emails,
        "phone_number": phones, "region": regions, "country": countries,
        "hashed_password": hashed_passwords, "is_deleted": is_deleted,
    })

def generate_products(n_products: int, rng: np.random.Generator) -> pd.DataFrame:
    ids = np.arange(1, n_products + 1)
    categories = rng.choice(CATEGORIES, size=n_products)
    product_types = np.array([rng.choice(PRODUCT_TYPES[c]) for c in categories])
    names = np.array([f"{pt} Model-{i}" for pt, i in zip(product_types, ids)])
    short_desc = np.array([f"High quality {pt.lower()} model-{i} built for durability." for pt, i in zip(product_types, ids)])
    long_desc = np.array([f"Experience premium performance with the {n}." for n in names])
    features = np.array([f"Premium Quality/Durable Build/Version v{i}" for i in ids])
    prices = np.round(rng.uniform(9.99, 2999.99, size=n_products), 2)

    created_offsets = rng.integers(0, TOTAL_SECONDS_RANGE, size=n_products)
    created_on = pd.to_datetime(START_DATE) + pd.to_timedelta(created_offsets, unit="s")
    update_offsets = rng.integers(0, 60 * 86400, size=n_products)
    updated_on = created_on + pd.to_timedelta(update_offsets, unit="s")

    sig_ids = rng.integers(10000000, 99999999, size=n_products)
    image_urls = np.array([f"https://images.unsplash.com/photo-15{s}?auto=format&fit=crop&w=500&q=80&sig={i}" for s, i in zip(sig_ids, ids)])

    return pd.DataFrame({
        "id": ids, "name": names, "short_description": short_desc, "long_description": long_desc,
        "features": features, "category": categories, "image_url": image_urls, "price": prices,
        "created_on": created_on.strftime("%Y-%m-%d %H:%M:%S"), "updated_on": updated_on.strftime("%Y-%m-%d %H:%M:%S"),
    })

def generate_events_chunk(chunk_size: int, start_id: int, n_users: int, n_products: int, rng: np.random.Generator) -> pd.DataFrame:
    ids = np.arange(start_id, start_id + chunk_size)
    event_type = rng.choice(ALL_EVENTS, size=chunk_size, p=ALL_WEIGHTS)
    topics = np.array([EVENT_CONFIG[et] for et in event_type])

    offsets = rng.integers(0, TOTAL_SECONDS_RANGE, size=chunk_size)
    base_timestamps = pd.to_datetime(START_DATE) + pd.to_timedelta(offsets, unit="s")
    
    # Introduce small session steps forward for late-stage actions (transactions, refunds)
    tweaks = np.where((event_type == "transaction") | (event_type == "refund_request") | (event_type == "offer_accepted"), 
                      rng.integers(60, 1800, size=chunk_size), 0)
    timestamps = base_timestamps + pd.to_timedelta(tweaks, unit="s")

    anon_mask = rng.random(chunk_size) < USER_ID_NULL_RATE
    user_ids = rng.integers(1, n_users + 1, size=chunk_size).astype(object)
    user_ids[anon_mask] = ""
    visitor_ids = np.where(anon_mask, np.array([f"visitor_{i}" for i in ids]), "")

    product_null_mask = rng.random(chunk_size) < PRODUCT_ID_NULL_RATE
    product_ids = rng.integers(1, n_products + 1, size=chunk_size).astype(object)
    product_ids[product_null_mask] = ""

    refund_mask = (event_type == "refund_request") | (event_type == "offer_accepted")
    order_ids = np.full(chunk_size, "", dtype=object)
    n_refunds = int(refund_mask.sum())
    if n_refunds > 0:
        letters_idx = rng.integers(0, len(ALNUM), size=(n_refunds, 8))
        refund_orders = np.array(["ORD-" + "".join(row) for row in ALNUM[letters_idx]])
        order_ids[refund_mask] = refund_orders

    is_recommended = rng.random(chunk_size) < IS_RECOMMENDED_RATE
    recommendation_view = np.where(is_recommended, rng.choice(RECOMMENDATION_VIEWS, size=chunk_size), "")

    # session_id: one session per user per day, format matches sample/events.csv
    dates = timestamps.strftime("%Y-%m-%d")
    session_ids = np.where(
        np.array(user_ids, dtype=str) != "",
        np.array([f"u{uid}-{d}-s0" for uid, d in zip(user_ids, dates)]),
        np.array([f"v{vid}-{d}-s0" for vid, d in zip(visitor_ids, dates)]),
    )

    df = pd.DataFrame({
        "id": ids, "topic": topics, "session_id": session_ids,
        "visitor_id": visitor_ids, "user_id": user_ids,
        "timestamp": timestamps.strftime("%Y-%m-%dT%H:%M:%S"), "event_type": event_type,
        "product_id": product_ids, "order_id_for_refund": order_ids,
        "is_recommended": is_recommended, "recommendation_view": recommendation_view,
    })
    
    # Chronological sort within the generated block
    return df.sort_values(by="timestamp").reset_index(drop=True)

def generate_events_to_csv(path: str, n_events: int, n_users: int, n_products: int, 
                            chunk_size: int, rng: np.random.Generator, stream: bool, broker: str) -> None:
    def write_chunk_csv(df: pd.DataFrame, target_path: str, header: bool) -> None:
        df.to_csv(target_path, mode="w" if header else "a", header=header, index=False)

        base_name = os.path.basename(target_path)
        if base_name != "events.csv":
            compat_path = os.path.join(os.path.dirname(target_path), "events.csv")
            if os.path.abspath(target_path) != os.path.abspath(compat_path):
                df.to_csv(compat_path, mode="w" if header else "a", header=header, index=False)

    kafka_producer = None
    if stream:
        try:
            from kafka import KafkaProducer
            from kafka.errors import NoBrokersAvailable
            import json

            deadline = time.time() + 60
            last_error = None
            while time.time() < deadline:
                try:
                    kafka_producer = KafkaProducer(
                        bootstrap_servers=broker,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        api_version_auto_timeout_ms=30000,
                        request_timeout_ms=30000,
                    )
                    if kafka_producer.bootstrap_connected():
                        print(f" Connected to Kafka broker at: {broker}")
                        break
                    kafka_producer.close()
                except (NoBrokersAvailable, OSError, ConnectionError) as exc:
                    last_error = exc
                    print(f"[producer] Kafka broker not ready at {broker}, retrying in 3s...")
                    time.sleep(3)
                else:
                    break
            else:
                raise RuntimeError(f"Kafka broker at {broker} did not become ready in time: {last_error}")
        except ImportError:
            print("\n[WARNING] 'kafka-python' not found. Falling back to Log-Dry-Run mode.")

    n_written = 0
    first_chunk = True
    t_start = time.time()

    while n_written < n_events:
        this_chunk = min(chunk_size, n_events - n_written)
        df = generate_events_chunk(chunk_size=this_chunk, start_id=n_written + 1, n_users=n_users, n_products=n_products, rng=rng)
        
        if kafka_producer:
            for _, row in df.iterrows():
                record = row.to_dict()
                kafka_producer.send(record['topic'], record)
        elif stream:
            # Dry-run logging simulation if flag is set but package missing
            if first_chunk:
                print("\n--- KAFKA STREAM SIMULATION DRY-RUN (First 2 records) ---")
                print(df.head(2).to_dict(orient="records"))
                print("--------------------------------------------------------\n")
        
        # Always output to CSV as a persistent file store backing
        print(f"Writing {this_chunk:,} records to {path}...")
        write_chunk_csv(df, path, first_chunk)

        n_written += this_chunk
        first_chunk = False
        elapsed = time.time() - t_start
        rate = n_written / elapsed if elapsed > 0 else 0
        print(f"  events.csv: {n_written:,}/{n_events:,} records ({n_written / n_events:.1%}) | {rate:,.0f} recs/sec")

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce dataset matching ARCH.md structural requirements.")
    parser.add_argument("--n-users", type=int, default=1000)
    parser.add_argument("--n-products", type=int, default=200)
    parser.add_argument("--n-events", type=int, default=10000)
    parser.add_argument("--chunk-size", type=int, default=500000)
    parser.add_argument("--out-dir", type=str, default=".")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--stream", action="store_true", help="Simulate pushing to Kafka topics")
    parser.add_argument("--kafka-broker", type=str, default="localhost:9094")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    rng = np.random.default_rng(args.seed)

    users_path = os.path.join(args.out_dir, "users.csv")
    products_path = os.path.join(args.out_dir, "products.csv")
    events_path = os.path.join(args.out_dir, "events.csv")

    print(f"Generating users.csv ({args.n_users:,} rows)...")
    generate_users(args.n_users, rng).to_csv(users_path, index=False)

    print(f"Generating products.csv ({args.n_products:,} rows)...")
    generate_products(args.n_products, rng).to_csv(products_path, index=False)

    print(f"Generating events.csv ({args.n_events:,} rows)...")
    generate_events_to_csv(events_path, args.n_events, args.n_users, args.n_products, args.chunk_size, rng, args.stream, args.kafka_broker)
    print("Execution finalized successfully.")

if __name__ == "__main__":
    main()
