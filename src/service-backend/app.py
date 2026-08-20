import os
import json
import requests as http_requests
from datetime import datetime, date, timedelta
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import func, desc
import time as _time
from prometheus_client import Counter, Histogram, Gauge, start_http_server

app = Flask(__name__)
CORS(app)

db_host = os.environ.get("DB_HOST", "ecomm-postgres")
db_user = os.environ.get("DB_USER", "ecomm")
db_password = os.environ.get("DB_PASSWORD", "ecomm")
db_name = os.environ.get("DB_NAME", "ecommdb")
app.config["SQLALCHEMY_DATABASE_URI"] = (
    f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
REQUEST_COUNT = Counter(
    'flask_http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)
REQUEST_LATENCY = Histogram(
    'flask_http_request_duration_seconds',
    'HTTP request latency in seconds',
    ['method', 'endpoint']
)
ACTIVE_REQUESTS = Gauge(
    'flask_http_requests_active',
    'Number of active HTTP requests'
)


@app.before_request
def _before_request():
    request._prom_start_time = _time.time()
    ACTIVE_REQUESTS.inc()


@app.after_request
def _after_request(response):
    latency = _time.time() - getattr(request, '_prom_start_time', _time.time())
    endpoint = request.path
    REQUEST_COUNT.labels(method=request.method, endpoint=endpoint, status=response.status_code).inc()
    REQUEST_LATENCY.labels(method=request.method, endpoint=endpoint).observe(latency)
    ACTIVE_REQUESTS.dec()
    return response


# ---------------------------------------------------------------------------
# Kafka producer (optional — backend still works without Kafka)
APRIORI_API_URL = os.environ.get("APRIORI_API_URL", "http://apriori-api:5002")

# ---------------------------------------------------------------------------
kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_producer = None

try:
    from kafka import KafkaProducer
    kafka_producer = KafkaProducer(
        bootstrap_servers=kafka_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        request_timeout_ms=5000,
    )
except Exception as e:
    print(f"WARNING: Kafka producer unavailable — events will be logged locally. ({e})")


def send_kafka_event(topic: str, message: dict) -> None:
    if kafka_producer:
        try:
            kafka_producer.send(topic, message)
        except Exception as e:
            print(f"Kafka send error on topic {topic}: {e}")
    else:
        print(f"[Kafka:{topic}] {message}")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Category(db.Model):
    __tablename__ = "categories"
    id = db.Column(db.Integer, primary_key=True)
    category_name = db.Column(db.String(255), nullable=False)

    def to_dict(self):
        return {"id": self.id, "category_name": self.category_name}


class Product(db.Model):
    __tablename__ = "products"
    id = db.Column(db.Integer, primary_key=True)
    asin = db.Column(db.String(50), unique=True, nullable=False)
    title = db.Column(db.Text)
    img_url = db.Column(db.Text)
    product_url = db.Column(db.Text)
    stars = db.Column(db.Float)
    reviews = db.Column(db.Integer)
    price = db.Column(db.Float)
    list_price = db.Column(db.Float)
    category_id = db.Column(db.Integer, db.ForeignKey("categories.id"), nullable=False)
    is_best_seller = db.Column(db.Boolean)
    bought_in_last_month = db.Column(db.Integer)
    category = db.relationship("Category", backref=db.backref("products", lazy=True))

    def to_dict(self):
        return {
            "id": self.id,
            "asin": self.asin,
            "title": self.title,
            "img_url": self.img_url,
            "product_url": self.product_url,
            "stars": self.stars,
            "reviews": self.reviews,
            "price": self.price,
            "list_price": self.list_price,
            "category_id": self.category_id,
            "category_name": self.category.category_name if self.category else None,
            "is_best_seller": self.is_best_seller,
            "bought_in_last_month": self.bought_in_last_month,
        }


class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    first_name = db.Column(db.String(100))
    last_name = db.Column(db.String(100))
    email = db.Column(db.String(150), unique=True, nullable=False)
    phone_number = db.Column(db.String(50))
    region = db.Column(db.String(100))
    country = db.Column(db.String(100))
    hashed_password = db.Column(db.String(255), nullable=False)
    is_deleted = db.Column(db.Boolean, default=False)

    def to_dict(self):
        return {
            "id": self.id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "email": self.email,
            "phone_number": self.phone_number,
            "region": self.region,
            "country": self.country,
            "is_deleted": self.is_deleted,
        }


class Event(db.Model):
    __tablename__ = "events"
    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    visitor_id = db.Column(db.String(100), nullable=True)
    user_id = db.Column(db.Integer, nullable=True)
    timestamp = db.Column(db.DateTime, default=func.now())
    event_type = db.Column(db.String(50))
    product_id = db.Column(db.Integer, nullable=True)
    order_id_for_refund = db.Column(db.String(100), nullable=True)
    is_recommended = db.Column(db.Boolean, default=False)

    def to_dict(self):
        return {
            "id": self.id,
            "visitor_id": self.visitor_id,
            "user_id": self.user_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "event_type": self.event_type,
            "product_id": self.product_id,
            "order_id_for_refund": self.order_id_for_refund,
            "is_recommended": self.is_recommended,
        }


class CartItem(db.Model):
    __tablename__ = "cart_items"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    product_id = db.Column(db.Integer, db.ForeignKey("products.id"), nullable=False)
    quantity = db.Column(db.Integer, default=1)
    user = db.relationship("User", backref=db.backref("cart_items", lazy=True))
    product = db.relationship("Product", backref=db.backref("cart_items", lazy=True))

    def to_dict(self):
        return {
            "id": self.id,
            "user_id": self.user_id,
            "product_id": self.product_id,
            "quantity": self.quantity,
            "product": self.product.to_dict() if self.product else None,
        }


class RecommendationScore(db.Model):
    __tablename__ = "recommendation_scores"
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey("products.id"), primary_key=True)
    score = db.Column(db.Float, nullable=False)
    user = db.relationship("User", backref=db.backref("recommendation_scores", lazy=True))
    product = db.relationship("Product", backref=db.backref("recommendation_scores", lazy=True))

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "product_id": self.product_id,
            "score": self.score,
            "product": self.product.to_dict() if self.product else None,
        }


class DailyReport(db.Model):
    __tablename__ = "daily_reports"
    date = db.Column(db.Date, primary_key=True)
    total_revenue = db.Column(db.Float, nullable=False, default=0.0)
    total_orders = db.Column(db.Integer, nullable=False, default=0)
    recommended_revenue = db.Column(db.Float, nullable=False, default=0.0)
    recommender_ctr = db.Column(db.Float, nullable=False, default=0.0)
    recommender_conversion = db.Column(db.Float, nullable=False, default=0.0)

    def to_dict(self):
        return {
            "date": self.date.isoformat() if self.date else None,
            "total_revenue": self.total_revenue,
            "total_orders": self.total_orders,
            "recommended_revenue": self.recommended_revenue,
            "recommender_ctr": self.recommender_ctr,
            "recommender_conversion": self.recommender_conversion,
        }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


# 1. Register
@app.route("/api/register", methods=["POST"])
def register():
    data = request.get_json() or {}
    email = data.get("email")
    password = data.get("password")
    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400
    if User.query.filter_by(email=email).first():
        return jsonify({"error": "User with this email already exists"}), 400
    new_user = User(
        first_name=data.get("first_name", ""),
        last_name=data.get("last_name", ""),
        email=email,
        phone_number=data.get("phone_number", ""),
        region=data.get("region", ""),
        country=data.get("country", ""),
        hashed_password=generate_password_hash(password),
    )
    db.session.add(new_user)
    db.session.commit()
    return jsonify({"message": "User registered successfully", "user": new_user.to_dict()}), 201


# 2. Login
@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json() or {}
    email = data.get("email")
    password = data.get("password")
    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400
    user = User.query.filter_by(email=email, is_deleted=False).first()
    if not user or not check_password_hash(user.hashed_password, password):
        return jsonify({"error": "Invalid email or password"}), 401
    return jsonify({"message": "Login successful", "user": user.to_dict()}), 200


# 3. Categories
@app.route("/api/categories", methods=["GET"])
def get_categories():
    return jsonify([c.to_dict() for c in Category.query.order_by(Category.category_name).all()])


# 4. Products (paginated + search)
@app.route("/api/products", methods=["GET"])
def get_products():
    page = request.args.get("page", 1, type=int)
    limit = min(request.args.get("limit", 20, type=int), 100)
    search = request.args.get("search", "", type=str)
    query = Product.query
    if search:
        query = query.filter(Product.title.ilike(f"%{search}%"))
    paginated = query.paginate(page=page, per_page=limit, error_out=False)
    return jsonify({
        "products": [p.to_dict() for p in paginated.items],
        "total": paginated.total,
        "page": page,
        "pages": paginated.pages,
        "limit": limit,
    })


# 5. Product detail + log view event
@app.route("/api/products/<int:product_id>", methods=["GET"])
def get_product(product_id):
    product = Product.query.get_or_404(product_id)
    user_id = request.args.get("user_id", type=int)
    visitor_id = request.args.get("visitor_id", type=str)
    is_rec = request.args.get("is_recommended", "false").lower() == "true"
    if user_id or visitor_id:
        valid_user = User.query.get(user_id) if user_id else None
        event = Event(
            visitor_id=visitor_id,
            user_id=valid_user.id if valid_user else None,
            event_type="view",
            product_id=product.id,
            is_recommended=is_rec,
        )
        db.session.add(event)
        db.session.commit()
        send_kafka_event("user-events", {
            "visitor_id": visitor_id,
            "user_id": valid_user.id if valid_user else None,
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": "view",
            "product_id": product.id,
            "is_recommended": is_rec,
        })
    return jsonify(product.to_dict())


# 6. Products by category (paginated)
@app.route("/api/products/category/<int:category_id>", methods=["GET"])
def get_products_by_category(category_id):
    page = request.args.get("page", 1, type=int)
    limit = min(request.args.get("limit", 20, type=int), 100)
    paginated = Product.query.filter_by(category_id=category_id).paginate(
        page=page, per_page=limit, error_out=False
    )
    return jsonify({
        "products": [p.to_dict() for p in paginated.items],
        "total": paginated.total,
        "page": page,
        "pages": paginated.pages,
        "limit": limit,
    })


# 7. Get cart
@app.route("/api/cart", methods=["GET"])
def get_cart():
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return jsonify({"error": "user_id is required"}), 400
    return jsonify([item.to_dict() for item in CartItem.query.filter_by(user_id=user_id).all()])


# 8. Add to cart
@app.route("/api/cart/add", methods=["POST"])
def add_to_cart():
    data = request.get_json() or {}
    user_id = data.get("user_id")
    product_id = data.get("product_id")
    quantity = data.get("quantity", 1)
    is_rec = data.get("is_recommended", False)
    if not user_id or not product_id:
        return jsonify({"error": "user_id and product_id are required"}), 400
    User.query.get_or_404(user_id)
    Product.query.get_or_404(product_id)
    cart_item = CartItem.query.filter_by(user_id=user_id, product_id=product_id).first()
    if cart_item:
        cart_item.quantity += quantity
    else:
        cart_item = CartItem(user_id=user_id, product_id=product_id, quantity=quantity)
        db.session.add(cart_item)
    db.session.add(Event(user_id=user_id, event_type="add_to_cart", product_id=product_id, is_recommended=is_rec))
    db.session.commit()
    send_kafka_event("user-events", {
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": "add_to_cart",
        "product_id": product_id,
        "is_recommended": is_rec,
    })
    return jsonify({"message": "Product added to cart", "cart_item": cart_item.to_dict()}), 200


# 9. Remove from cart
@app.route("/api/cart/remove", methods=["POST"])
def remove_from_cart():
    data = request.get_json() or {}
    user_id = data.get("user_id")
    product_id = data.get("product_id")
    quantity = data.get("quantity")
    if not user_id or not product_id:
        return jsonify({"error": "user_id and product_id are required"}), 400
    cart_item = CartItem.query.filter_by(user_id=user_id, product_id=product_id).first()
    if not cart_item:
        return jsonify({"error": "Item not found in cart"}), 404
    if quantity and cart_item.quantity > quantity:
        cart_item.quantity -= quantity
        message = "Cart quantity decremented"
    else:
        db.session.delete(cart_item)
        message = "Product removed from cart"
    db.session.add(Event(user_id=user_id, event_type="delete_from_cart", product_id=product_id))
    db.session.commit()
    send_kafka_event("user-events", {
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": "delete_from_cart",
        "product_id": product_id,
    })
    return jsonify({"message": message}), 200


# 10. Checkout
@app.route("/api/transaction", methods=["POST"])
def make_transaction():
    data = request.get_json() or {}
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"error": "user_id is required"}), 400
    cart_items = CartItem.query.filter_by(user_id=user_id).all()
    if not cart_items:
        return jsonify({"error": "Cart is empty"}), 400
    ts = datetime.utcnow().isoformat()
    for item in cart_items:
        db.session.add(Event(user_id=user_id, event_type="quantity", product_id=item.product_id))
        db.session.add(Event(user_id=user_id, event_type="transaction", product_id=item.product_id))
        db.session.delete(item)
        send_kafka_event("user-events", {"user_id": user_id, "timestamp": ts, "event_type": "quantity", "product_id": item.product_id})
        send_kafka_event("user-events", {"user_id": user_id, "timestamp": ts, "event_type": "transaction", "product_id": item.product_id})
    db.session.commit()
    return jsonify({"message": "Transaction successful. Cart cleared."}), 200


# 11. Recommendations (Apriori model → precomputed scores → co-occurrence → category popularity → global fallback)
@app.route("/api/recommendations", methods=["GET"])
def get_recommendations():
    user_id = request.args.get("user_id", type=int)
    visitor_id = request.args.get("visitor_id", type=str)
    limit = request.args.get("limit", 8, type=int)

    # Resolve recent product interactions for this user/visitor
    interacted_products = []
    if user_id:
        events = (Event.query.filter_by(user_id=user_id)
                  .filter(Event.product_id.isnot(None))
                  .order_by(Event.timestamp.desc()).limit(10).all())
        interacted_products = [e.product_id for e in events if e.product_id]
    elif visitor_id:
        events = (Event.query.filter_by(visitor_id=visitor_id)
                  .filter(Event.product_id.isnot(None))
                  .order_by(Event.timestamp.desc()).limit(10).all())
        interacted_products = [e.product_id for e in events if e.product_id]

    # --- Strategy 0: Apriori association-rules model ---
    apriori_product_ids = []
    if interacted_products:
        try:
            resp = http_requests.post(
                f"{APRIORI_API_URL}/predict",
                json={"items": interacted_products},
                timeout=3,
            )
            if resp.status_code == 200:
                apriori_product_ids = [
                    int(x) for x in resp.json().get("predicted_items", [])
                    if str(x).isdigit()
                ]
        except Exception:
            pass  # Apriori service unavailable — continue to fallback

    recommendations = []
    if apriori_product_ids:
        recommendations = Product.query.filter(Product.id.in_(apriori_product_ids)).limit(limit).all()

    # Strategy 1: co-occurrence collaborative filtering (only if Apriori didn't fill results)
    if len(recommendations) < limit and interacted_products:
        users_sub = (
            db.session.query(Event.user_id)
            .filter(Event.product_id.in_(interacted_products))
            .filter(Event.user_id.isnot(None))
            .subquery()
        )
        visitors_sub = (
            db.session.query(Event.visitor_id)
            .filter(Event.product_id.in_(interacted_products))
            .filter(Event.visitor_id.isnot(None))
            .subquery()
        )
        co_occurring = (
            db.session.query(Event.product_id, func.count(Event.product_id).label("co_count"))
            .filter(
                (Event.user_id.in_(users_sub)) | (Event.visitor_id.in_(visitors_sub))
            )
            .filter(Event.product_id.isnot(None))
            .filter(~Event.product_id.in_(interacted_products))
            .group_by(Event.product_id)
            .order_by(desc("co_count"))
            .limit(limit)
            .all()
        )
        if co_occurring:
            existing_ids = [p.id for p in recommendations]
            co_prods = Product.query.filter(
                Product.id.in_([p[0] for p in co_occurring]),
                ~Product.id.in_(existing_ids + interacted_products)
            ).limit(limit - len(recommendations)).all()
            recommendations.extend(co_prods)

    # Strategy 2: category-based popularity
    if len(recommendations) < limit and interacted_products:
        cat_ids = [
            p.category_id
            for p in Product.query.filter(Product.id.in_(interacted_products)).all()
            if p.category_id
        ]
        if cat_ids:
            existing_ids = [p.id for p in recommendations]
            category_popular = (
                Product.query.filter(Product.category_id.in_(cat_ids))
                .filter(~Product.id.in_(interacted_products + existing_ids))
                .order_by(Product.reviews.desc(), Product.stars.desc())
                .limit(limit - len(recommendations))
                .all()
            )
            recommendations.extend(category_popular)

    # Strategy 3: global popularity cold start
    if len(recommendations) < limit:
        existing_ids = [p.id for p in recommendations]
        all_exclude = interacted_products + existing_ids
        global_popular = (
            Product.query.filter(~Product.id.in_(all_exclude))
            .order_by(desc(Product.is_best_seller), Product.reviews.desc(), Product.stars.desc())
            .limit(limit - len(recommendations))
            .all()
        )
        recommendations.extend(global_popular)

    return jsonify([p.to_dict() for p in recommendations[:limit]])


# 12. Trigger Airflow training DAG
@app.route("/api/train", methods=["POST"])
def trigger_training():
    import requests as http_requests
    airflow_url = os.environ.get("AIRFLOW_URL", "http://airflow-webserver:8080")
    dag_id = "mlops_pipeline"
    try:
        resp = http_requests.post(
            f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns",
            json={"conf": {}},
            auth=(
                os.environ.get("AIRFLOW_USER", "admin"),
                os.environ.get("AIRFLOW_PASSWORD", "admin"),
            ),
            timeout=10,
        )
        if resp.status_code in (200, 201):
            return jsonify({"status": "success", "message": f"DAG {dag_id} triggered", "run": resp.json()}), 200
        return jsonify({"status": "error", "message": resp.text}), resp.status_code
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# 13. Daily report
@app.route("/api/report/daily", methods=["POST"])
def daily_report():
    target_date = date.today() - timedelta(days=1)
    existing = DailyReport.query.get(target_date)
    if existing:
        return jsonify({"status": "success", "message": "Report already exists", "report": existing.to_dict()}), 200
    try:
        total_rev = (
            db.session.query(func.coalesce(func.sum(Product.price), 0))
            .select_from(Event).join(Product, Event.product_id == Product.id)
            .filter(Event.event_type == "transaction", func.date(Event.timestamp) == target_date)
            .scalar()
        )
        total_ord = (
            db.session.query(func.count(Event.id))
            .filter(Event.event_type == "transaction", func.date(Event.timestamp) == target_date)
            .scalar()
        )
        rec_rev = (
            db.session.query(func.coalesce(func.sum(Product.price), 0))
            .select_from(Event).join(Product, Event.product_id == Product.id)
            .filter(Event.event_type == "transaction", Event.is_recommended.is_(True), func.date(Event.timestamp) == target_date)
            .scalar()
        )
        impressions = (
            db.session.query(func.count(Event.id))
            .filter(Event.event_type == "view", Event.is_recommended.is_(True), func.date(Event.timestamp) == target_date)
            .scalar()
        )
        clicks = (
            db.session.query(func.count(Event.id))
            .filter(Event.event_type == "add_to_cart", Event.is_recommended.is_(True), func.date(Event.timestamp) == target_date)
            .scalar()
        )
        conversions = (
            db.session.query(func.count(Event.id))
            .filter(Event.event_type == "transaction", Event.is_recommended.is_(True), func.date(Event.timestamp) == target_date)
            .scalar()
        )
        ctr = (clicks / impressions) if impressions else 0.0
        conv = (conversions / clicks) if clicks else 0.0
        report = DailyReport(
            date=target_date,
            total_revenue=float(total_rev),
            total_orders=int(total_ord),
            recommended_revenue=float(rec_rev),
            recommender_ctr=float(ctr * 100),
            recommender_conversion=float(conv * 100),
        )
        db.session.add(report)
        db.session.commit()
        return jsonify({"status": "success", "message": "Daily report generated", "report": report.to_dict()}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500


# 14. Get daily reports
@app.route("/api/report/daily", methods=["GET"])
def get_daily_reports():
    reports = DailyReport.query.order_by(DailyReport.date.desc()).limit(30).all()
    return jsonify([r.to_dict() for r in reports])


if __name__ == "__main__":
    # Start Prometheus metrics server on port 8000 (separate from Flask's port 5001)
    start_http_server(8000)
    with app.app_context():
        # Only creates tables that don't exist (cart_items, recommendation_scores, daily_reports)
        db.create_all()
    app.run(host="0.0.0.0", port=5001, debug=False)
