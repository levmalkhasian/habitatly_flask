import math
import os
from datetime import datetime, timedelta, timezone
from statistics import mean

import certifi
import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import PyMongoError

app = Flask(__name__)
load_dotenv()

WEIGHT_FIELDS = ("w_cost", "w_safety", "w_jobs", "w_weather", "w_lifestyle")

CITY_PROFILES = (
    {"city": "Austin", "w_cost": 3, "w_safety": 3, "w_jobs": 5, "w_weather": 4, "w_lifestyle": 5},
    {"city": "Seattle", "w_cost": 2, "w_safety": 4, "w_jobs": 5, "w_weather": 2, "w_lifestyle": 4},
    {"city": "Denver", "w_cost": 3, "w_safety": 4, "w_jobs": 4, "w_weather": 4, "w_lifestyle": 4},
    {"city": "Raleigh", "w_cost": 4, "w_safety": 4, "w_jobs": 4, "w_weather": 4, "w_lifestyle": 3},
    {"city": "San Diego", "w_cost": 1, "w_safety": 4, "w_jobs": 4, "w_weather": 5, "w_lifestyle": 5},
)

OPEN_DATA_CITIES_URL = (
    "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "geonames-all-cities-with-a-population-1000/records"
)
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


class ImportSourceError(RuntimeError):
    pass


class WeatherAPIError(RuntimeError):
    pass


def _clamp(value, min_value, max_value):
    return max(min_value, min(max_value, value))


def _parse_weight(form, field, default=3):
    try:
        return _clamp(int(form.get(field, default)), 0, 5)
    except (TypeError, ValueError):
        return default


def _parse_int(form, field, default):
    try:
        return int(form.get(field, default))
    except (TypeError, ValueError):
        return default


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _score_100_to_5(value):
    number = _to_float(value)
    if number is None:
        return None
    return _clamp(number / 20.0, 0.0, 5.0)


def _affordability_to_5(value):
    number = _to_float(value)
    if number is None:
        return None
    return _clamp((100.0 - number) / 20.0, 0.0, 5.0)


def _weather_to_5(avg_temp_f):
    number = _to_float(avg_temp_f)
    if number is None:
        return None
    return _clamp(5.0 - (abs(number - 68.0) / 12.0), 0.0, 5.0)


def _population_factor(population):
    pop_value = _to_float(population)
    if pop_value is None:
        return 50.0
    log_pop = math.log10(max(pop_value, 1000.0))
    return _clamp((log_pop - 3.0) / (7.3 - 3.0) * 100.0, 0.0, 100.0)


def _jobs_score_from_population(population):
    factor = _population_factor(population)
    return round(_clamp(25.0 + (factor * 0.75), 0.0, 100.0), 1)


def _build_city_name(doc):
    name = (doc.get("name") or "Unknown").strip()
    state = (doc.get("state") or "").strip()
    country = (doc.get("country") or "").strip()
    if state and country:
        return f"{name}, {state}, {country}"
    if country:
        return f"{name}, {country}"
    return name


def _quality_score(*scores):
    values = [score for score in scores if isinstance(score, (float, int))]
    if not values:
        return None
    return round(mean(values), 1)


def _init_cities_collection():
    mongodb_uri = os.getenv("MONGODB_URI", "").strip()
    mongodb_db_name = os.getenv("MONGODB_DB_NAME", "habitatly").strip() or "habitatly"
    if not mongodb_uri:
        return None, None, "MongoDB URI is not configured."

    try:
        client = MongoClient(
            mongodb_uri,
            serverSelectionTimeoutMS=3000,
            tlsCAFile=certifi.where(),
        )
        client.admin.command("ping")

        cities = client[mongodb_db_name]["cities"]
        cities.create_index([("name", ASCENDING), ("state", ASCENDING), ("country", ASCENDING)], unique=True)
        cities.create_index([("external_id", ASCENDING)], unique=True, sparse=True)
        cities.create_index([("quality_score", DESCENDING)])
        return client, cities, None
    except PyMongoError as exc:
        return None, None, f"MongoDB connection failed: {exc.__class__.__name__}"


MONGO_CLIENT, CITIES_COLLECTION, MONGO_ERROR = _init_cities_collection()


def _load_city_profiles_from_mongo():
    if CITIES_COLLECTION is None:
        return []

    try:
        docs = CITIES_COLLECTION.find(
            {},
            {
                "name": 1,
                "state": 1,
                "country": 1,
                "cost_of_living_score": 1,
                "safety_score": 1,
                "quality_score": 1,
                "jobs_score": 1,
                "avg_temp_f": 1,
                "environment_score": 1,
                "mobility_score": 1,
                "population": 1,
            },
        ).limit(500)
    except PyMongoError:
        return []

    profiles = []
    append_profile = profiles.append
    for doc in docs:
        cost_score = _affordability_to_5(doc.get("cost_of_living_score"))
        safety_score = _score_100_to_5(doc.get("safety_score"))
        jobs_raw = doc.get("jobs_score")
        if jobs_raw is None:
            jobs_raw = _jobs_score_from_population(doc.get("population"))
        jobs_score = _score_100_to_5(jobs_raw)
        weather_score = _weather_to_5(doc.get("avg_temp_f"))

        lifestyle_scores = [
            _score_100_to_5(doc.get("environment_score")),
            _score_100_to_5(doc.get("mobility_score")),
        ]
        lifestyle_values = [value for value in lifestyle_scores if value is not None]
        lifestyle_score = sum(lifestyle_values) / len(lifestyle_values) if lifestyle_values else None

        append_profile(
            {
                "city": _build_city_name(doc),
                "w_cost": cost_score if cost_score is not None else 2.5,
                "w_safety": safety_score if safety_score is not None else 2.5,
                "w_jobs": jobs_score if jobs_score is not None else 2.5,
                "w_weather": weather_score if weather_score is not None else 2.5,
                "w_lifestyle": lifestyle_score if lifestyle_score is not None else 2.5,
            }
        )
    return profiles


def _normalize_profiles(profiles):
    if not profiles:
        return []

    field_bounds = {}
    for field in WEIGHT_FIELDS:
        values = [profile[field] for profile in profiles]
        field_bounds[field] = (min(values), max(values))

    normalized = []
    append_normalized = normalized.append
    for profile in profiles:
        row = {"city": profile["city"]}
        for field in WEIGHT_FIELDS:
            min_value, max_value = field_bounds[field]
            if max_value > min_value:
                row[field] = (profile[field] - min_value) / (max_value - min_value)
            else:
                row[field] = 0.5
        append_normalized(row)
    return normalized


def _request_json(url, *, params=None):
    try:
        response = requests.get(
            url,
            params=params,
            timeout=20,
            headers={
                "Accept": "application/json",
                "User-Agent": "flask-base-app/1.0 (+city-import)",
            },
        )
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as exc:
        response = exc.response
        status = response.status_code if response is not None else "unknown"
        body = (response.text or "").strip()[:240] if response is not None else ""
        raise ImportSourceError(f"Request failed: HTTP {status}. {body}") from exc
    except requests.RequestException as exc:
        raise ImportSourceError(f"Request failed: {exc.__class__.__name__}") from exc


def _normalize_country_name(country):
    value = (country or "").strip()
    if not value:
        return ""
    lowered = value.lower()
    if lowered in {"us", "usa", "united states", "united states of america"}:
        return "United States"
    return value


def _escape_odsql_string(value):
    return value.replace("'", "''")


def _fetch_city_candidates(limit, country=""):
    normalized_country = _normalize_country_name(country)
    where_clauses = ["population is not null"]
    if normalized_country:
        where_clauses.append(f"cou_name_en = '{_escape_odsql_string(normalized_country)}'")
    where_expr = " and ".join(where_clauses)

    query_variants = (
        {
            "limit": limit,
            "select": "name,cou_name_en,coordinates,population",
            "where": where_expr,
            "order_by": "-population",
        },
        {
            "limit": limit,
            "select": "name,cou_name_en,coordinates,population",
            "where": where_expr,
        },
        {
            "limit": limit,
            "select": "name,cou_name_en,coordinates,population",
        },
    )

    payload = None
    errors = []
    for params in query_variants:
        try:
            payload = _request_json(OPEN_DATA_CITIES_URL, params=params)
            break
        except ImportSourceError as exc:
            errors.append(str(exc))

    if payload is None:
        raise ImportSourceError(" | ".join(errors[-2:]) if errors else "No source query could be completed.")

    candidates = []
    append_candidate = candidates.append
    for row in payload.get("results", []):
        name = (row.get("name") or "").strip()
        country = (row.get("cou_name_en") or "").strip()
        coords = row.get("coordinates") or {}
        lat = _to_float(coords.get("lat"))
        lon = _to_float(coords.get("lon"))
        population = _to_float(row.get("population"))

        if not name or not country or lat is None or lon is None:
            continue
        if normalized_country and country.lower() != normalized_country.lower():
            continue

        append_candidate(
            {
                "name": name,
                "country": country,
                "state": "",
                "latitude": lat,
                "longitude": lon,
                "population": int(population) if population is not None else None,
            }
        )

    return candidates


def _fetch_annual_avg_temp_f(latitude, longitude):
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    start_date = end_date - timedelta(days=364)
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "daily": "temperature_2m_mean",
        "timezone": "auto",
    }

    try:
        response = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=20)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise WeatherAPIError(f"Open-Meteo request failed: {exc.__class__.__name__}") from exc

    payload = response.json()
    values = payload.get("daily", {}).get("temperature_2m_mean", [])
    clean_values = [value for value in values if isinstance(value, (float, int))]
    if not clean_values:
        return None

    avg_c = mean(clean_values)
    return round((avg_c * 9 / 5) + 32, 1)


def _estimate_scores(population, avg_temp_f):
    population_factor = _population_factor(population)

    cost_of_living_score = round(_clamp(25.0 + (population_factor * 0.7), 0.0, 100.0), 1)
    safety_score = round(_clamp(80.0 - (population_factor * 0.35), 0.0, 100.0), 1)
    mobility_score = round(_clamp(20.0 + (population_factor * 0.75), 0.0, 100.0), 1)
    jobs_score = _jobs_score_from_population(population)

    if avg_temp_f is None:
        climate_score = 55.0
    else:
        climate_score = _clamp(100.0 - (abs(avg_temp_f - 68.0) * 3.0), 0.0, 100.0)
    environment_score = round(_clamp((climate_score * 0.7) + ((100.0 - population_factor) * 0.3), 0.0, 100.0), 1)

    affordability_score = 100.0 - cost_of_living_score
    quality_score = _quality_score(safety_score, affordability_score, jobs_score, mobility_score, environment_score)

    return {
        "cost_of_living_score": cost_of_living_score,
        "safety_score": safety_score,
        "jobs_score": jobs_score,
        "mobility_score": mobility_score,
        "environment_score": environment_score,
        "quality_score": quality_score,
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.post("/import/teleport")
@app.post("/import/cities")
@app.post("/import/us-cities")
def import_cities():
    if CITIES_COLLECTION is None:
        return jsonify({"ok": False, "error": MONGO_ERROR or "MongoDB not configured"}), 503

    limit = _clamp(_parse_int(request.form, "limit", default=15), 1, 100)
    requested_country = request.form.get("country", "")
    if request.path.endswith("/import/us-cities") and not requested_country:
        requested_country = "United States"
    normalized_country = _normalize_country_name(requested_country)

    summary = {
        "ok": True,
        "source": "geonames+open-meteo",
        "requested_limit": limit,
        "country": normalized_country or "all",
        "imported": 0,
        "updated": 0,
        "failed": 0,
        "errors": [],
    }

    try:
        candidates = _fetch_city_candidates(limit, country=normalized_country)
    except ImportSourceError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 502

    for city in candidates:
        avg_temp_f = None
        try:
            avg_temp_f = _fetch_annual_avg_temp_f(city["latitude"], city["longitude"])
        except WeatherAPIError:
            avg_temp_f = None

        scores = _estimate_scores(city.get("population"), avg_temp_f)

        city_doc = {
            "external_id": (
                f"geonames:{city['name'].lower()}:{city['country'].lower()}:"
                f"{city['latitude']:.4f}:{city['longitude']:.4f}"
            ),
            "name": city["name"],
            "state": city["state"],
            "country": city["country"],
            "rent_usd": None,
            "avg_temp_f": avg_temp_f,
            "population": city.get("population"),
            "safety_score": scores["safety_score"],
            "cost_of_living_score": scores["cost_of_living_score"],
            "jobs_score": scores["jobs_score"],
            "mobility_score": scores["mobility_score"],
            "environment_score": scores["environment_score"],
            "quality_score": scores["quality_score"],
            "data_sources": {
                "import": "opendatasoft_geonames",
                "avg_temp_f": "open_meteo_archive",
                "scoring": "estimated_from_population_and_temperature",
            },
            "updated_at": datetime.now(timezone.utc),
        }

        try:
            upsert_result = CITIES_COLLECTION.update_one(
                {"external_id": city_doc["external_id"]},
                {
                    "$set": city_doc,
                    "$setOnInsert": {"created_at": datetime.now(timezone.utc)},
                },
                upsert=True,
            )
        except PyMongoError as exc:
            summary["failed"] += 1
            if len(summary["errors"]) < 5:
                summary["errors"].append(f"Mongo write failed: {exc.__class__.__name__}")
            continue

        if upsert_result.upserted_id is not None:
            summary["imported"] += 1
        else:
            summary["updated"] += 1

    if summary["imported"] == 0 and summary["updated"] == 0 and not summary["errors"]:
        summary["failed"] = limit
        summary["errors"].append("No city records were returned by the source API.")

    return jsonify(summary), 200


@app.post("/results")
def results():
    weights = {field: _parse_weight(request.form, field) for field in WEIGHT_FIELDS}
    profiles = _load_city_profiles_from_mongo() or CITY_PROFILES
    ranking_profiles = _normalize_profiles(profiles)
    weight_total = sum(weights.values())
    if weight_total <= 0:
        weight_total = len(WEIGHT_FIELDS)
        weights = {field: 1 for field in WEIGHT_FIELDS}

    city_results = []
    append_result = city_results.append
    for profile in ranking_profiles:
        weighted_sum = sum(weights[field] * profile[field] for field in WEIGHT_FIELDS)
        score = (weighted_sum / weight_total) * 100.0
        append_result({"city": profile["city"], "score": round(score, 2)})

    city_results.sort(key=lambda row: row["score"], reverse=True)
    return render_template(
        "results.html",
        results=city_results,
        using_mongo=CITIES_COLLECTION is not None,
        db_error=MONGO_ERROR,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)