import json
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
FIELD_LABELS = {
    "w_cost": "Cost",
    "w_safety": "Safety",
    "w_jobs": "Jobs",
    "w_weather": "Weather",
    "w_lifestyle": "Lifestyle",
}

CITY_PROFILES = (
    {"city": "Austin", "w_cost": 3, "w_safety": 3, "w_jobs": 5, "w_weather": 4, "w_lifestyle": 5,
     "latitude": 30.27, "longitude": -97.74, "population": 979263, "avg_temp_f": 68.3, "cost_of_living_score": 55},
    {"city": "Seattle", "w_cost": 2, "w_safety": 4, "w_jobs": 5, "w_weather": 2, "w_lifestyle": 4,
     "latitude": 47.61, "longitude": -122.33, "population": 737015, "avg_temp_f": 52.0, "cost_of_living_score": 72},
    {"city": "Denver", "w_cost": 3, "w_safety": 4, "w_jobs": 4, "w_weather": 4, "w_lifestyle": 4,
     "latitude": 39.74, "longitude": -104.99, "population": 715522, "avg_temp_f": 50.1, "cost_of_living_score": 58},
    {"city": "Raleigh", "w_cost": 4, "w_safety": 4, "w_jobs": 4, "w_weather": 4, "w_lifestyle": 3,
     "latitude": 35.78, "longitude": -78.64, "population": 474069, "avg_temp_f": 60.0, "cost_of_living_score": 45},
    {"city": "San Diego", "w_cost": 1, "w_safety": 4, "w_jobs": 4, "w_weather": 5, "w_lifestyle": 5,
     "latitude": 32.72, "longitude": -117.16, "population": 1386932, "avg_temp_f": 64.5, "cost_of_living_score": 75},
)
EXTRA_FIELDS = ("latitude", "longitude", "population", "avg_temp_f", "cost_of_living_score")

OPEN_DATA_CITIES_URL = (
    "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "geonames-all-cities-with-a-population-1000/records"
)

US_STATE_CODES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
TELEPORT_BASE = "https://api.teleport.org/api"


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
        cities.create_index([("country", ASCENDING)])
        return client, cities, None
    except PyMongoError as exc:
        return None, None, f"MongoDB connection failed: {exc.__class__.__name__}"


MONGO_CLIENT, CITIES_COLLECTION, MONGO_ERROR = _init_cities_collection()


def _load_city_profiles_from_mongo(country_filter=None):
    if CITIES_COLLECTION is None:
        return []

    query = {}
    if country_filter:
        query["country"] = country_filter

    try:
        docs = CITIES_COLLECTION.find(
            query,
            {
                "name": 1, "state": 1, "country": 1,
                "cost_of_living_score": 1, "safety_score": 1, "quality_score": 1,
                "jobs_score": 1, "avg_temp_f": 1, "environment_score": 1,
                "mobility_score": 1, "population": 1, "latitude": 1, "longitude": 1,
            },
        ).limit(500)
    except PyMongoError:
        return []

    profiles = []
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
        lifestyle_values = [v for v in lifestyle_scores if v is not None]
        lifestyle_score = sum(lifestyle_values) / len(lifestyle_values) if lifestyle_values else None

        profiles.append({
            "city": _build_city_name(doc),
            "w_cost": cost_score if cost_score is not None else 2.5,
            "w_safety": safety_score if safety_score is not None else 2.5,
            "w_jobs": jobs_score if jobs_score is not None else 2.5,
            "w_weather": weather_score if weather_score is not None else 2.5,
            "w_lifestyle": lifestyle_score if lifestyle_score is not None else 2.5,
            "latitude": doc.get("latitude"),
            "longitude": doc.get("longitude"),
            "population": doc.get("population"),
            "avg_temp_f": doc.get("avg_temp_f"),
            "cost_of_living_score": doc.get("cost_of_living_score"),
        })
    return profiles


COUNTRY_DISPLAY_NAMES = {
    "Russian Federation": "Russia",
    "Korea, Republic of": "South Korea",
    "Korea, Democratic People's Republic of": "North Korea",
    "Vietnam": "Vietnam",
    "Congo, Democratic Republic of the": "DR Congo",
    "Congo, The Democratic Republic of the": "DR Congo",
    "Tanzania, United Republic of": "Tanzania",
    "Syrian Arab Republic": "Syria",
    "Iran, Islamic Republic of": "Iran",
    "Bolivia, Plurinational State of": "Bolivia",
    "Venezuela, Bolivarian Republic of": "Venezuela",
    "Lao People's Democratic Republic": "Laos",
    "Moldova, Republic of": "Moldova",
    "Macedonia, the Former Yugoslav Republic of": "North Macedonia",
    "Micronesia, Federated States of": "Micronesia",
}


def _display_country(name):
    return COUNTRY_DISPLAY_NAMES.get(name, name)


def _get_available_countries():
    if CITIES_COLLECTION is None:
        return []
    try:
        raw = CITIES_COLLECTION.distinct("country")
        seen = {}
        for c in raw:
            if c:
                display = _display_country(c)
                seen[display] = c  # display name → raw DB value
        items = sorted(seen.items())
        us = [i for i in items if i[1] == "United States"]
        rest = [i for i in items if i[1] != "United States"]
        return us + rest
    except PyMongoError:
        return []


def _normalize_profiles(profiles):
    if not profiles:
        return []

    field_bounds = {}
    for field in WEIGHT_FIELDS:
        values = [profile[field] for profile in profiles]
        field_bounds[field] = (min(values), max(values))

    normalized = []
    for profile in profiles:
        row = {"city": profile["city"]}
        for field in WEIGHT_FIELDS:
            min_value, max_value = field_bounds[field]
            if max_value > min_value:
                row[field] = (profile[field] - min_value) / (max_value - min_value)
            else:
                row[field] = 0.5
        for field in EXTRA_FIELDS:
            if field in profile:
                row[field] = profile[field]
        normalized.append(row)
    return normalized


def _request_json(url, *, params=None):
    try:
        response = requests.get(
            url,
            params=params,
            timeout=20,
            headers={
                "Accept": "application/json",
                "User-Agent": "habitatly/1.0",
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

    fields = "name,cou_name_en,coordinates,population,admin1_code"
    api_limit = 100  # max per API request

    candidates = []
    seen = set()
    offset = 0

    while len(candidates) < limit:
        batch_size = min(api_limit, limit - len(candidates))
        params = {"limit": batch_size, "offset": offset, "select": fields,
                  "where": where_expr, "order_by": "-population"}
        try:
            payload = _request_json(OPEN_DATA_CITIES_URL, params=params)
        except ImportSourceError:
            if offset == 0:
                raise
            break

        results = payload.get("results", [])
        if not results:
            break

        for row in results:
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

            dedup_key = (name.lower(), country.lower())
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            admin1 = (row.get("admin1_code") or "").strip()
            state = ""
            if country == "United States" and admin1 in US_STATE_CODES:
                state = US_STATE_CODES[admin1]

            candidates.append({
                "name": name,
                "country": country,
                "state": state,
                "latitude": lat,
                "longitude": lon,
                "population": int(population) if population is not None else None,
            })

        offset += len(results)

    return candidates[:limit]


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
    clean_values = [v for v in values if isinstance(v, (float, int))]
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


# ---- Teleport helpers ----

def _teleport_scores_to_db(raw):
    """Convert Teleport scores (0-10) to 0-100 DB fields."""
    def s(key):
        v = raw.get(key)
        return round(_clamp(v * 10, 0, 100), 1) if v is not None else None

    cost_raw = s("Cost of Living")
    cost_of_living_score = round(100.0 - (cost_raw or 50.0), 1)
    safety_score = s("Safety") or 50.0

    jobs_parts = [v for v in [s("Startups"), s("Economy"), s("Business Freedom")] if v is not None]
    jobs_score = round(sum(jobs_parts) / len(jobs_parts), 1) if jobs_parts else None

    env_parts = [v for v in [s("Environmental Quality"), s("Outdoors")] if v is not None]
    environment_score = round(sum(env_parts) / len(env_parts), 1) if env_parts else None

    mob_parts = [v for v in [s("Healthcare"), s("Leisure & Culture"), s("Education")] if v is not None]
    mobility_score = round(sum(mob_parts) / len(mob_parts), 1) if mob_parts else None

    affordability = 100.0 - (cost_raw or 50.0)
    quality_score = _quality_score(safety_score, affordability, jobs_score, environment_score, mobility_score)

    return {
        "cost_of_living_score": cost_of_living_score,
        "safety_score": safety_score,
        "jobs_score": jobs_score,
        "environment_score": environment_score,
        "mobility_score": mobility_score,
        "quality_score": quality_score,
    }


def _fetch_teleport_urban_areas(limit):
    data = _request_json(f"{TELEPORT_BASE}/urban_areas/")
    return data.get("_links", {}).get("ua:item", [])[:limit]


def _fetch_teleport_scores(slug):
    try:
        data = _request_json(f"{TELEPORT_BASE}/urban_areas/slug:{slug}/scores/")
        return {cat["name"]: cat["score_out_of_10"] for cat in data.get("categories", [])}
    except ImportSourceError:
        return {}


def _fetch_teleport_primary_city(slug):
    try:
        data = _request_json(f"{TELEPORT_BASE}/urban_areas/slug:{slug}/cities/")
        cities = data.get("_links", {}).get("ua:cities", [])
        if not cities:
            return None
        city_data = _request_json(cities[0]["href"])
        full_name = city_data.get("full_name", "")
        parts = [p.strip() for p in full_name.split(",")]
        name = parts[0] if parts else slug
        country = parts[-1] if len(parts) >= 2 else ""
        state = parts[1] if len(parts) >= 3 else ""
        latlon = city_data.get("location", {}).get("latlon", {})
        return {
            "name": name,
            "state": state,
            "country": country,
            "latitude": latlon.get("latitude"),
            "longitude": latlon.get("longitude"),
            "population": city_data.get("population"),
        }
    except (ImportSourceError, KeyError):
        return None


# ---- Routes ----

@app.route("/")
def index():
    city_count = 0
    if CITIES_COLLECTION is not None:
        try:
            city_count = CITIES_COLLECTION.count_documents({})
        except PyMongoError:
            pass
    countries = _get_available_countries()
    return render_template(
        "index.html",
        city_count=city_count,
        db_connected=CITIES_COLLECTION is not None,
        countries=countries,
    )


@app.post("/import/cities")
@app.post("/import/us-cities")
def import_cities():
    if CITIES_COLLECTION is None:
        return jsonify({"ok": False, "error": MONGO_ERROR or "MongoDB not configured"}), 503

    limit = _clamp(_parse_int(request.form, "limit", default=15), 1, 250)
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
            pass

        scores = _estimate_scores(city.get("population"), avg_temp_f)

        city_doc = {
            "external_id": (
                f"geonames:{city['name'].lower()}:{city['country'].lower()}:"
                f"{city['latitude']:.4f}:{city['longitude']:.4f}"
            ),
            "name": city["name"],
            "state": city["state"],
            "country": city["country"],
            "latitude": city["latitude"],
            "longitude": city["longitude"],
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
                "scoring": "estimated_from_population_and_temperature",
            },
            "updated_at": datetime.now(timezone.utc),
        }

        try:
            result = CITIES_COLLECTION.update_one(
                {"external_id": city_doc["external_id"]},
                {"$set": city_doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
                upsert=True,
            )
            if result.upserted_id is not None:
                summary["imported"] += 1
            else:
                summary["updated"] += 1
        except PyMongoError as exc:
            summary["failed"] += 1
            if len(summary["errors"]) < 5:
                summary["errors"].append(f"Mongo write failed: {exc.__class__.__name__}")

    if summary["imported"] == 0 and summary["updated"] == 0 and not summary["errors"]:
        summary["failed"] = limit
        summary["errors"].append("No city records were returned by the source API.")

    return jsonify(summary), 200


@app.post("/import/teleport")
def import_teleport_cities():
    if CITIES_COLLECTION is None:
        return jsonify({"ok": False, "error": MONGO_ERROR or "MongoDB not configured"}), 503

    limit = _clamp(_parse_int(request.form, "limit", default=50), 1, 260)

    summary = {
        "ok": True,
        "source": "teleport",
        "requested_limit": limit,
        "imported": 0,
        "updated": 0,
        "failed": 0,
        "errors": [],
    }

    try:
        urban_areas = _fetch_teleport_urban_areas(limit)
    except ImportSourceError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 502

    for ua in urban_areas:
        href = ua.get("href", "")
        slug = href.rstrip("/").split("/")[-1].replace("slug:", "")
        if not slug:
            summary["failed"] += 1
            continue

        raw_scores = _fetch_teleport_scores(slug)
        city_info = _fetch_teleport_primary_city(slug)

        if city_info is None:
            summary["failed"] += 1
            continue

        avg_temp_f = None
        if city_info.get("latitude") and city_info.get("longitude"):
            try:
                avg_temp_f = _fetch_annual_avg_temp_f(city_info["latitude"], city_info["longitude"])
            except WeatherAPIError:
                pass

        scores = _teleport_scores_to_db(raw_scores) if raw_scores else _estimate_scores(
            city_info.get("population"), avg_temp_f
        )

        city_doc = {
            "external_id": f"teleport:{slug}",
            "name": city_info["name"],
            "state": city_info.get("state", ""),
            "country": city_info["country"],
            "latitude": city_info["latitude"],
            "longitude": city_info["longitude"],
            "avg_temp_f": avg_temp_f,
            "population": city_info.get("population"),
            "safety_score": scores["safety_score"],
            "cost_of_living_score": scores["cost_of_living_score"],
            "jobs_score": scores["jobs_score"],
            "mobility_score": scores.get("mobility_score"),
            "environment_score": scores.get("environment_score"),
            "quality_score": scores["quality_score"],
            "data_sources": {
                "import": "teleport",
                "scoring": "teleport_quality_of_life" if raw_scores else "estimated",
            },
            "updated_at": datetime.now(timezone.utc),
        }

        try:
            result = CITIES_COLLECTION.update_one(
                {"external_id": city_doc["external_id"]},
                {"$set": city_doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
                upsert=True,
            )
            if result.upserted_id is not None:
                summary["imported"] += 1
            else:
                summary["updated"] += 1
        except PyMongoError as exc:
            summary["failed"] += 1
            if len(summary["errors"]) < 5:
                summary["errors"].append(str(exc.__class__.__name__))

    return jsonify(summary), 200


def _explain_match(breakdown):
    """Generate a one-liner explaining why a city matched."""
    if not breakdown:
        return ""
    sorted_cats = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
    top2 = [cat.lower() for cat, _ in sorted_cats[:2]]
    weakest_cat, weakest_val = sorted_cats[-1]
    if weakest_val >= 70:
        return f"A well-rounded pick — excels in {top2[0]} and {top2[1]}."
    if weakest_val >= 40:
        return f"Strong on {top2[0]} and {top2[1]}, with room to grow on {weakest_cat.lower()}."
    return f"Stands out for {top2[0]} and {top2[1]}, but {weakest_cat.lower()} lags behind."


@app.route("/results", methods=["GET", "POST"])
def results():
    weights = {field: _parse_weight(request.values, field) for field in WEIGHT_FIELDS}
    country_filter = request.values.get("country_filter", "").strip()

    profiles = _load_city_profiles_from_mongo(country_filter=country_filter or None) or CITY_PROFILES
    ranking_profiles = _normalize_profiles(profiles)
    weight_total = sum(weights.values())
    if weight_total <= 0:
        weight_total = len(WEIGHT_FIELDS)
        weights = {field: 1 for field in WEIGHT_FIELDS}

    city_results = []
    for profile in ranking_profiles:
        weighted_sum = sum(weights[field] * profile[field] for field in WEIGHT_FIELDS)
        score = (weighted_sum / weight_total) * 100.0

        breakdown = {FIELD_LABELS[f]: round(profile[f] * 100) for f in WEIGHT_FIELDS}
        sorted_cats = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
        top_strengths = [cat for cat, val in sorted_cats if val >= 55][:3]
        if not top_strengths:
            top_strengths = [sorted_cats[0][0]] if sorted_cats else []

        pop = profile.get("population")
        temp = profile.get("avg_temp_f")
        cost = profile.get("cost_of_living_score")

        if cost is None:
            cost_tier = None
        elif cost < 30:
            cost_tier = "Low"
        elif cost < 55:
            cost_tier = "Moderate"
        elif cost < 75:
            cost_tier = "High"
        else:
            cost_tier = "Very High"

        if pop is None:
            pop_display = None
        elif pop >= 1_000_000:
            pop_display = f"{pop / 1_000_000:.1f}M"
        elif pop >= 1_000:
            pop_display = f"{pop / 1_000:.0f}K"
        else:
            pop_display = str(int(pop))

        city_results.append({
            "city": profile["city"],
            "score": round(score, 2),
            "breakdown": breakdown,
            "strengths": top_strengths,
            "reason": _explain_match(breakdown),
            "population": pop_display,
            "avg_temp_f": f"{round(temp)}" if temp else None,
            "cost_tier": cost_tier,
            "latitude": profile.get("latitude"),
            "longitude": profile.get("longitude"),
        })

    city_results.sort(key=lambda row: row["score"], reverse=True)
    city_results = city_results[:20]

    map_data = [
        {"city": r["city"], "lat": r["latitude"], "lon": r["longitude"], "score": r["score"]}
        for r in city_results
        if r.get("latitude") and r.get("longitude")
    ]

    return render_template(
        "results.html",
        results=city_results,
        weights=weights,
        country_filter_raw=country_filter,
        map_data_json=json.dumps(map_data),
        country_filter=_display_country(country_filter) if country_filter else "",
        using_mongo=CITIES_COLLECTION is not None,
        db_error=MONGO_ERROR,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)
