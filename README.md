# Habitatly (Flask)

Habitatly is a Flask app that ranks cities based on user-selected priorities:
- Cost of Living
- Safety
- Jobs
- Weather
- Lifestyle

Users set slider weights (`0` to `5`), then Habitatly computes a weighted score and returns ranked city matches.

## Features

- Slider-based weighting UI on `/`
- Ranked results page on `/results`
- Optional MongoDB-backed city data
- Built-in fallback sample city profiles when MongoDB is unavailable
- City import endpoint that pulls place + weather data and estimates scoring fields

## Requirements

- Python 3.10+
- `pip`
- Optional: MongoDB Atlas/local MongoDB instance

## Setup

```bash
cd Python/habitatly_flask
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set environment variables in `.env`:

```env
MONGODB_URI=mongodb+srv://<username>:<password>@<cluster-url>/?retryWrites=true&w=majority
MONGODB_DB_NAME=habitatly
```

MongoDB is optional. If `MONGODB_URI` is missing or connection fails, the app still runs and uses fallback data.

## Run

```bash
flask --app app run --debug
```

Open `http://127.0.0.1:5000`.

## Routes

- `GET /`  
  Home page with sliders and city-priority form.

- `POST /results`  
  Returns ranked city results from weighted factors.

- `POST /import/cities`  
  Imports city candidates (OpenDataSoft GeoNames) and weather (Open-Meteo), estimates scores, and upserts into MongoDB.

- `POST /import/us-cities`  
  Same as above, defaults country to United States when not provided.

- `POST /import/teleport`  
  Alias to the same import handler.

## Import Example

```bash
curl -X POST http://127.0.0.1:5000/import/us-cities \
  -d "limit=20"
```

Optional form fields:
- `limit` (1-100, default `15`)
- `country` (for `/import/cities`)

## Notes

- Slider weights are clamped to `0..5`.
- If all sliders are set to `0`, Habitatly falls back to equal weighting.
- Cost of Living prioritizes affordability (higher preference favors more affordable cities).
