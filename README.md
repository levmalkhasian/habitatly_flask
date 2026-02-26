# Flask Base App

Minimal Flask starter with:
- base app file (`app.py`)
- template files (`templates/base.html`, `templates/index.html`)
- no routes yet

## Run

```bash
cd flask_base_app
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
flask --app app run --debug
```

Note: with no routes defined, `/` will return 404 until you add one.
