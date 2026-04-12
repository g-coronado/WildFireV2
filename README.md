# Wildfire Django Dashboard

This is a simple Django website version of your notebook.

## What it does
- Loads the wildfire CSV from GitHub
- Cleans the data
- Creates zones
- Shows charts in a web dashboard
- Trains a simple Random Forest model
- Forecasts the next month by zone

## Run locally

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver
```

Open:

```bash
http://127.0.0.1:8000/
```

## Simplest hosting path

### Option 1: Render
1. Put this folder into a GitHub repo.
2. Create a new Web Service on Render.
3. Connect the GitHub repo.
4. Render will detect `render.yaml`.
5. Deploy.

After deployment, set `ALLOWED_HOSTS` to your Render domain.

## Important note
Right now this app recalculates the dashboard from the CSV source. That is the simplest setup.
For a future upgrade, you could save cleaned data into a database or cache results.
