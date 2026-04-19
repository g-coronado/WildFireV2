# 🔥 Alberta Wildfire Dashboard — Django Project

A data-driven wildfire monitoring and prediction dashboard built with Django, SQLite, and interactive frontend visualizations. The application ingests wildfire CSV data, processes and cleans it through a structured pipeline, generates predictions using a machine learning model, and presents insights through interactive maps and charts.

### 🌐 [Live Demo — wildfirev2.onrender.com](https://wildfirev2.onrender.com/)

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Data Source](#data-source)
- [Folder Structure](#folder-structure)
- [Setup Instructions](#setup-instructions)
- [CSV to SQLite Workflow](#csv-to-sqlite-workflow)
- [Running the Project Locally](#running-the-project-locally)
- [Deployment](#deployment)
- [License](#license)

---

## Project Overview

This project delivers a full-stack wildfire dashboard that transforms raw CSV wildfire data into actionable insights. The pipeline covers data ingestion, cleaning, storage, prediction, and visualization — all within a single Django application backed by SQLite.

**Key workflow:**

1. Raw wildfire CSV data is imported into SQLite via a custom Django management command (`import_wildfire_csv.py`).
2. `services.py` handles data cleaning, transformation, zone assignment, cause consolidation, and prediction model execution.
3. `views.py` passes the processed data as context to the frontend.
4. `dashboard.html` renders interactive maps (Leaflet), a heatmap layer (Leaflet.heat), and charts (Chart.js) for analysis and monitoring.

---

## Features

- **CSV Data Import** — Automated ingestion of wildfire datasets through a Django management command.
- **Data Cleaning & Processing** — Centralized data processing logic in `services.py` for validation, transformation, zone assignment, and cause consolidation.
- **Geographic Zone Classification** — Records are assigned to five Alberta zones (South, Central West, Central East, North West, North East) based on latitude/longitude.
- **Prediction Model** — Machine learning model (RandomForestRegressor / scikit-learn) with lag-based feature engineering for next-month wildfire forecasting by zone.
- **Interactive Heatmap** — Leaflet.heat layer displaying 5-year historical wildfire density across Alberta.
- **Risk Forecast Map** — Circle markers sized and colored by predicted risk level (Low, Likely, Very Likely) for each zone.
- **Dynamic Charts** — Chart.js visualizations with dynamic green-to-red color gradients for causes, zones, monthly trends, area burned, weather conditions, and forecasts.
- **Forecast Table** — Zone-by-zone prediction table with model performance metrics (RMSE, MAE, R²).
- **Lightweight Database** — SQLite for zero-configuration, file-based storage suitable for development and small-scale deployment.
- **Production-Ready** — WhiteNoise for static file serving, automatic DEBUG toggle, and Render deployment configuration.

---

## Tech Stack

| Layer           | Technology                           |
|-----------------|--------------------------------------|
| Backend         | Python 3.x, Django                   |
| Database        | SQLite                               |
| Data Processing | pandas, NumPy, services.py           |
| ML Model        | scikit-learn (RandomForestRegressor) |
| Frontend        | HTML, CSS, JavaScript                |
| Mapping         | Leaflet.js, Leaflet.heat             |
| Charts          | Chart.js                             |
| Static Files    | WhiteNoise                           |
| Data Source     | CSV (Alberta wildfire datasets)      |
| Hosting         | Render                               |

---

## Data Source

The wildfire dataset used in this project is sourced from the **Government of Alberta Open Data Portal**:

- **Source page:** [Alberta Open Data — Wildfire Data](https://open.alberta.ca/opendata/wildfire-data)
- **CSV repository:** [fp-historical-wildfire-data-2006-2025.csv](https://raw.githubusercontent.com/g-coronado/mln_data/refs/heads/main/fp-historical-wildfire-data-2006-2025.csv)

The dataset contains historical wildfire records across Alberta from 2006 to 2025, including geographic coordinates, fire sizes, cause classifications, weather conditions, and response data.

---

## Folder Structure
wildfire-dashboard/
├── manage.py                              # Django project entry point
├── requirements.txt                       # Python dependencies
├── runtime.txt                            # Python version for deployment
├── db.sqlite3                             # SQLite database (generated after migration)
├── README.md
│
├── wildfire_site/                         # Django project configuration
│   ├── init.py
│   ├── asgi.py                            # ASGI entry point (async server support)
│   ├── settings.py                        # Project settings (DB, static, security, middleware)
│   ├── urls.py                            # Root URL routing
│   ├── wsgi.py                            # WSGI entry point (production server interface)
│   └── build.sh                           # Deployment build script for Render
│
├── dashboard/                             # Main Django application
│   ├── init.py
│   ├── apps.py                            # App registration with Django
│   ├── models.py                          # WildfireRecord model (17 fields per record)
│   ├── views.py                           # Dashboard view — passes context to template
│   ├── services.py                        # Data pipeline: cleaning, ML model, chart/map data
│   ├── management/
│   │   └── commands/
│   │       └── import_wildfire_csv.py     # Management command to load CSV into SQLite
│   ├── migrations/                        # Django database migrations
│   ├── templates/
│   │   └── dashboard/
│   │       └── dashboard.html             # Main template (maps, charts, tables)
│   └── static/
│       └── dashboard/
│           └── style.css                  # Custom dashboard styling
│
└── staticfiles/                           # Collected static files (generated by collectstatic)


---

## Setup Instructions

### Prerequisites

- Python 3.9+
- pip
- Git

### Clone, Install, and Run

```bash
# 1. Clone the Repository
git clone https://github.com/g-coronado/wildfire-dashboard.git
cd wildfire-dashboard

# 2. Create and Activate a Virtual Environment
python -m venv venv
venv\Scripts\activate              # Windows
# source venv/bin/activate         # macOS / Linux

# 3. Install Dependencies
pip install -r requirements.txt

# 4. Apply Database Migrations
python manage.py makemigrations
python manage.py migrate

# 5. Import CSV Data into SQLite
python manage.py import_wildfire_csv

# 6. Collect Static Files
python manage.py collectstatic --no-input

# 7. Run the Development Server
python manage.py runserver

# 8. Open in browser
# http://127.0.0.1:8000/


