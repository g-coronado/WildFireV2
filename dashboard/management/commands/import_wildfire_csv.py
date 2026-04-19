"""
Management command to import wildfire CSV data into the SQLite database.

Fetches the historical wildfire dataset from a remote GitHub repository,
cleans invalid date entries, maps each row to a WildfireRecord model
instance, and bulk-inserts all records into the database.

Data source: Alberta Open Data Portal
https://open.alberta.ca/opendata/wildfire-data
"""

from django.core.management.base import BaseCommand
import pandas as pd
from dashboard.models import WildfireRecord


class Command(BaseCommand):
    # Short description shown when running `python manage.py help`
    help = "Import wildfire CSV into SQLite"

    def handle(self, *args, **kwargs):

        # --- 1. LOAD CSV FROM REMOTE SOURCE ---
        # Fetch the dataset directly from the GitHub-hosted CSV file
        DATA_URL = "https://raw.githubusercontent.com/g-coronado/mln_data/refs/heads/main/fp-historical-wildfire-data-2006-2025.csv"
        df = pd.read_csv(DATA_URL)

        # --- 2. CLEAN DATE COLUMN ---
        # Convert to datetime; 'coerce' turns unparseable values into NaT
        df['FIRE_START_DATE'] = pd.to_datetime(df['FIRE_START_DATE'], errors='coerce')
        # Drop rows with invalid dates; .copy() avoids SettingWithCopyWarning
        df = df[df['FIRE_START_DATE'].notna()].copy()

        # --- 3. BUILD MODEL OBJECTS ---
        # Iterate through each cleaned row and create a WildfireRecord instance
        # .get() returns None gracefully if a column is missing from the CSV
        records = []
        for _, row in df.iterrows():
            records.append(WildfireRecord(
                FIRE_START_DATE=row['FIRE_START_DATE'],         # Date the fire started
                LATITUDE=row.get("LATITUDE"),                   # Fire location latitude
                LONGITUDE=row.get("LONGITUDE"),                 # Fire location longitude
                CURRENT_SIZE=row.get("CURRENT_SIZE"),           # Fire size in hectares
                SIZE_CLASS=row.get("SIZE_CLASS"),                # Categorical size classification
                GENERAL_CAUSE=row.get("GENERAL_CAUSE"),         # High-level cause category
                TRUE_CAUSE=row.get("TRUE_CAUSE"),               # Specific cause of the fire
                INDUSTRY_IDENTIFIER=row.get("INDUSTRY_IDENTIFIER"),   # Related industry if applicable
                ACTIVITY_CLASS=row.get("ACTIVITY_CLASS"),        # Activity class at time of fire
                RESPONSIBLE_GROUP=row.get("RESPONSIBLE_GROUP"),  # Group responsible for the fire
                FIRE_ORIGIN=row.get("FIRE_ORIGIN"),             # Origin point description
                WEATHER_CONDITIONS_OVER_FIRE=row.get("WEATHER_CONDITIONS_OVER_FIRE"),  # Weather overview
                TEMPERATURE=row.get("TEMPERATURE"),             # Temperature (°C)
                RELATIVE_HUMIDITY=row.get("RELATIVE_HUMIDITY"),  # Relative humidity (%)
                WIND_SPEED=row.get("WIND_SPEED"),               # Wind speed (km/h)
                WIND_DIRECTION=row.get("WIND_DIRECTION"),       # Wind direction (degrees)
                DISPATCHED_RESOURCE=row.get("DISPATCHED_RESOURCE"),  # Resources dispatched to the fire
            ))

        # --- 4. BULK INSERT INTO DATABASE ---
        # batch_size=5000 limits memory usage by inserting in chunks
        WildfireRecord.objects.bulk_create(records, batch_size=5000)

        # --- 5. CONFIRMATION OUTPUT ---
        self.stdout.write(self.style.SUCCESS("CSV imported successfully (clean dates, no NaT rows)"))
