from django.core.management.base import BaseCommand
import pandas as pd
from dashboard.models import WildfireRecord

class Command(BaseCommand):
    help = "Import wildfire CSV into SQLite"

    def handle(self, *args, **kwargs):
        # Load CSV from GitHub (recommended)
        DATA_URL = "https://raw.githubusercontent.com/g-coronado/mln_data/refs/heads/main/fp-historical-wildfire-data-2006-2025.csv"
        df = pd.read_csv(DATA_URL)

        # Convert FIRE_START_DATE to datetime and drop invalid rows
        df['FIRE_START_DATE'] = pd.to_datetime(df['FIRE_START_DATE'], errors='coerce')
        df = df[df['FIRE_START_DATE'].notna()].copy()

        # Build model objects
        records = []
        for _, row in df.iterrows():
            records.append(WildfireRecord(
                FIRE_START_DATE=row['FIRE_START_DATE'],
                LATITUDE=row.get("LATITUDE"),
                LONGITUDE=row.get("LONGITUDE"),
                CURRENT_SIZE=row.get("CURRENT_SIZE"),
                SIZE_CLASS=row.get("SIZE_CLASS"),
                GENERAL_CAUSE=row.get("GENERAL_CAUSE"),
                TRUE_CAUSE=row.get("TRUE_CAUSE"),
                INDUSTRY_IDENTIFIER=row.get("INDUSTRY_IDENTIFIER"),
                ACTIVITY_CLASS=row.get("ACTIVITY_CLASS"),
                RESPONSIBLE_GROUP=row.get("RESPONSIBLE_GROUP"),
                FIRE_ORIGIN=row.get("FIRE_ORIGIN"),
                WEATHER_CONDITIONS_OVER_FIRE=row.get("WEATHER_CONDITIONS_OVER_FIRE"),
                TEMPERATURE=row.get("TEMPERATURE"),
                RELATIVE_HUMIDITY=row.get("RELATIVE_HUMIDITY"),
                WIND_SPEED=row.get("WIND_SPEED"),
                WIND_DIRECTION=row.get("WIND_DIRECTION"),
                DISPATCHED_RESOURCE=row.get("DISPATCHED_RESOURCE"),
            ))

        # Insert into DB
        WildfireRecord.objects.bulk_create(records, batch_size=5000)

        self.stdout.write(self.style.SUCCESS("CSV imported successfully (clean dates, no NaT rows)"))
