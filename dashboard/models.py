"""
Database models for the dashboard application.

Defines the WildfireRecord model, which represents a single wildfire
event from the Alberta historical wildfire dataset (2006–2025).
Each record stores geographic, cause, weather, and response data
imported from the CSV source via the import_wildfire_csv management command.
"""

from django.db import models


class WildfireRecord(models.Model):
    """
    Represents a single wildfire record from the Alberta Open Data Portal.
    All fields are nullable to handle incomplete rows in the source CSV.
    """

    # --- Core identifier ---
    FIRE_START_DATE = models.DateField(null=True)               # Date the fire started

    # --- Geographic data ---
    LATITUDE = models.FloatField(null=True)                     # Fire location latitude
    LONGITUDE = models.FloatField(null=True)                    # Fire location longitude

    # --- Fire characteristics ---
    CURRENT_SIZE = models.FloatField(null=True)                 # Fire size in hectares
    SIZE_CLASS = models.CharField(max_length=50, null=True)     # Categorical size classification

    # --- Cause information ---
    GENERAL_CAUSE = models.CharField(max_length=200, null=True)     # High-level cause category
    TRUE_CAUSE = models.CharField(max_length=200, null=True)        # Specific cause of the fire
    INDUSTRY_IDENTIFIER = models.CharField(max_length=200, null=True)  # Related industry if applicable
    ACTIVITY_CLASS = models.CharField(max_length=200, null=True)    # Activity class at time of fire
    RESPONSIBLE_GROUP = models.CharField(max_length=200, null=True) # Group responsible for the fire
    FIRE_ORIGIN = models.CharField(max_length=200, null=True)       # Origin point description

    # --- Weather conditions at time of fire ---
    WEATHER_CONDITIONS_OVER_FIRE = models.CharField(max_length=200, null=True)  # Weather overview
    TEMPERATURE = models.FloatField(null=True)                  # Temperature (°C)
    RELATIVE_HUMIDITY = models.FloatField(null=True)            # Relative humidity (%)
    WIND_SPEED = models.FloatField(null=True)                   