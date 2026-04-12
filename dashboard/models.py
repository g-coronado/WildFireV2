from django.db import models

class WildfireRecord(models.Model):
    FIRE_START_DATE = models.DateField(null=True)
    LATITUDE = models.FloatField(null=True)
    LONGITUDE = models.FloatField(null=True)
    CURRENT_SIZE = models.FloatField(null=True)
    SIZE_CLASS = models.CharField(max_length=50, null=True)
    GENERAL_CAUSE = models.CharField(max_length=200, null=True)
    TRUE_CAUSE = models.CharField(max_length=200, null=True)
    INDUSTRY_IDENTIFIER = models.CharField(max_length=200, null=True)
    ACTIVITY_CLASS = models.CharField(max_length=200, null=True)
    RESPONSIBLE_GROUP = models.CharField(max_length=200, null=True)
    FIRE_ORIGIN = models.CharField(max_length=200, null=True)
    WEATHER_CONDITIONS_OVER_FIRE = models.CharField(max_length=200, null=True)
    TEMPERATURE = models.FloatField(null=True)
    RELATIVE_HUMIDITY = models.FloatField(null=True)
    WIND_SPEED = models.FloatField(null=True)
    WIND_DIRECTION = models.CharField(max_length=50, null=True)
    DISPATCHED_RESOURCE = models.CharField(max_length=200, null=True)

    class Meta:
        db_table = "wildfire_data"
