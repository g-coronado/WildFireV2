"""
App configuration for the dashboard application.

Registers the 'dashboard' app with Django so it can be discovered
and included in the project via INSTALLED_APPS in settings.py.

For more information, see:
https://docs.djangoproject.com/en/stable/ref/applications/
"""

from django.apps import AppConfig


class DashboardConfig(AppConfig):
    # Default primary key type for models in this app.
    default_auto_field = 'django.db.models.BigAutoField'

    # Must match the app directory name and the entry in INSTALLED_APPS.
    name = 'dashboard'
