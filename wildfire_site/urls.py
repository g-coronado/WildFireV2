"""
URL configuration for the wildfire_site project.

Defines the top-level URL routes for the application.
All incoming requests are matched against these patterns
and routed to the corresponding view function.

For more information, see:
https://docs.djangoproject.com/en/stable/topics/http/urls/
"""

from django.contrib import admin
from django.urls import path
from dashboard.views import dashboard_view  # Main dashboard view from the dashboard app

# --- URL PATTERNS ---
# Maps URL paths to their corresponding views.
# Django evaluates these patterns top-to-bottom and stops at the first match.
urlpatterns = [
    path('admin/', admin.site.urls),            # Django admin panel (http://host/admin/)
    path('', dashboard_view, name='dashboard'),  # Root URL serves the wildfire dashboard
]
