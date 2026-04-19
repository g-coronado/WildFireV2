"""
Views for the dashboard application.

Handles HTTP requests and renders the wildfire dashboard template
with processed data from services.py. The build_dashboard_data()
function returns a complete context dictionary containing chart data,
map points, forecasts, and summary statistics.
"""

from django.shortcuts import render
from .services import build_dashboard_data


def dashboard_view(request):
    """
    Main dashboard view. Calls the data pipeline in services.py
    and passes the full context (charts, maps, forecasts, metrics)
    to the dashboard.html template for rendering.
    """
    context = build_dashboard_data()
    return render(request, 'dashboard/dashboard.html', context)
