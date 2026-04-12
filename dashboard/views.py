from django.shortcuts import render
from .services import build_dashboard_data


def dashboard_view(request):
    context = build_dashboard_data()
    return render(request, 'dashboard/dashboard.html', context)
