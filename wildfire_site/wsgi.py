"""
WSGI configuration for the wildfire_site project.

WSGI (Web Server Gateway Interface) serves as the entry point for
synchronous web servers to communicate with the Django application.
This is the interface used by Render and other production hosting
platforms to serve the application.

For more information, see:
https://docs.djangoproject.com/en/stable/howto/deployment/wsgi/
"""

import os
from django.core.wsgi import get_wsgi_application

# Set the default Django settings module for the WSGI application.
# This tells Django which settings file to use when the server starts.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'wildfire_site.settings')

# Create the WSGI application instance that the web server will use
# to handle incoming HTTP requests and pass them to Django.
application = get_wsgi_application()
