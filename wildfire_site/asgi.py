"""
ASGI configuration for the wildfire_site project.

ASGI (Asynchronous Server Gateway Interface) serves as the entry point
for asynchronous web servers and applications. It enables support for
asynchronous request handling, WebSockets, and long-lived connections.

For more information, see:
https://docs.djangoproject.com/en/stable/howto/deployment/asgi/
"""

import os
from django.core.asgi import get_asgi_application

# Set the default Django settings module for the ASGI application.
# This tells Django which settings file to use when the server starts.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'wildfire_site.settings')

# Create the ASGI application instance that the web server will use
# to communicate with the Django project.
application = get_asgi_application()
