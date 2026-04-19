"""
Django settings for the wildfire_site project.

This module contains all configuration for the Django application,
including security, database, static files, and deployment settings.
Automatically adapts between local development and Render hosting
based on environment variables.

For the full list of settings and their values, see:
https://docs.djangoproject.com/en/stable/ref/settings/
"""

from pathlib import Path
import os

# Build the absolute path to the project root directory.
# This is used as a reference point for file paths throughout the settings.
BASE_DIR = Path(__file__).resolve().parent.parent

# --- SECURITY ---
# Load the secret key from environment variables for production.
# Falls back to a default key for local development only.
SECRET_KEY = os.environ.get('SECRET_KEY', 'python-groupGHJ-secret-key')

# --- DEBUG MODE ---
# Automatically disables DEBUG when deployed on Render.
# Render sets the RENDER environment variable, so its presence turns DEBUG off.
DEBUG = 'RENDER' not in os.environ

# --- ALLOWED HOSTS ---
# Defines which hostnames Django will accept requests from.
# Starts with localhost for development, then appends the Render hostname
# if the app is deployed.
ALLOWED_HOSTS = ['localhost', '127.0.0.1']
RENDER_EXTERNAL_HOSTNAME = os.environ.get('RENDER_EXTERNAL_HOSTNAME')
if RENDER_EXTERNAL_HOSTNAME:
    ALLOWED_HOSTS.append(RENDER_EXTERNAL_HOSTNAME)

# --- CSRF PROTECTION ---
# Trusted origins for Cross-Site Request Forgery protection.
# Required for POST requests (forms) to work on the deployed Render domain.
CSRF_TRUSTED_ORIGINS = []
if RENDER_EXTERNAL_HOSTNAME:
    CSRF_TRUSTED_ORIGINS.append(f"https://{RENDER_EXTERNAL_HOSTNAME}")

# --- INSTALLED APPS ---
# Django built-in apps plus the custom 'dashboard' app for wildfire data.
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'dashboard',                      # Wildfire dashboard application
]

# --- MIDDLEWARE ---
# Request/response processing pipeline. Order matters.
MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',  # Serves static files in production without a CDN
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

# Points Django to the project-level URL configuration module.
ROOT_URLCONF = 'wildfire_site.urls'

# --- TEMPLATES ---
# Configuration for Django's template engine.
# APP_DIRS=True tells Django to look for templates inside each app's templates/ folder.
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],                   # No project-level template directories
        'APP_DIRS': True,             # Auto-discovers templates in app directories
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

# Entry point for WSGI-compatible web servers (used by Render).
WSGI_APPLICATION = 'wildfire_site.wsgi.application'

# --- DATABASE ---
# Uses SQLite as a lightweight, file-based database.
# The db.sqlite3 file is stored in the project root directory.
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

# Password validation is disabled (not required for this data-focused project).
AUTH_PASSWORD_VALIDATORS = []

# --- INTERNATIONALIZATION ---
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'America/Edmonton'        # Alberta time zone for wildfire data context
USE_I18N = True                       # Enable Django's translation framework
USE_TZ = True                         # Store datetimes as timezone-aware in the database

# --- STATIC FILES ---
# URL path prefix for serving static assets (CSS, JS, images).
STATIC_URL = '/static/'

# Directory where `collectstatic` gathers all static files for production.
STATIC_ROOT = BASE_DIR / 'staticfiles'

# WhiteNoise compresses and caches static files for efficient production serving.
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

# Default primary key type for Django models.
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
