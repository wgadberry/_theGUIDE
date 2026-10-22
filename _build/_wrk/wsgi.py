"""WSGI entry point for gunicorn.

Usage:
    gunicorn -c gunicorn.conf.py wsgi:app
"""
import importlib

# guide-gateway.py has a hyphen — can't use normal import
mod = importlib.import_module("guide-gateway")
app = mod.create_app()
