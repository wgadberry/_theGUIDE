#!/usr/bin/env python3
"""WSGI entrypoint for gunicorn."""
import importlib
mod = importlib.import_module('chroma-search-api')
app = mod.create_app()
