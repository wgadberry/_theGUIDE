"""Gunicorn config for ChromaDB Search API."""
bind = '0.0.0.0:5300'
workers = 2
threads = 4
worker_class = 'gthread'
timeout = 30
accesslog = '-'
errorlog = '-'
loglevel = 'info'
