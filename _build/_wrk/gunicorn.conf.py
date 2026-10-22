"""Gunicorn config for the gateway.

4 worker processes — each has its own GIL, DB pool, and RMQ channel.
gthread worker class: each process handles concurrent requests via threads,
so DB/RMQ I/O releases the GIL and requests overlap.
"""

bind = "0.0.0.0:5100"
workers = 4               # 4 processes (2-4x CPU cores is typical)
worker_class = "gthread"   # threaded workers — best for I/O-bound Flask
threads = 16               # 16 threads per worker = 64 concurrent handlers
backlog = 4096             # OS listen queue — handles connection bursts
timeout = 90               # portfolio mode needs ~40s for Solscan API fan-out
keepalive = 5              # reuse HTTP connections
accesslog = "-"            # stdout
errorlog = "-"             # stdout
loglevel = "info"
preload_app = False        # each worker gets its own DB pool + RMQ connection
