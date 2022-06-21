# Reference: http://docs.gunicorn.org/en/stable/configure.html#configuration-file
import multiprocessing
from os import environ


# http://docs.gunicorn.org/en/stable/settings.html#workers
# workers = multiprocessing.cpu_count() * 2 + 1
workers = 2

# http://docs.gunicorn.org/en/stable/settings.html#bind
bind = "0.0.0.0:8080"

# http://docs.gunicorn.org/en/stable/settings.html#worker-class
worker_class = "gevent"
# worker_class = "sync"

# http://docs.gunicorn.org/en/stable/settings.html#worker-connections
worker_connections = 1001

# http://docs.gunicorn.org/en/stable/settings.html#timeout
timeout = environ["TIMEOUT"]
