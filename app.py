"""ASGI entrypoint for Uvicorn.

Usage: uvicorn app:app --host 0.0.0.0 --port $PORT --workers 1
"""

from asgiref.wsgi import WsgiToAsgi

from live_scanner_server import app as flask_app


app = WsgiToAsgi(flask_app)
