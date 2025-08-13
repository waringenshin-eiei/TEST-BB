# api/ward_validation.py - Refined with dynamic schedules and improved structure
from http.server import BaseHTTPRequestHandler
import json
import os
import psycopg
from urllib.parse import urlparse, parse_qs
from contextlib import contextmanager

# --- Configuration ---
POSTGRES_URL = os.environ.get('POSTGRES_URL')

# --- REFINEMENT: Centralized Database Connection Handling ---
@contextmanager
def database_cursor():
    """A context manager to handle database connection and cursor creation."""
    if not POSTGRES_URL:
        raise ConnectionError("Database URL is not configured.")
    conn = None
    try:
        conn = psycopg.connect(POSTGRES_URL)
        with conn.cursor() as cur:
            yield cur
    except psycopg.OperationalError as e:
        raise ConnectionError(f"Database connection failed: {e}") from e
    finally:
        if conn:
            conn.close()

# --- Main HTTP Handler ---
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Main router for GET requests."""
        try:
            query_params = parse_qs(urlparse(self.path).query)
            action = query_params.get('action', [None])[0]
            print(f"INFO: Received request for action: '{action}'")

            if action == 'search':
                query = query_params.get('query', [None])[0]
                self.handle_ward_search(query)
            elif action == 'list_all':
                self.handle_ward_list()
            # --- REFINEMENT: New action to fetch delivery schedules ---
            elif action == 'list_schedules':
                self.handle_schedule_list()
            else:
                self._send_response(400, {"error": "Invalid action. Use 'search', 'list_all', or 'list_schedules'."})
        except Exception as e:
            print(f"ERROR: Unhandled exception in do_GET: {e}")
            self._send_response(500, {"error": f"An unexpected server error occurred."})

    def handle_ward_search(self, query):
        """Handles autocomplete search for wards."""
        if not query:
            return self._send_response(400, {"error": "A 'query' parameter is required for search."})
        
        try:
            with database_cursor() as cur:
                cur.execute(
                    "SELECT ward_name FROM ward_directory WHERE ward_name ILIKE %s AND is_active = TRUE ORDER BY ward_name LIMIT 10",
                    (f"%{query}%",)
                )
                wards = [row[0] for row in cur.fetchall()]
                self._send_response(200, {"results": wards})
        except (ConnectionError, psycopg.Error) as e:
            print(f"ERROR: in handle_ward_search: {e}")
            self._send_response(503, {"error": "Database service is unavailable."})

    def handle_ward_list(self):
        """Handles request to list all active wards."""
        try:
            with database_cursor() as cur:
                cur.execute("SELECT ward_name FROM ward_directory WHERE is_active = TRUE ORDER BY ward_name")
                wards = [row[0] for row in cur.fetchall()]
                self._send_response(200, {"wards": wards})
        except (ConnectionError, psycopg.Error) as e:
            print(f"ERROR: in handle_ward_list: {e}")
            self._send_response(503, {"error": "Database service is unavailable."})

    def handle_schedule_list(self):
        """Handles request to list all active delivery schedules."""
        try:
            with database_cursor() as cur:
                # Assumes you have a 'delivery_schedules' table from init_db.py
                cur.execute("SELECT delivery_time FROM delivery_schedules WHERE is_active = TRUE ORDER BY delivery_time")
                schedules = [row[0] for row in cur.fetchall()]
                self._send_response(200, {"schedules": schedules})
        except (ConnectionError, psycopg.Error) as e:
            print(f"ERROR: in handle_schedule_list: {e}")
            self._send_response(503, {"error": "Database service is unavailable."})

    def do_OPTIONS(self):
        """Handles CORS preflight requests."""
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()
        
    def _send_response(self, status_code, body):
        """Sends a JSON response with appropriate headers."""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False).encode('utf-8'))

    def _send_cors_headers(self):
        """Sends CORS headers."""
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
