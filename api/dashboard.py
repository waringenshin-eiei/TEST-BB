# api/dashboard.py - Confirmed to work with your data structure
from http.server import BaseHTTPRequestHandler
import json
import os
import psycopg
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager

POSTGRES_URL = os.environ.get('POSTGRES_URL')
THAILAND_TZ = timezone(timedelta(hours=7))

@contextmanager
def get_db_connection():
    if not POSTGRES_URL: raise ConnectionError("Database URL is not configured.")
    conn = None
    try:
        conn = psycopg.connect(POSTGRES_URL)
        yield conn
    finally:
        if conn: conn.close()

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            query_params = parse_qs(urlparse(self.path).query)
            action = query_params.get('action', [None])[0]
            print(f"INFO: Dashboard API received action: '{action}'")

            if action == 'get_summary':
                self.handle_get_summary()
            elif action == 'get_requests':
                self.handle_get_requests(query_params)
            else:
                self._send_response(400, {"error": "Invalid action."})
        except Exception as e:
            print(f"ERROR: Unhandled exception in do_GET: {e}")
            self._send_response(500, {"error": "An unexpected server error occurred."})

    def handle_get_summary(self):
        summary = {}
        try:
            with get_db_connection() as conn:
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    today_start = datetime.now(THAILAND_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    cur.execute("SELECT COUNT(*) FROM blood_requests WHERE created_at >= %s;", (today_start,))
                    summary['requests_today'] = cur.fetchone()['count']
                    cur.execute("SELECT COUNT(*) FROM blood_requests WHERE status = 'submitted';")
                    summary['submitted_count'] = cur.fetchone()['count']
                    
                    cur.execute("""
                        SELECT blood_type as component_type, COUNT(*) as count 
                        FROM blood_requests WHERE status = 'submitted' AND blood_type IS NOT NULL
                        GROUP BY blood_type ORDER BY count DESC;
                    """)
                    summary['by_blood_type'] = cur.fetchall()

                    cur.execute("""
                        SELECT ward_name, COUNT(*) as count 
                        FROM blood_requests WHERE status = 'submitted' AND ward_name IS NOT NULL
                        GROUP BY ward_name ORDER BY count DESC LIMIT 10;
                    """)
                    summary['by_ward'] = cur.fetchall()

                    cur.execute("SELECT * FROM blood_requests WHERE status = 'submitted' ORDER BY created_at DESC LIMIT 5;")
                    summary['recent_requests'] = cur.fetchall()

            self._send_response(200, summary)
        except (ConnectionError, psycopg.Error) as e:
            self._send_response(503, {"error": f"Database service unavailable: {e}"})

    def handle_get_requests(self, params):
        try:
            page = int(params.get('page', [1])[0])
            limit = int(params.get('limit', [15])[0])
            status = params.get('status', [None])[0]
            search = params.get('search', [None])[0]
            offset = (page - 1) * limit

            query = "SELECT * FROM blood_requests"
            count_query = "SELECT COUNT(*) FROM blood_requests"
            
            conditions = []
            sql_params = []

            if status:
                conditions.append("status = %s")
                sql_params.append(status)
            if search:
                conditions.append("(patient_name ILIKE %s OR hospital_number ILIKE %s)")
                sql_params.extend([f"%{search}%", f"%{search}%"])

            if conditions:
                query += " WHERE " + " AND ".join(conditions)
                count_query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
            sql_params_with_pagination = sql_params + [limit, offset]

            with get_db_connection() as conn:
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    cur.execute(query, sql_params_with_pagination)
                    requests = cur.fetchall()
                    
                    cur.execute(count_query, sql_params)
                    total_records = cur.fetchone()['count']
            
            response = {
                "requests": requests, "total_records": total_records,
                "current_page": page, "total_pages": (total_records + limit - 1) // limit
            }
            self._send_response(200, response)
        except Exception as e:
            self._send_response(500, {"error": f"Error processing requests: {e}"})

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()
        
    def _send_response(self, status_code, body):
        def default_serializer(obj):
            if isinstance(obj, (datetime, timedelta)): return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False, default=default_serializer).encode('utf-8'))

    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
