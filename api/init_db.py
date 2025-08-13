# api/init_db.py - Optimized for Performance and Scalability
from http.server import BaseHTTPRequestHandler
import json
import os
import psycopg
from contextlib import contextmanager

# --- Configuration ---
POSTGRES_URL = os.environ.get('POSTGRES_URL')

# --- Database Connection Manager ---
@contextmanager
def get_db_connection():
    """Provides a database connection that is automatically closed."""
    if not POSTGRES_URL:
        raise ConnectionError("Database URL (POSTGRES_URL) is not configured.")
    conn = None
    try:
        conn = psycopg.connect(POSTGRES_URL)
        yield conn
    finally:
        if conn:
            conn.close()

# --- Main HTTP Handler ---
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handles GET request to initialize or update the database schema."""
        try:
            message = self.initialize_database()
            status_code = 200
        except (ConnectionError, psycopg.Error) as e:
            message = f"Failed to initialize database: {e}"
            status_code = 503 # Service Unavailable
        
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(json.dumps({"status": message}).encode('utf-8'))

    def initialize_database(self):
        """Creates or updates all necessary tables and populates default data."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                print("INFO: Starting database schema initialization...")
                
                # --- Schema Definitions ---
                commands = [
                    # OPTIMIZATION: Use UUID for non-sequential, unique IDs.
                    # This is better for distributed systems and security than VARCHAR.
                    """
                    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                    """,
                    # OPTIMIZATION: Use ENUM types for fixed sets of values (e.g., statuses).
                    # This improves data integrity and is more space-efficient than VARCHAR.
                    """
                    DO $$ BEGIN
                        CREATE TYPE user_status AS ENUM ('active', 'inactive', 'suspended');
                    EXCEPTION
                        WHEN duplicate_object THEN null;
                    END $$;
                    """,
                    """
                    DO $$ BEGIN
                        CREATE TYPE request_status AS ENUM ('pending', 'confirmed', 'in_progress', 'delivered', 'cancelled');
                    EXCEPTION
                        WHEN duplicate_object THEN null;
                    END $$;
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                        line_user_id VARCHAR(255) UNIQUE NOT NULL, -- The external ID from LINE
                        status user_status DEFAULT 'active',
                        first_seen TIMESTAMPTZ DEFAULT NOW(),
                        last_activity TIMESTAMPTZ DEFAULT NOW()
                    );
                    """,
                    """
                    -- A canonical list of all valid ward names.
                    -- This is our central source of truth for wards.
                    CREATE TABLE IF NOT EXISTS ward_directory (
                        ward_id SERIAL PRIMARY KEY,
                        ward_name VARCHAR(255) UNIQUE NOT NULL,
                        is_active BOOLEAN DEFAULT TRUE
                    );
                    """,
                    """
                    -- Stores user-specific settings, like their primary ward.
                    -- Renamed from 'ward_id' for clarity.
                    CREATE TABLE IF NOT EXISTS user_profiles (
                        user_id UUID PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                        -- OPTIMIZATION: Use an integer foreign key to the ward directory.
                        primary_ward_id INTEGER REFERENCES ward_directory(ward_id),
                        reporter_name VARCHAR(255)
                    );
                    """,
                    """
                    -- A canonical list of all valid delivery schedules.
                    CREATE TABLE IF NOT EXISTS delivery_schedules (
                        schedule_id SERIAL PRIMARY KEY,
                        delivery_time VARCHAR(10) UNIQUE NOT NULL, -- Keeping VARCHAR for "10.00น." format
                        is_active BOOLEAN DEFAULT TRUE
                    );
                    """,
                    """
                    -- The main transactional table for blood requests.
                    CREATE TABLE IF NOT EXISTS blood_requests (
                        request_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                        user_id UUID NOT NULL REFERENCES users(user_id),
                        -- OPTIMIZATION: Use integer foreign keys for faster joins and data integrity.
                        ward_id INTEGER NOT NULL REFERENCES ward_directory(ward_id),
                        schedule_id INTEGER NOT NULL REFERENCES delivery_schedules(schedule_id),
                        
                        blood_type VARCHAR(50),
                        patient_name VARCHAR(255),
                        hospital_number VARCHAR(100),
                        blood_details TEXT,
                        delivery_location VARCHAR(255), -- Could be different from the ward
                        reporter_name VARCHAR(255),
                        status request_status DEFAULT 'pending',
                        request_data JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW() -- Will be managed by a trigger
                    );
                    """,
                    """
                    -- Child table for the components of a single blood request.
                    CREATE TABLE IF NOT EXISTS blood_components (
                        component_id SERIAL PRIMARY KEY,
                        request_id UUID NOT NULL REFERENCES blood_requests(request_id) ON DELETE CASCADE,
                        component_type VARCHAR(50) NOT NULL,
                        quantity INTEGER,
                        component_subtype VARCHAR(100),
                        properties JSONB
                    );
                    """,
                    # --- INDEXING FOR PERFORMANCE ---
                    # Indexes on foreign keys and frequently queried columns are critical for speed.
                    """
                    CREATE INDEX IF NOT EXISTS idx_users_line_user_id ON users(line_user_id);
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_blood_requests_user_id ON blood_requests(user_id);
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_blood_requests_status ON blood_requests(status);
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_blood_requests_created_at ON blood_requests(created_at DESC);
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS idx_blood_components_request_id ON blood_components(request_id);
                    """,
                    # OPTIMIZATION: Add a GIN index for fast searching within the JSONB data.
                    """
                    CREATE INDEX IF NOT EXISTS idx_blood_requests_request_data_gin ON blood_requests USING GIN(request_data);
                    """,
                    
                    # --- TRIGGER for automatically updating 'updated_at' timestamp ---
                    """
                    CREATE OR REPLACE FUNCTION trigger_set_timestamp()
                    RETURNS TRIGGER AS $$
                    BEGIN
                      NEW.updated_at = NOW();
                      RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    """,
                    """
                    DROP TRIGGER IF EXISTS set_timestamp ON blood_requests;
                    CREATE TRIGGER set_timestamp
                    BEFORE UPDATE ON blood_requests
                    FOR EACH ROW
                    EXECUTE PROCEDURE trigger_set_timestamp();
                    """
                ]

                # Execute all schema creation commands
                for command in commands:
                    cur.execute(command)
                print("INFO: All tables, types, indexes, and triggers created or verified successfully.")

                # Populate tables with default data
                self.insert_default_data(cur)
                
                conn.commit()
                print("INFO: Database initialization complete and data committed.")
                return "Database initialized successfully."

    def insert_default_data(self, cur):
        """Inserts default wards and schedules if they don't exist."""
        
        # --- Default Ward Data ---
        wards_data = [
            ("2ก",), ("2ข",), ("IMC 2ค",), ("NICU",), ("2ง",), ("IMC 2ง",), ("2ฉ",), 
            ("3ก",), ("3ข",), ("3ค",), ("3ง",), ("3จ",), ("IMC 3จ",), ("3ฉ",), 
            ("4ก",), ("4ข1",), ("4ข2",), ("4ข3",), ("4ค",), ("4ง",), ("5ก",), ("5ข",), 
            ("5ค",), ("5ง",), ("5จ",), ("6ก",), ("6ข",), ("6จ",), ("OPD AE",), 
            ("AE1",), ("AE2",), ("AE3",), ("AE4",), ("SICU1",), ("SICU2",), ("SICU3",), 
            ("NSICU",), ("Burn Unit",), ("CCU",), ("PICU",), ("MICU 1",), ("MICU 2",), 
            ("MICU 3",), ("CVT-ICU",), ("SCTU 1",), ("หอสงฆ์อาพาธ",), ("8B",), ("8C",), 
            ("9A",), ("9B",), ("9C",), ("สว 11",), ("สว 12",), ("สว 13",), ("สว 14",), 
            ("สว 15",), ("กว. 6/1",), ("กว. 6/2",), ("กว. 7/1",), ("ห้องคลอด",), 
            ("ห้องให้เลือดผู้ป่วยนอก",), ("ไตเทียม",), ("ห้อง x-ray",), ("Endoscope สว.ชั้น4",)
        ]
        insert_ward_sql = "INSERT INTO ward_directory (ward_name) VALUES (%s) ON CONFLICT (ward_name) DO NOTHING;"
        cur.executemany(insert_ward_sql, wards_data)
        print(f"INFO: Verified {len(wards_data)} wards. {cur.rowcount} new wards inserted.")

        # --- Default Delivery Schedule Data ---
        delivery_times = [
            ("10.00น.",), ("11.00น.",), ("12.00น.",), ("13.00น.",), ("14.00น.",), 
            ("15.00น.",), ("16.00น.",), ("18.00น.",), ("20.00น.",), ("21.00น",), ("23.00น.",)
        ]
        insert_schedule_sql = "INSERT INTO delivery_schedules (delivery_time) VALUES (%s) ON CONFLICT (delivery_time) DO NOTHING;"
        cur.executemany(insert_schedule_sql, delivery_times)
        print(f"INFO: Verified {len(delivery_times)} delivery schedules. {cur.rowcount} new schedules inserted.")
