from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import pyodbc
from loguru import logger

# Log file configuration
LOG_FILE = "logs/db_inserts.log"

# Batch size for insertion
BATCH_SIZE = 10000

# Switch for enabling/disabling terminal logging
DEV_MODE = os.getenv("DEV_MODE", "False").lower() == "true"

# Configure log rotation and file logging
if not DEV_MODE:
    logger.add(
        LOG_FILE,
        rotation="5 MB",          # Rotate after 5 MB
        retention="7 days",       # Keep logs for 7 days
        compression="zip",        # Compress old logs
        level="DEBUG",            # Log level for the file
        mode="a",                 # Open the log file in append mode
        encoding="utf-8"          # Ensure the log file encoding is correct
    )

app = Flask(__name__)
CORS(app)

app.json.sort_keys = False

# Database configuration (use environment variables for security)
DB_CONFIG = {
    "driver": os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    "server": os.getenv("DB_SERVER", "localhost"),
    "database": os.getenv("DB_DATABASE", "masking"),
    "uid": os.getenv("DB_UID", "pcuser"),
    "pwd": os.getenv("DB_PWD", "pcuser")
}

# Function to validate table names
def validate_table_name(table_name):
    if not table_name.isidentifier():
        raise ValueError(f"Invalid table name: {table_name}")

# Function to connect to the database
def connect_to_db():
    try:
        connection = pyodbc.connect(
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['uid']};"
            f"PWD={DB_CONFIG['pwd']}"
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

# Function to truncate a table
def truncate_table(connection, table_name):
    validate_table_name(table_name)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            connection.commit()
            logger.info(f"Table {table_name} truncated successfully.")
    except Exception as e:
        logger.error(f"Error truncating table {table_name}: {e}")
        connection.rollback()
        raise

# Function to insert data into a table in batches
def insert_data_in_batches(connection, table_name, rows):
    validate_table_name(table_name)
    logger.info(f"Inserting data into table: {table_name}")
    if not rows:
        logger.warning(f"No data to insert into {table_name}.")
        return {"table_name": table_name, "inserted": 0, "duplicates": 0}

    columns = rows[0].keys()
    column_names = ", ".join(columns)
    placeholders = ", ".join(["?"] * len(columns))
    insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

    inserted_count = 0
    duplicate_count = 0

    try:
        with connection.cursor() as cursor:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                values = [tuple(row.values()) for row in batch]
                for value in values:
                    try:
                        cursor.execute(insert_query, value)
                        connection.commit()
                        inserted_count += 1
                    except pyodbc.IntegrityError as e:
                        duplicate_count += 1
                        logger.warning(f"Duplicate key error: {e}")
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise

    return {"table_name": table_name, "inserted": inserted_count, "duplicates": duplicate_count}

# Table processor function
def process_table(connection, table_data):
    table_name = table_data.get("table_name")
    truncate = table_data.get("truncate_table", False)
    rows = table_data.get("columns", [])

    if not table_name or not isinstance(rows, list):
        raise ValueError("Invalid table data. 'table_name' must be a string and 'columns' must be a list.")

    if truncate:
        truncate_table(connection, table_name)

    return insert_data_in_batches(connection, table_name, rows)

# Core insert method for external or API use
def insert_records_method(data):
    if not data or not isinstance(data, dict):
        raise ValueError("Invalid data. Must be a dictionary.")
    
    # logger.debug(data)

    parent_tables = data.get("parent_tables", [])
    child_tables = data.get("child_tables", [])

    connection = connect_to_db()
    if not connection:
        raise ConnectionError("Failed to connect to the database.")

    try:
        logger.info("Processing parent tables...")
        parent_results = []
        for table_data in parent_tables:
            parent_results.append(process_table(connection, table_data))

        logger.info("Processing child tables...")
        child_results = []
        for table_data in child_tables:
            child_results.append(process_table(connection, table_data))

        logger.info("Data insertion completed successfully.")
        return {"parent_results": parent_results, "child_results": child_results}
    finally:
        connection.close()
        logger.info("Database connection closed.")

@app.route('/insert', methods=['POST'])
def insert_records():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "Invalid JSON payload."}), 400

        try:
            results = insert_records_method(data)
            return jsonify({"message": "Records processed successfully.", "results": results}), 200

        except ValueError as ve:
            logger.error(f"Validation error: {ve}")
            return jsonify({"error": f"Validation error: {str(ve)}"}), 400

        except ConnectionError as ce:
            logger.error(f"Connection error: {ce}")
            return jsonify({"error": f"Database connection error: {str(ce)}"}), 500

        except Exception as e:
            logger.error(f"Processing error: {e}")
            return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred."}), 500

if __name__ == '__main__':
    app.run(debug=DEV_MODE, port=5002)
