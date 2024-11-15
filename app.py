from flask import Flask, request, send_file, jsonify
import pandas as pd
from io import BytesIO
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Database connection details from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Setup logging
logging.basicConfig(level=logging.DEBUG)

@app.route('/download', methods=['GET'])
def download_data():
    # Get query parameters
    start_time = request.args.get('from')
    end_time = request.args.get('to')
    sensor_name = request.args.get('sensor')  # Optional parameter for sensor name

    # Validate input
    if not start_time or not end_time:
        return jsonify({"error": "Both 'from' and 'to' timestamps are required."}), 400

    try:
        # Convert milliseconds to datetime format
        start_time = datetime.fromtimestamp(int(start_time) / 1000)
        end_time = datetime.fromtimestamp(int(end_time) / 1000)
    except ValueError:
        return jsonify({"error": "Invalid timestamp format. Please provide timestamps in milliseconds."}), 400

    # Establish connection to PostgreSQL database
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        return jsonify({"error": "Database connection error."}), 500

    # Build SQL query with optional sensor filter
    query = """
        SELECT s.name, m.data, m.time
        FROM metrics m
        LEFT JOIN sensors s ON m.sensor_id = s.sensor_id
        WHERE m.time BETWEEN %s AND %s
    """
    query_params = [start_time, end_time]

    if sensor_name:
        query += " AND s.name = %s"
        query_params.append(sensor_name)

    query += " ORDER BY m.time ASC;"

    try:
        logging.debug(f"Executing query: {query} with params: {query_params}")

        # Use a cursor with dictionary output for easy DataFrame creation
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute(query, query_params)
            rows = cursor.fetchall()

        # Check if data exists
        if not rows:
            return jsonify({"error": "No data found for the given time range."}), 404

        # Convert rows to DataFrame
        df = pd.DataFrame(rows, columns=["name", "data", "time"])
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return jsonify({"error": "Error executing query."}), 500
    finally:
        conn.close()

    # Create CSV from DataFrame
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Return the CSV file as an attachment
    return send_file(csv_buffer, as_attachment=True, download_name='sensor_data.csv', mimetype='text/csv')

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
