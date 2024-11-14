from flask import Flask, request, send_file
import pandas as pd
from io import BytesIO
import psycopg2
import logging

app = Flask(__name__)

# Database connection details
DB_HOST = ""
DB_PORT = ""
DB_NAME = ""
DB_USER = ""
DB_PASS = ""

# Setup logging
logging.basicConfig(level=logging.DEBUG)

@app.route('/download', methods=['GET'])
def download_data():
    # Get query parameters
    start_time = request.args.get('from')
    end_time = request.args.get('to')
    sensor_name = request.args.get('sensor', default=None)  # New parameter for sensor name

    # Validate input
    if not start_time or not end_time:
        return {"error": "Both 'from' and 'to' timestamps are required."}, 400

    try:
        start_time = int(start_time) / 1000  # Convert milliseconds to seconds
        end_time = int(end_time) / 1000
    except ValueError:
        return {"error": "Invalid timestamp format. Please provide timestamps in milliseconds."}, 400

    # Establish connection to PostgreSQL database
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
    except Exception as e:
        return {"error": f"Database connection error: {str(e)}"}, 500

    # Build SQL query based on sensor name
    if sensor_name is not None:
        query = f"""
        SELECT s.name, m.data, m.time
        FROM metrics m
        LEFT JOIN sensors s ON m.sensor_id = s.sensor_id
        WHERE m.time BETWEEN to_timestamp({start_time}) AND to_timestamp({end_time}) 
        AND s.name = %s
        ORDER BY m.time ASC;
        """
        query_params = (sensor_name,)
    else:
        query = f"""
        SELECT s.name, m.data, m.time
        FROM metrics m
        LEFT JOIN sensors s ON m.sensor_id = s.sensor_id
        WHERE m.time BETWEEN to_timestamp({start_time}) AND to_timestamp({end_time})
        ORDER BY m.time ASC;
        """
        query_params = ()

    try:
        # Log the query for debugging
        logging.debug(f"Executing query: {query} with params: {query_params}")

        # Execute the query with parameters
        df = pd.read_sql(query, conn, params=query_params)
    except Exception as e:
        return {"error": f"Error executing query: {str(e)}"}, 500
    finally:
        conn.close()

    if df.empty:
        return {"error": "No data found for the given time range."}, 404

    # Create CSV from DataFrame
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    return send_file(csv_buffer, as_attachment=True, download_name='sensor_data.csv', mimetype='text/csv')

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")


