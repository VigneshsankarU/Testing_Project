import pyodbc
import logging
from logging.handlers import RotatingFileHandler
import paho.mqtt.client as mqtt
import json
from decimal import Decimal
from datetime import datetime
import time
import os

# Configure logging
log_file = 'data_transfer_publisher.log'
log_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)
logging.basicConfig(handlers=[log_handler], level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# JSON file to store last SCAN_TIME
last_record_file = "last_record_times.json"

# Ensure the JSON file exists
if not os.path.exists(last_record_file):
    with open(last_record_file, "w") as f:
        json.dump({}, f)


# Function to load last SCAN_TIME from JSON file
def load_last_record_times():
    try:
        with open(last_record_file, "r") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading last record times: {e}")
        print(f"Error loading last record times: {e}")
        return {}


# Function to save last SCAN_TIME to JSON file
def save_last_record_times(last_record_times):
    try:
        with open(last_record_file, "w") as f:
            json.dump(last_record_times, f, indent=4)
    except Exception as e:
        logging.error(f"Error saving last record times: {e}")
        print(f"Error saving last record times: {e}")


# Function to connect to MS SQL
def connect_to_mssql():
    try:
        server = 'DESKTOP-TKOO9UM'
        database = 'HMIL_ASSY'
        username = 'sa'
        password = '123'
        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password}'
        )
        ms_sql_conn = pyodbc.connect(connection_string)
        logging.info("Connected to MS SQL Server successfully.")
        return ms_sql_conn
    except Exception as e:
        logging.error(f"Error connecting to MS SQL Server: {e}")
        print(f"Error connecting to MS SQL Server: {e}")
        raise


# Function to convert data types for JSON serialization
def convert_for_json(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    return obj


def fetch_and_publish(ms_sql_conn, query, parameter, last_record_times):
    try:
        cursor = ms_sql_conn.cursor()

        # Add SCAN_TIME filter if applicable
        last_time = last_record_times.get(parameter)
        if last_time:
            query_with_filter = f"{query} WHERE [SCAN_TIME] > ?"
            cursor.execute(query_with_filter, last_time)
        else:
            query_with_filter = query
            cursor.execute(query_with_filter)

        # Fetch data and retrieve column names
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]  # Get actual column names
        logging.info(f"Fetched {len(data)} rows for {parameter}.")
        print(f"Fetched {len(data)} rows for {parameter}.")

        if data:
            # Update last processed timestamp
            last_record_times[parameter] = data[-1][2].strftime("%Y-%m-%d %H:%M:%S")
            save_last_record_times(last_record_times)

            # Publish data to MQTT
            for row in data:
                row_dict = {column_names[i]: convert_for_json(row[i]) for i in range(len(row))}
                row_dict["PARAMETER"] = parameter
                publish_data(json.dumps(row_dict))
        else:
            logging.info(f"No new data for {parameter}.")
            print(f"No new data for {parameter}.")
    except Exception as e:
        logging.error(f"Error in processing {parameter}: {e}")
        print(f"Error in processing {parameter}: {e}")
    finally:
        cursor.close()


# Function to publish data to MQTT
def publish_data(data):
    try:
        client = mqtt.Client("UCAl_MQTT", False)
        client.connect("192.168.30.10", 1883, 60)
        client.publish("ucaltes", data, retain=True)
        logging.info(f"Published to MQTT: {data}")
        # print(f"Published to MQTT: {data}")
        client.disconnect()
    except Exception as e:
        logging.error(f"Error publishing data to MQTT: {e}")
        print(f"Error in publishing data to MQTT : {e}")


# SQL queries to fetch data
sql_queries = {
    "BODY_LEAK_TESTING": "SELECT BARCODE, [SL.NO], SCAN_TIME, LEAK_STATUS, [LEAK_RATE(Pa)] FROM BODY_LEAK_TESTING",
    "FINAL_INSPECTION": "SELECT BARCODE, [SL.NO], SCAN_TIME, FI_STATUS FROM FINAL_INSPECTION",
    "PERFORMANCE_TEST": """SELECT BARCODE, [SL.NO], SCAN_TIME, PRF_STATUS, [500m_BAR_ACTUAL_TIME(Sec)], RESULT_1, [665m_BAR_ACTUAL_TIME(Sec)], RESULT_2,
                            [800m_BAR_ACTUAL_TIME(Sec)], RESULT_3, [LEAK_RATE(mBar)], RESULT_4, [MAXIMUM_RPM_TEST_ACTUAL(mBar)], 
                            RESULT_5,[700mBar_ACTUAL_TIME(Sec)], RESULT_6 FROM PERFORMANCE_TEST""",
    "PRODUCT_LEAK-0.2_BAR": "SELECT BARCODE, [SL.NO], SCAN_TIME, [PL_0.2_STATUS] FROM [PRODUCT_LEAK-0.2_BAR]",
    "PRODUCT_LEAK-1.0_BAR": "SELECT BARCODE, [SL.NO], SCAN_TIME, [PL_1.0_STATUS], [LEAK_RATE(Pa)] FROM [PRODUCT_LEAK-1.0_BAR]",
    "REED_AND_STOPPER_ASSY": "SELECT BARCODE, [SL.NO], SCAN_TIME, RS_STATUS, [TORQUE(Nm)] FROM [REED_AND_STOPPER_ASSY]",
    "TOP_COVER_TIGHTENING": """SELECT BARCODE, [SL.NO], SCAN_TIME, TC_STATUS,
                                 [TORQUE_1(Nm/deg)], [ANGLE_1(Nm/deg)],
                                 [TORQUE_2(Nm/deg)], [ANGLE_2(Nm/deg)],
                                 [TORQUE_3(Nm/deg)], [ANGLE_3(Nm/deg)]
                                 FROM [TOP_COVER_TIGHTENING]"""
}


def main():
    while True:
        try:
            try:
                logging.info("Starting MS SQL connection...")
                ms_sql_conn = connect_to_mssql()

                logging.info("Processing queries...")
                for parameter, query in sql_queries.items():
                    fetch_and_publish(
                        ms_sql_conn,
                        query=query,
                        parameter=parameter,
                        last_record_times=load_last_record_times()
                    )

                logging.info("All queries processed. Sleeping for 60 seconds.")
                print("All queries processed. Sleeping for 60 seconds.")
            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                print(f"Error in main loop: {e}")
            finally:
                if 'ms_sql_conn' in locals():
                    ms_sql_conn.close()
                    logging.info("MS SQL connection closed.")
                time.sleep(60)

        except KeyboardInterrupt:
            print("Stopping...")
            break


if __name__ == "__main__":
    main()
