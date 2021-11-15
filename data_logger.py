import time
import datetime
import csv
import Adafruit_DHT
import boto3

from botocore.config import Config

DHT_TYPE = Adafruit_DHT.DHT11
DHT_PIN  = 4

FREQUENCY_SECONDS = 60
SENSOR_ID = '001'
SENSOR_LOCATION = 'Office'
DATA_FILE = 'data.txt'

DATABASE_NAME = "DataLoggerDB"
TABLE_NAME = "IoT"

def main():

    print('Logging sensor measurements to {0} every {1} seconds.'.format(DATA_FILE, FREQUENCY_SECONDS))
    print('Press Ctrl-C to quit.')

    write_file_header()

    while True:
    
        # Attempt to get sensor reading
        humidity, temperature = Adafruit_DHT.read(DHT_TYPE, DHT_PIN)

        # Skip to the next sensor reading if a valid measurement couldn't be taken
        if humidity is None or temperature is None:
            time.sleep(2)
            continue

        # Get the current time for the timestamp
        timestamp = datetime.datetime.now().isoformat()

        write_file_row(timestamp, 'Temperature', 'DegC', temperature)
        write_file_row(timestamp, 'Relative Humidity', '%', humidity)

        # Write to Timestream database
        write_to_timestream ('Temperature', 'DegC', temperature)
        write_to_timestream ('Relative Humidity', '%', humidity)

        # Wait before taking next reading
        time.sleep(FREQUENCY_SECONDS)

def write_file_header():

    with open(DATA_FILE, 'w') as csvfile:
        fieldnames = ['Timestamp','SensorId','Location','Measurement','Value','Unit']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    print('Timestamp                   SensorId  Location  Measurement        Value  Unit')
    print('---------                   --------  --------  -----------        -----  ----')

def write_file_row(timestamp, measurement, unit, value):

    with open(DATA_FILE, 'a') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([timestamp, SENSOR_ID, SENSOR_LOCATION, measurement, str(value), unit])

    print(  timestamp + "  " + 
            SENSOR_ID.ljust(8, ' ') + "  " + 
            SENSOR_LOCATION.ljust(8, ' ') + "  " + 
            measurement.ljust(18, ' ') + " " + 
            str(value).ljust(6, ' ') + " " + 
            unit )

def write_to_timestream(measurement, unit, value):

    # Create AWS session
    session = boto3.Session()

    client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000,
                                                                    retries={'max_attempts': 10}))

    # Build record to send to Timestream
    dimensions = [
        {'Name': 'SensorId', 'Value': SENSOR_ID},
        {'Name': 'Location', 'Value': SENSOR_LOCATION},
        {'Name': 'Unit', 'Value': unit}
    ]

    record = {
        'Dimensions': dimensions,
        'MeasureName': measurement,
        'MeasureValue': str(value),
        'MeasureValueType': 'DOUBLE',
        'Time': str(int(round(time.time() * 1000)))
    }

    records = [record]

    # Write record to Timestream and log errors
    try:
        client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                            Records=records, CommonAttributes={})

    except client.exceptions.RejectedRecordsException as err:
        print("RejectedRecords: ", err)
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        print("Other records were written successfully. ")

    except Exception as err:
        print("Error:", err)

if __name__ == '__main__':
    main()
