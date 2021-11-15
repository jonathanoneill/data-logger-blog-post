import time
import datetime
import csv
import Adafruit_DHT

DHT_TYPE = Adafruit_DHT.DHT11
DHT_PIN  = 4

FREQUENCY_SECONDS = 60
SENSOR_ID = '001'
SENSOR_LOCATION = 'Office'
DATA_FILE = 'data.txt'

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

if __name__ == '__main__':
    main()
