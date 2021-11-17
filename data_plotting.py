import awswrangler as wr

from matplotlib import pyplot as plt

DATABASE_NAME = "DataLoggerDB"
TABLE_NAME = "IoT"

QUERY_DATE = "2021-11-15"
QUERY_SENSOR = "001"

# Get date from Timestream database
SELECT_SQL = f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME} WHERE SensorId = '{QUERY_SENSOR}' AND DATE(time) = '{QUERY_DATE}' ORDER BY time ASC"

df = wr.timestream.query(SELECT_SQL)

print('Raw data from Timestream')
print(df)

# Data cleaning - Remove date and seconds
df['time'] = df['time'].apply(lambda t: t.strftime('%H:%M'))

# Data cleaning - Pivot table so Temperature and Relative Humidity are columns
df = df.pivot(index="time", columns="measure_name", values="measure_value::double")
df.reset_index(inplace=True)

# Data cleaning - Remove rows where there null values
df.dropna()

# Data cleaning - Smooth out data before plotting
df['Temperature'] = df['Temperature'].interpolate(method='polynomial', order=2)
df['Relative Humidity'] = df['Relative Humidity'].interpolate(method='polynomial', order=2)

print('Cleaned data for plotting')
print(df)

# Plot data
figure, axes = plt.subplots(2)

figure.suptitle(f'Temperature and Relative Humidity\n({QUERY_SENSOR} - {QUERY_DATE})')

df.plot(kind='line',x='time',y='Temperature', ax=axes[0], color='tab:blue', legend=None)
df.plot(kind='line',x='time',y='Relative Humidity', ax=axes[1], color='tab:orange', legend=None)

axes[0].set_ylabel('Temperature ($^\circ$C)')
axes[0].set_xlabel(None)
axes[1].set_ylabel('Relavive Humidity (%)')
axes[1].set_xlabel('Time')

plt.show()
