import awswrangler as wr

DATABASE_NAME = "DataLoggerDB"
TABLE_NAME = "IoT"

SELECT_SQL = f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME} ORDER BY time DESC LIMIT 10"

df = wr.timestream.query(SELECT_SQL)

print(df)
