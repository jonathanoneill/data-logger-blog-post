import awswrangler as wr

DATABASE_NAME = "DataLoggerDB"
TABLE_NAME = "IoT"

wr.timestream.create_database(DATABASE_NAME)
wr.timestream.create_table(DATABASE_NAME, TABLE_NAME, memory_retention_hours=1, magnetic_retention_days=30)
