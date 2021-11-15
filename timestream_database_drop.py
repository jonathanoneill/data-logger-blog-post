import awswrangler as wr

DATABASE_NAME = "DataLoggerDB"
TABLE_NAME = "IoT"

wr.timestream.delete_table(DATABASE_NAME, TABLE_NAME)
wr.timestream.delete_database(DATABASE_NAME)
