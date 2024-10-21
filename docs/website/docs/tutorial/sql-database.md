import dlt
from dlt.sources.sql_database import sql_database_source
import pyarrow.parquet as pq

# Define the connection to the SQL database
source = sql_database_source({
    "drivername": "postgresql",
    "username": "your_username",
    "password": "your_password",
    "host": "your_host",
    "port": "your_port",
    "database": "your_database"
})

# Specify the tables to extract data from
source.add_table("employee")
source.add_table("fee_payments")

# Create the pipeline to load data into Parquet format
pipeline = dlt.pipeline(
    pipeline_name="employee_fee_pipeline",
    destination="duckdb",  # For Parquet
    dataset_name="employee_fee_data"
)

# Run the pipeline to extract and load data
load_info = pipeline.run(source)

# Optionally verify the Parquet file
table = pq.read_table('employee_fee_data.parquet')
print(table.to_pandas())
