from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings,
    DataTypes,
    TableEnvironment,
    StreamTableEnvironment,
)


# create table for flink to place data once processed
def create_taxi_events_sink_postgres(t_env):
    table_name = "processed_taxi_events"
    # passenger_count INTEGER,
    sink_ddl = f"""
        CREATE OR REPLACE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance FLOAT,
            tip_amount FLOAT,
            passenger_count INTEGER,
            dropoff_timestamp TIMESTAMP
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    # sink_ddl = f"""
    #     CREATE OR REPLACE TABLE {table_name} (
    #         VendorID INTEGER,
    #         lpep_pickup_datetime VARCHAR,
    #         lpep_dropoff_datetime VARCHAR,
    #         store_and_fwd_flag VARCHAR,
    #         RatecodeID INTEGER ,
    #         PULocationID INTEGER,
    #         DOLocationID INTEGER,
    #         passenger_count INTEGER,
    #         trip_distance DOUBLE,
    #         fare_amount DOUBLE,
    #         extra DOUBLE,
    #         mta_tax DOUBLE,
    #         tip_amount DOUBLE,
    #         tolls_amount DOUBLE,
    #         ehail_fee DOUBLE,
    #         improvement_surcharge DOUBLE,
    #         total_amount DOUBLE,
    #         payment_type INTEGER,
    #         trip_type INTEGER,
    #         congestion_surcharge DOUBLE
    #     ) WITH (
    #         'connector' = 'jdbc',
    #         'url' = 'jdbc:postgresql://postgres:5432/postgres',
    #         'table-name' = '{table_name}',
    #         'username' = 'postgres',
    #         'password' = 'postgres',
    #         'driver' = 'org.postgresql.Driver'
    #     );
    #     """
    t_env.execute_sql(sink_ddl)
    return table_name


# create source (similar to a table) for raw events from kafka
def create_events_source_kafka(t_env):
    table_name = "taxi_events"
    pattern = "yyyy-MM-dd HH:mm:ss"
    # passenger_count AS COALESCE(passenger_count, 0)),
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance FLOAT,
            tip_amount FLOAT,
            passenger_count INTEGER,
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    # source_ddl = f"""
    #     CREATE TABLE {table_name} (
    #         VendorID INTEGER,
    #         lpep_pickup_datetime VARCHAR,
    #         lpep_dropoff_datetime VARCHAR,
    #         store_and_fwd_flag VARCHAR,
    #         RatecodeID INTEGER ,
    #         PULocationID INTEGER,
    #         DOLocationID INTEGER,
    #         passenger_count INTEGER,
    #         trip_distance DOUBLE,
    #         fare_amount DOUBLE,
    #         extra DOUBLE,
    #         mta_tax DOUBLE,
    #         tip_amount DOUBLE,
    #         tolls_amount DOUBLE,
    #         ehail_fee DOUBLE,
    #         improvement_surcharge DOUBLE,
    #         total_amount DOUBLE,
    #         payment_type INTEGER,
    #         trip_type INTEGER,
    #         congestion_surcharge DOUBLE,
    #         pickup_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, '{pattern}'),
    #         WATERMARK FOR pickup_timestamp AS pickup_timestamp - INTERVAL '15' SECOND
    #     ) WITH (
    #         'connector' = 'kafka',
    #         'properties.bootstrap.servers' = 'redpanda-1:29092',
    #         'topic' = 'green-trips',
    #         'scan.startup.mode' = 'earliest-offset',
    #         'properties.auto.offset.reset' = 'earliest',
    #         'format' = 'json'
    #     );
    #     """
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    # env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_taxi_events_sink_postgres(t_env)
        # write records to postgres too!
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        *,
                        CAST(CASE WHEN passenger_count = '' THEN '0' ELSE passenger_count END AS INT) AS passenger_count
                    FROM {source_table}
                    """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == "__main__":
    log_processing()
