from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings,
    DataTypes,
    TableEnvironment,
    StreamTableEnvironment,
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration


# lpep_pickup_datetime VARCHAR,
# lpep_dropoff_datetime VARCHAR,
def create_events_aggregated_sink(t_env):
    table_name = "processed_taxi_events_aggregated"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            event_hour TIMESTAMP(3),
            num_hits BIGINT,
            PRIMARY KEY (event_hour, DOLocationID, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


# lpep_pickup_datetime VARCHAR,
# lpep_dropoff_datetime VARCHAR,
def create_events_source_kafka(t_env):
    table_name = "taxi_events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            lpep_dropoff_datetime VARCHAR,
            event_watermark AS TO_TIMESTAMP(lpep_dropoff_datetime),
            WATERMARK for event_watermark as event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(
        # [puLocID, DOLocID, do_datetime, timestamp, watermark]
        # This lambda is your timestamp assigner:
        #   event -> The data record
        #   timestamp -> The previously assigned (or default) timestamp
        lambda event, timestamp: event[
            3
        ]  # We treat the third tuple element as the event-time (ms).
    )
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        # t_env.from_path(source_table).key_by(lambda e: (e[0], e[1])) \
        #     .window(
        #         Tumble.over(lit(5).minutes).on(col('event_watermark')).alias("w")
        # ).group_by(
        #     col("w"),
        #     col("DOLocationID"),
        #     col("PULocationID")
        # ) \
        #     .select(
        #         col('w').start.alias('event_hour'),
        #         col('PULocationID'),
        #         col('DOLocationID'),
        #         col('DOLocationID').count.alias('num_hits')
        #     ) \
        #     .execute_insert(aggregated_table).wait()

        t_env.execute_sql(
            f"""
        INSERT INTO {aggregated_table}
        SELECT
            window_start as event_hour,
            DOLocationID,
            PULocationID,
            COUNT(*) AS num_hits
        FROM TABLE(
            TUMBLE(
                TABLE {source_table}, 
                DESCRIPTOR(event_watermark), 
                INTERVAL '5' MINUTE
            )
        )
        GROUP BY window_start, DOLocationID, PULocationID;
        
        """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == "__main__":
    log_aggregation()
