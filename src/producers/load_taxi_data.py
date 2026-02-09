import csv
import json
from kafka import KafkaProducer
from time import time


def main():
    t0 = time()
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    csv_file = "data/green_tripdata_2019-10.csv"

    topic_name = "green-trips"
    desired_columns = [
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "tip_amount",
    ]

    with open(csv_file, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        #         filter out columns here (only need 7 columns) ??

        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            selected_data = {col: row[col] for col in desired_columns}
            # Send data to Kafka topic "green-trips"
            # producer.send(topic_name, value=row)
            producer.send(topic_name, value=selected_data)

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()

    t1 = time()
    took = t1 - t0
    print(f"SEND DATA TO PRODUCER IN {took}")


if __name__ == "__main__":
    main()
