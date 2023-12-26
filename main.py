from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import os
import psycopg2
import json

load_dotenv()
# Kafka broker address (replace with your broker address)
bootstrap_servers = '192.168.0.233:9092'
db_config = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"),
    'database': os.getenv("DB_NAME"),
    'port': os.getenv("DB_PORT")
}
# Create Consumer instance
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'test',
    'auto.offset.reset': 'earliest'
}

# Subscribe to topics
topics = ['humancount']


def process_message(msg):
    try:
        # Parse the JSON message
        data = json.loads(msg.value().decode('utf-8'))

        # Process the data (modify this part according to your needs)
        #print(f"Received message: {data}")

        # Insert data into PostgreSQL
        insert_into_database(data)

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")


def insert_into_database(data):
    # Connect to PostgreSQL database
    connection = psycopg2.connect(**db_config)

    try:
        with connection.cursor() as cursor:
            # SQL query to insert data into a table
            sql = "INSERT INTO humancount.rawsensordata (sensor, value) VALUES (%s, %s)"

            # Execute the query
            cursor.execute(sql, (data['sensor'], data['value']))

        # Commit changes to the database
        connection.commit()

    finally:
        # Close the database connection
        connection.close()


def kafka_consumer():
    consumer = Consumer(consumer_conf)

    # Subscribe to the Kafka topic
    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Process the received message
            process_message(msg)

    except KeyboardInterrupt:
        pass

    finally:
        # Close the Kafka consumer
        consumer.close()


if __name__ == '__main__':
    kafka_consumer()
