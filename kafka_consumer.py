from kafka import KafkaConsumer
import json

# Create a Kafka consumer
consumer = KafkaConsumer(
    'JerryWang-topic',  # subscribe to the specific topic
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # start reading from the earliest available message
    enable_auto_commit=True,
    group_id='JerryWang-group',
    value_deserializer=lambda x: json.loads(x)
)

# Consume messages
for message in consumer:
    print(f"Received message: {message.value}")
