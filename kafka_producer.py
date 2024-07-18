from kafka import KafkaProducer
import json
import time

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Produce messages
for i in range(10):
    message = {'number': i, 'timestamp': time.time()}
    producer.send('JerryWang-topic', value=message)
    print(f"Produced message: {message}")
    time.sleep(1)

# Ensure all messages are sent
producer.flush()

