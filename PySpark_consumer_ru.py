from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')

topic = "UK"
consumer.subscribe([topic])

for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
