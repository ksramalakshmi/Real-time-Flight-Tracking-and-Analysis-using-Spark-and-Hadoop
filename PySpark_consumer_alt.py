from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')

topic = "alt5000"
consumer.subscribe([topic])

for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
