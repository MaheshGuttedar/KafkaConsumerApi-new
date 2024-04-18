from confluent_kafka import Consumer, KafkaError

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'your_broker_address:9092',  # Change this to your Kafka broker address
    'group.id': 'my_consumer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest message in the topic
}

# Create Kafka consumer
consumer = Consumer(conf)

# Subscribe to Kafka topic(s)
topics = ['your_topic']  # Change this to your Kafka topic
consumer.subscribe(topics)

# Consume messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Adjust timeout as needed
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, consumer reached end of the topic
                print('%% %s [%d] reached end at offset %d\n' %
                      (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Process the received message
            print('Received message: %s' % (msg.value().decode('utf-8')))
except KeyboardInterrupt:
    pass
finally:
    # Close Kafka consumer
    consumer.close()
#test4