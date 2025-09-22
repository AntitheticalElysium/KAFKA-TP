from kafka import KafkaConsumer

def main():
    consumer = KafkaConsumer(
        'exo1',
        bootstrap_servers=['nowledgeable.com:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='consumer-group-1',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    
    print("Consumer démarré sur topic 'exo1'...")
    
    try:
        for message in consumer:
            print(f"Message: {message.value}")
    except KeyboardInterrupt:
        print("\nArrêt du consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()