from kafka import KafkaProducer

def main():
    producer = KafkaProducer(
        bootstrap_servers=['nowledgeable.com:9092'],
        value_serializer=lambda x: x.encode('utf-8'),
        acks='all',
        retries=3
    )
    
    message = "coucou guelennoc"
    
    try:
        future = producer.send('exo1', value=message)
        record_metadata = future.get(timeout=10)
        print(f"Message envoyé: {message}")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
    main()
