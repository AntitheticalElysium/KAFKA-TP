from kafka import KafkaProducer
import json

def main():
    producer = KafkaProducer(
        bootstrap_servers=['nowledgeable.com:9092'],
        value_serializer=lambda x: x.encode('utf-8'),
        acks='all',
        retries=3
    )
    
    message_data = {"data": [[1, 2], [3, 4]]}
    message_json = json.dumps(message_data)
    
    try:
        future = producer.send('guelennoc', value=message_json)
        record_metadata = future.get(timeout=10)
        print(f"Envoyé: {message_json}")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
