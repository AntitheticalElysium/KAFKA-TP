from kafka import KafkaConsumer
import json

def main():
    consumer = KafkaConsumer(
        'prediction_guelennoc',
        bootstrap_servers=['nowledgeable.com:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='prediction-monitor-group',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    
    print("Monitoring predictions...")
    
    try:
        for message in consumer:
            prediction_data = json.loads(message.value)
            print(f"Received prediction: {prediction_data['prediction']} for user {prediction_data['user_id']}")
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
