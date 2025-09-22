from kafka import KafkaConsumer
import json
import numpy as np

def main():
    consumer = KafkaConsumer(
        'guelennoc',
        bootstrap_servers=['nowledgeable.com:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='json-consumer-group',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    
    try:
        for message in consumer:
            data_dict = json.loads(message.value)
            data_array = np.array(data_dict['data'])
            somme = np.sum(data_array)
            print(f"Reçu: {data_dict}")
            print(f"Somme: {somme}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
