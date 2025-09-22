from kafka import KafkaConsumer, KafkaProducer
import json
import numpy as np

def main():
    consumer = KafkaConsumer(
        'guelennoc',
        bootstrap_servers=['nowledgeable.com:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='processor-group',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    
    producer = KafkaProducer(
        bootstrap_servers=['nowledgeable.com:9092'],
        value_serializer=lambda x: x.encode('utf-8'),
        acks='all',
        retries=3
    )
    
    try:
        for message in consumer:
            data_dict = json.loads(message.value)
            data_array = np.array(data_dict['data'])
            total = np.sum(data_array)
            
            result = {"sum": int(total)}
            result_json = json.dumps(result)
            
            producer.send('processed', value=result_json)
            print(f"Processed: {data_dict} -> {result}")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()
