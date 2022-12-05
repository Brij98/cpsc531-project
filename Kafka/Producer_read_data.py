# import kafka producer
from kafka import KafkaProducer
import json
from Read_data import get_Read_Heart_data
import time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)

if __name__ == "__main__":
    heart_data = get_Read_Heart_data()
    for key in heart_data :
        producer.send("Read_heart_data", key)
        print(heart_data[key])
        time.sleep(10)