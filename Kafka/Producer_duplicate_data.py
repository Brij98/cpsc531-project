# import kafka producer
from kafka import KafkaProducer
import json
from duplicate_data import get_registered_users
import time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)

if __name__ == "__main__":
    while 1 == 1:
        registered_user = get_registered_users()
        producer.send("duplicate_heart_data", registered_user)
        print(registered_user)
        time.sleep(10)