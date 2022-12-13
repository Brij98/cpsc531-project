# import kafka producer
import random
from kafka import KafkaProducer
from json import dumps
from Read_data import get_Read_Heart_data
import time

KAFKA_TOPIC_NAME_CONS = "New_topic_4"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS, value_serializer=lambda x: dumps(x).encode('utf-8'))


    heart_data = None
    for i in range(500) :
        i = i + 1
        heart_data = {}
        print("Preparing message: " + str(i))
        heart_data['age'] = int(random.randint(40, 80))
        heart_data['sex'] = int(random.randint(0, 1))
        heart_data['cp'] = int(random.randint(1, 4))
        heart_data['trestbps'] = int(random.randint(90, 150))
        heart_data['chol'] = int(random.randint(125, 260))
        heart_data['fbs'] = int(random.randint(0, 1))
        heart_data['restecg'] = int(random.randint(0, 2))
        heart_data['thalach'] = int(random.randint(80, 180))
        heart_data['exang'] = int(random.randint(0, 1))
        heart_data['oldpeak'] = round(random.uniform(0.0, 4.0), 1)
        heart_data['slope'] = int(random.randint(0, 2))
        heart_data['ca'] = int(random.randint(0, 3))
        heart_data['thal'] = int(random.randint(1, 3))
        print("heart data : ", heart_data)
        producer.send(KAFKA_TOPIC_NAME_CONS, heart_data)
        time.sleep(10)
    print("Kafka Producer Application Completed. ")