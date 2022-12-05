import random

from faker import Faker
fack = Faker()


def get_Heart_data():
    heart_data = {}
    heart_data['age'] = str(random.randint(40, 80))
    heart_data['sex'] = str(random.randint(0, 1))
    heart_data['cp'] = str(random.randint(1, 4))
    heart_data['trestbps'] = str(random.randint(90, 150))
    heart_data['chol'] = str(random.randint(125, 260))
    heart_data['fbs'] = str(random.randint(0, 1))
    heart_data['restecg'] = str(random.randint(0, 2))
    heart_data['thalach'] = str(random.randint(80, 180))
    heart_data['exang'] = str(random.randint(0, 1))
    heart_data['oldpeak'] = str(round(random.uniform(0.0, 4.0), 1))
    heart_data['slope'] = str(random.randint(0, 2))
    heart_data['ca'] = str(random.randint(0, 3))
    heart_data['thal'] = str(random.randint(1, 3))
    heart_data['angiographic_disease_status'] = str(random.randint(0, 1))

    return heart_data