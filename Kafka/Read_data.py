import random


def get_Read_Heart_data():
    heart_data = {}
    i = 0
    with open('processed.va.data', 'r') as f:
        for line in f:
            heart_data_nested = {}
            Heart_data_string = line
            size = len(Heart_data_string)
            Heart_data_string = Heart_data_string[:size - 1]
            Heart_data_list = list(Heart_data_string.split(","))

            # for line in f:
            #     Heart_data_string = line
            #     size = len(Heart_data_string)
            #     Heart_data_string = Heart_data_string[:size - 1]
            #     Heart_data_list = list(Heart_data_string.split(","))
            #     print(Heart_data_list)
            heart_data_nested['age'] = Heart_data_list[0]
            heart_data_nested['sex'] = Heart_data_list[1]
            heart_data_nested['cp'] = Heart_data_list[2]
            heart_data_nested['trestbps'] = Heart_data_list[3]
            heart_data_nested['chol'] = Heart_data_list[4]
            heart_data_nested['fbs'] = Heart_data_list[5]
            heart_data_nested['restecg'] = Heart_data_list[6]
            heart_data_nested['thalach'] = Heart_data_list[7]
            heart_data_nested['exang'] = Heart_data_list[8]
            heart_data_nested['oldpeak'] = Heart_data_list[9]
            heart_data_nested['slope'] = Heart_data_list[10]
            heart_data_nested['ca'] = Heart_data_list[11]
            heart_data_nested['thal'] = Heart_data_list[12]
            heart_data_nested['angiographic_disease_status'] = Heart_data_list[13]
            heart_data[i] = heart_data_nested
            i += 1

    return heart_data

get_Read_Heart_data()
