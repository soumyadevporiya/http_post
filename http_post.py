import urllib.request
from flask import Flask
from flask import jsonify
import json
from kafka import KafkaConsumer
from flask import request
from kafka import KafkaProducer
# Required for Kubernetes
import os
import pint
import kubernetes
from kubernetes import client, config, watch
import time


import editdistance
import pandas as pd

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World'


@app.route('/hello/<name>')
def hello_name(name):
    return 'Hello %s!' %name


@app.route('/trigger')
def trigger_read():
    consumer = KafkaConsumer('my-topic', bootstrap_servers=['34.28.118.32:9094'], auto_offset_reset='latest')
    i=0
    dict = {}
    for message in consumer:
        message_1 = message.value
        print(message_1)
        my_json = message_1.decode('utf8')
        print(my_json)
        #my_json_dict = json.loads(my_json)
        #print(my_json_dict)
        dict[str(i)] = my_json
        i = i + 1
        print(i)
        print(dict)
        if i == 1:
            break

    consumer.close()
    return dict




#Model
@app.route('/linearregression')
def model_serving():
    consumer = KafkaConsumer('my-topic', bootstrap_servers=['34.28.118.32:9094'], auto_offset_reset='latest')
    i=0
    dict = {}
    for message in consumer:
        message_1 = message.value
        #print byte-stream
        print(message_1)

        #print the json equivalent
        my_json = message_1.decode('utf8')
        #print(my_json)

        #extract values of individual fields; first convert json string into dictionary
        v1 = json.loads(my_json)["v1"]
        v2 = json.loads(my_json)["v2"]
        v3 = json.loads(my_json)["v3"]
        v4 = json.loads(my_json)["v4"]
        print(v1)
        print(v2)
        print(v3)
        print(v4)

        prediction = 0.3 + float(v1)*0.1 + float(v2)*0.2 + float(v3)*0.3 + float(v4)*0.4

        my_json_dict = json.loads(my_json)

        my_json_dict["prediction"] = prediction
        print(my_json_dict)

        # Updating Dictionary Values
        dict[str(i)]  = my_json_dict
        #dict.append(my_json_dict)

        i=i+1
        if i==10:
            break

    consumer.close()
    return dict
    #return 'hello'

@app.route('/type')
def return_type():
    consumer = KafkaConsumer('my-second-topic', bootstrap_servers=['34.28.118.32:9094'], auto_offset_reset='latest')
    i=0
    dict = {}
    for message in consumer:
        message_1 = message.value
        #print byte-stream
        #print(message_1)

        #print the json equivalent
        my_json = message_1.decode('utf8')
        #print(my_json)

        #extract values of individual fields; first convert json string into dictionary
        identifier = json.loads(my_json)["id"]
        type = json.loads(my_json)["type"]



        my_json_dict = json.loads(my_json)


        # Updating Dictionary Values
        dict[str(i)]  = my_json_dict
        #dict.append(my_json_dict)

        i=i+1
        if i==10:
            break

    consumer.close()
    return dict
    #return 'hello'

@app.route('/fifth')
def get_details():
    consumer = KafkaConsumer('my-fifth-topic', bootstrap_servers=['34.28.118.32:9094'], auto_offset_reset='latest')
    i = 0
    dict = {}
    for message in consumer:
        message_1 = message.value
        # print byte-stream
        print(message_1)

        # print the json equivalent
        my_json = message_1.decode('utf8')
        print(my_json)
        my_json_dict = json.loads(my_json)
        # Updating Dictionary Values
        dict[str(i)] = my_json_dict

        i = i + 1
        if i == 2:
            break

    consumer.close()
    return dict

# K8 Pod Details
@app.route('/hello/pods')
def get_pod_details():
    config.load_incluster_config()
    v1 = kubernetes.client.CoreV1Api()
    #ret = v1.list_node()
    ret = v1.list_pod_for_all_namespaces(watch=False)
    #details = " " + ret
    details = " "
    for i in ret.items:
        details = details + " " + i.status.pod_ip+ " " + i.metadata.namespace + " " + i.metadata.name + "\n"

    return jsonify({"message":"POD Details ", "Information: ": details})


#Page 1
@app.route('/hello/gen_msg/1')
def generate_message_1():
    return jsonify({'message': "This is from Page 1"})

@app.route('/hello/get_msg/1')
def get_msg_1():
    content = urllib.request.urlopen('http://127.0.0.1:60/hello/gen_msg/1').read().decode('utf-8')
    return jsonify({'message': 'Delivered', 'Value': json.loads(content)['message'] })

#Page 2
@app.route('/hello/gen_msg/2')
def generate_message_2():
    return jsonify({'message': "This is from Page 2"})

@app.route('/hello/get_msg/2')
def get_msg_2():
    content = urllib.request.urlopen('http://127.0.0.1:60/hello/gen_msg/2').read().decode('utf-8')
    return jsonify({'message': 'Delivered', 'Value': json.loads(content)['message'] })


#Page 3
@app.route('/hello/gen_msg/3')
def generate_message_3():

    return jsonify({'message': "This is from Page 3"})

@app.route('/hello/get_msg/3')
def get_msg_3():
    content = urllib.request.urlopen('http://127.0.0.1:60/hello/gen_msg/3').read().decode('utf-8')
    return jsonify({'message': 'Delivered', 'Value': json.loads(content)['message'] })

#Page 4
@app.route('/hello/gen_msg/4')
def generate_message_4():
    return jsonify({'message': "This is from Page 4"})

@app.route('/hello/get_msg/4')
def get_msg_4():
    content = urllib.request.urlopen('http://127.0.0.1:60/hello/gen_msg/4').read().decode('utf-8')
    return jsonify({'message': 'Delivered', 'Value': json.loads(content)['message'] })

#Page 5
@app.route('/hello/gen_msg/5')
def generate_message_5():
    return jsonify({'message': "This is from Page 5"})

@app.route('/hello/get_msg/5')
def get_msg_5():
    content = urllib.request.urlopen('http://127.0.0.1:60/hello/gen_msg/5').read().decode('utf-8')
    return jsonify({'message': 'Delivered', 'Value': json.loads(content)['message'] })

#Page 6
@app.route('/hello/gen_msg/6')
def generate_message_6():
    return jsonify({'message': "This is from Page 6"})

@app.route('/hello/get_msg/6')
def get_msg_6():
    content = urllib.request.urlopen('http://127.0.0.1:60/hello/gen_msg/6').read().decode('utf-8')
    return jsonify({'message': 'Delivered', 'Value': json.loads(content)['message'] })

#Page 7
@app.route('/hello/gen_msg/7')
def generate_message_7():
    return jsonify({'message': "This is from Page 7"})

@app.route('/hello/get_msg/7')
def get_msg_7():
    content = urllib.request.urlopen('http://127.0.0.1:60/hello/gen_msg/7').read().decode('utf-8')
    return jsonify({'message': 'Delivered', 'Value': json.loads(content)['message'] })


@app.route('/hello/post', methods=['POST'])
def post_message():
    i = 0
    if request.method == 'POST':
        data_id = request.form
        data_1 = {'customer_id': data_id.getlist('customer_id'), 'customer_name': data_id.getlist('customer_name')}
        df_customer = pd.DataFrame(data_1)

        data_2 = {'sanctioned_name': data_id.getlist('sanctioned_name')}
        df_watchlist = pd.DataFrame(data_2)

        df_customer['key'] = 1
        df_watchlist['key'] = 1

        df_merged = pd.merge(df_customer, df_watchlist, on='key').drop('key', 1)

        dict_merged = df_merged.to_dict('records')

        #print(df_merged)
        alert = {}


        for each in dict_merged:
            score = editdistance.eval(each['customer_name'], each['sanctioned_name'])
            if score <= 1:
                i = i + 1
                # print(each['customer_name'],each['sanctioned_name'],score)
                alert[str(i)] = dict(zip(('customer_id', 'customer_name', 'sanctioned_name', 'edit_distance'),
                                         (each['customer_id'], each['customer_name'], each['sanctioned_name'], score)))

        #print(alert)

        if i >= 1:
            producer = KafkaProducer(bootstrap_servers=['35.225.83.11:9094'], api_version=(0, 10))
            producer.send('my-second-topic', json.dumps(alert).encode('utf-8'))
            producer.close()

    message = "Finish Time: " + str(int(round(time.time()))) + "           Alert: " + alert.__str__()

    #return '<h1>message</h1>'
    return message

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80, threaded=True)
