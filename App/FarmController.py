import requests
import json
import random
import time
from paho.mqtt import client as mqtt_client
from datetime import datetime

broker = 'broker.hivemq.com'
port = 1883
topic = "Fazenda/Piquete/"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'mqtt1001'
password = '12345'


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    msg_count = 0
    while True:
        time.sleep(1)
        msg = f"messages: {msg_count}"
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        msgtopic = msg.payload.decode()
        json_decodificado = json.loads(msgtopic)
        node1 = json_decodificado['node']
        #if node1 == "635204857":
        #    print(json_decodificado['temperatura'])
        print(node1)

        insereLeituraMonitor(json_decodificado)

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

def insereLeituraMonitor(params):
    link = "https://farmmonitor-77437-default-rtdb.firebaseio.com"


    # datetime object containing current date and time
    now = datetime.now()

    print("now =", now)

    # dd/mm/YY H:M:S
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print("date and time =", dt_string)

    try:

        dados = {'data': dt_string, 'chuva': params['chuva'], 'direcao': params['direcao'], 'luz': params['luz'], 'node': params['node'],
                 'temperatura': params['temperatura'], 'umidade': params['umidade'],
                 'velocidadeVento': params['velocidade']}
        requisicao = requests.post(f'{link}/fazenda/piquete/.json', data=json.dumps(dados))
        print(requisicao)
        print(requisicao.text)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    run()

