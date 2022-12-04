import requests
import json
import random
import time
from paho.mqtt import client as mqtt_client
from datetime import datetime
from datetime import date, datetime
#import pandas as pd

#import binascii
#import pymysql
#import struct
#import codecs

tempmax=0.00
tempmin=100.0
tempmed=0.00
umidmax=0.00
umidmin=100.0
umidmed=0.00
luzmax=0.00
luzmin=100.0
luzmed=0.00
cont=0.0
leitdados=0.0


broker = 'broker.hivemq.com'
port = 1883
topic = "Fazenda/Soja/"
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'tercioasfilho'
password = '12345'

def HoraAtual():
    hora_atual = str(datetime.now())
    hora_atual = hora_atual[11:19]
    return hora_atual


def DataAtual():
    data_atual = str(date.today())
    return data_atual


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
        global tempmax
        global tempmin
        global leitdados
        global tempmed
        global umidmax
        global umidmin
        global umidmed
        global cont

        try:
                msgtopic = msg.payload.decode("latin1")
                json_decodificado = json.loads(msgtopic)
                node1 = json_decodificado['node']
                #print(node1) 
                hora_atual = HoraAtual()
                data_atual = DataAtual()
                #if node1=="635201875":
                res1="2"

                umidasolo=float(json_decodificado['umidadesolo'])
                sinal1= float(json_decodificado['sinalwifi'])

                # cursor.execute("INSERT INTO `Leituras`( `CodEst`,`CodSen`, `Dat`, `Hor`, `Val`,`RSSI`,`TemRes`) VALUES ('" + res1+"','11','" + data_atual + "','" + hora_atual + "','" + str(umidasolo) +"','" + str(sinal1) + "','" + str(20)+"')")
                # print ("INSERT INTO `Leituras`( `CodEst`,`CodSen`, `Dat`, `Hor`, `Val`,`RSSI`,`TemRes`) VALUES ('" + res1+"','11','" + data_atual + "','" + hora_atual + "','" + str(umidasolo) +"','" + str(sinal1) + "','" + str(20)+"')")
                # conexao.commit()
                #if node1=='635204857':

                res1="1"
                luz = float(json_decodificado['luz'])
                temp = float(json_decodificado['temperatura'])
                umid = float(json_decodificado['umidade'])
                vel = json_decodificado['velocidade']
                chuv = json_decodificado['chuva']
                dire = json_decodificado['direcao']
                sinal= float(json_decodificado['sinalwifi'])
        except Exception as e:
            print("An exception occurred" + str(e))


    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

def insereLeituraMonitor(params):
    link = "https://farmmonitor-77437-default-rtdb.firebaseio.com"
    now = datetime.now()

    print("now =", now)

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