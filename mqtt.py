#!/usr/bin/env python

import paho.mqtt.client as mqtt
import socket
import sys
import json

broker_url = "192.168.1.110"
broker_port = 1883

flag_connected = 0

def on_connect(client, userdata, flags, rc):
   global flag_connected
   flag_connected = 1

def on_disconnect(client, userdata, rc):
   global flag_connected
   flag_connected = 0

def on_message(client, userdata, message):
    global connection
    payload = message.payload.decode("utf-8")
    #print("Received message '" + payload + "' on topic '"
    #    + message.topic + "' with QoS " + str(message.qos))
    msg = '{"topic":"'+message.topic+'","payload":"'+payload+'","qos":'+str(message.qos)+'}'
    length = len(msg)
    print("msg json:"+msg+" with length: "+str(length))
    connection.sendall(length.to_bytes(4,'little'))
    connection.sendall(msg.encode())
    print("send success")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.connect(broker_url, broker_port)
client.loop_start()

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the address given on the command line
#server_name = sys.argv[1]
server_address = ('localhost', 10000)
#print >>sys.stderr, 'starting up on %s port %s' % server_address
print ('starting up on %s port %s' % server_address)
sock.bind(server_address)
sock.listen(1)

while True:
    #print >>sys.stderr, 'waiting for a connection'
    print ('waiting for a connection')
    connection, client_address = sock.accept()
    try:
        #print >>sys.stderr, 'client connected:', client_address
        print('client connected: ',client_address)
        while True:
            data = connection.recv(4)
            length = int.from_bytes(data,'little')
            #print >>sys.stderr, 'received "%s"' % data
            print ('length:'+str(length))
            data = connection.recv(length).decode()
            if(len(data)==0):
                break
            msg = json.loads(data)
            print ('message: '+data)
            print (json.dumps(msg))
            print (msg['command'])
            if(msg['command'] == 'publish'):
                client.publish(topic=msg['topic'],payload=msg['payload'],qos=msg['qos'],retain=msg['retain'])
            elif(msg['command'] == 'subscribe'):
                client.subscribe(msg['topic'],qos=msg['qos'])
            
    finally:
        connection.close()
