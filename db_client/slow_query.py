# pip install mysql-connector-python
import pika
import socket
import shutil
import sys
import os
import json
import base64
import time
import argparse
import string
import random
import requests
import datetime
from datetime import datetime
from datetime import timedelta
from pymongo import MongoClient

#*******************************************************************************************************
parser = argparse.ArgumentParser()
parser.add_argument("-o","--host", help="set host")
parser.add_argument("-rr","--routingrabbit", help="routing rabbit")
parser.add_argument("-sk","--socketkey", help="socket vm")
parser.add_argument("-i","--interval", help="set port")
parser.add_argument("-t","--type", help="type db (mongo / mysql)")
parser.add_argument("-th","--threshold", help="threshold")

#mongo
parser.add_argument("-db","--database", help="database name")

#mysql
parser.add_argument("-hostdb","--hostdb", help="host db")
parser.add_argument("-user","--username", help="username")
parser.add_argument("-pass","--password", help="password")
parser.add_argument("-portdb","--portdb", help="set port")

args = parser.parse_args()
# args.password
#*******************************************************************************************************

#-------------------------------------------mongodb
# python3.7 slow_query.py -i 5 -th 10 -t mongo -o 0.0.0.0 -rr autopay -db onepush -sk 0df684f0-6560-4c47-9f11-75185d3091df
# db.bni_log.aggregate([{$group:{"_id": "$typeService"}}])
# db.bni_log.find().forEach((doc) => { doc._id = new ObjectId(); db.bni_log.insert(doc) })

if args.type == 'mongo':
    # for k in range(0 , int(args.interval)):
    #     print (k)
    while True:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=args.host))

        channel_http = connection.channel()
        channel_http.queue_declare(queue='%s.slowquery' % args.routingrabbit, durable=True)

        print('ini mongo')

        client = MongoClient()

        x = args.database
        db = client.x
        current_ops = db.current_op( '{ "currentOp": true, "secs_running" : {"$gte":'+args.threshold+'}, "command.$db": {"$ne": "admin"} }' )

        dat_json = current_ops['inprog']
        for k in dat_json:
            if len(k) == 23:
                if k['secs_running'] >= int(args.threshold):
                    print(k['command'])
                    print('\n')
                    print(k['secs_running'])

                    datasave = {
                        "socket_key"     : args.socketkey,
                        "hostname"      : socket.gethostname(),
                        # "hostname"      : 'Host-002-AP',
                        "ip"            : socket.gethostbyname(socket.gethostname()),
                        "type_db"     : 1,
                        "pid"         : int(k['opid']),
                        "command"    : str(k['command']),
                        "time"    : k['secs_running'],
                        "create"     : datetime.now() ,

                        "host" : args.host,
                        "routing_key" : args.routingrabbit,
                        }

                    datajson = {
                        "socket_key"     : args.socketkey,
                        "hostname"      : socket.gethostname(),
                        # "hostname"      : 'Host-002-AP',
                        "ip"            : socket.gethostbyname(socket.gethostname()),
                        "type_db"     : 1,
                        "pid"         : int(k['opid']),
                        "command"    : str(k['command']),
                        "time"    : k['secs_running'],
                        "create"     : format(datetime.now() ),
                        }

                    # print(datajson)

                    API_ENDPOINT = "https://autopay.env-playground.online/app/db"
                    r = requests.post(url = API_ENDPOINT, data = datasave)

                    channel_http.basic_publish(
                        exchange='amq.topic',
                        routing_key='%s.slowquery' % args.routingrabbit,

                        body=json.dumps(datajson),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # make message persistent
                        ))
            else:
                pass

        time.sleep (float(args.interval))
#-------------------------------------------mysql
# mysqladmin -u root -p -i 1 processlist
# python3.7 slow_query.py -i 2 -hostdb localhost -portdb 3306 -user root -pass x -th 3 -t mysql -o 0.0.0.0 -rr autopay -sk 0df684f0-6560-4c47-9f11-75185d3091df
if args.type == 'mysql':
    while True:
        import mysql.connector

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=args.host))

        channel_http = connection.channel()
        channel_http.queue_declare(queue='%s.slowquery' % args.routingrabbit, durable=True)

        # Connect to server
        cnx = mysql.connector.connect(
            host= args.hostdb,
            port= int(args.portdb),
            user= args.username ,
            password= args.password)

        # Get a cursor
        cur = cnx.cursor()

        # Execute a query
        cur.execute("show full processlist")

        # Fetch one result
        row = cur.fetchone()
        if row[4] == 'Query':
            if row[5] >= int(args.threshold):
                print('slow')
                print(row)

                datasave = {
                    "socket_key"     : args.socketkey,
                    "hostname"      : socket.gethostname(),
                    # "hostname"      : 'Host-002-AP',
                    "ip"            : socket.gethostbyname(socket.gethostname()),
                    "type_db"     : 0,
                    "command"    : row[7],
                    "time"    : row[5],
                    "create"     : datetime.now() ,

                    "host" : args.host,
                    "routing_key" : args.routingrabbit,
                    }

                datajson = {
                    "socket_key"     : args.socketkey,
                    "hostname"      : socket.gethostname(),
                    # "hostname"      : 'Host-002-AP',
                    "ip"            : socket.gethostbyname(socket.gethostname()),
                    "type_db"     : 0,
                    "command"    : row[7],
                    "time"    : row[5],
                    "create"     : format(datetime.now() ),
                    }

                API_ENDPOINT = "https://autopay.env-playground.online/app/db"
                r = requests.post(url = API_ENDPOINT, data = datasave)

                channel_http.basic_publish(
                    exchange='amq.topic',
                    routing_key='%s.slowquery' % args.routingrabbit,
                    body=json.dumps(datajson),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    ))
        else:
            print('null query')

        # Close connection
        cnx.close()
        time.sleep (float(args.interval))
