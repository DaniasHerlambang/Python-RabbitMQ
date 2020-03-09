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
parser.add_argument("-rr","--routingrabbit", help="threshold")
parser.add_argument("-user","--username", help="username")
parser.add_argument("-pass","--password", help="password")
parser.add_argument("-port","--port", help="set port")
parser.add_argument("-i","--interval", help="set port")
parser.add_argument("-t","--type", help="type db (mongo / mysql)")
parser.add_argument("-th","--threshold", help="threshold")
parser.add_argument("-db","--database", help="database name")

args = parser.parse_args()
# args.password
#*******************************************************************************************************

#-------------------------------------------mongodb
# python3.7 slow_query.py -i 5 -th 10 -t mongo -o 0.0.0.0 -rr autopay -db onepush
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
            # print(k)
            # print('\n')
            if len(k) == 23:
                if k['secs_running'] >= int(args.threshold):
                    print(k['command'])
                    print('\n')
                    print(k['secs_running'])

                    datasave = {
                        "socket_key"     : '0df684f0-6560-4c47-9f11-75185d3091df',
                        # "hostname"      : socket.gethostname(),
                        "hostname"      : 'Host-002-AP',
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
                        "socket_key"     : '0df684f0-6560-4c47-9f11-75185d3091df',
                        # "hostname"      : socket.gethostname(),
                        "hostname"      : 'Host-002-AP',
                        "ip"            : socket.gethostbyname(socket.gethostname()),
                        "type_db"     : 1,
                        "pid"         : int(k['opid']),
                        "command"    : str(k['command']),
                        "time"    : k['secs_running'],
                        "create"     : format(datetime.now() ),
                        }

                    # print(datajson)

                    API_ENDPOINT = "http://127.0.0.1:8000/app/db"
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
# python3.7 slow_query.py -i 2 -th 3 -t mysql
if args.type == 'mysql':
    while True:
        import mysql.connector

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='0.0.0.0'))

        channel_http = connection.channel()
        channel_http.queue_declare(queue='autopay.slowquery', durable=True)

        # Connect to server
        cnx = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="root",
            password="x")

        # Get a cursor
        cur = cnx.cursor()

        # Execute a query
        # cur.execute("use dd_switcher")
        cur.execute("show full processlist")

        # Fetch one result
        row = cur.fetchone()
        # print(row)
        # print("Current date is: {0}".format(row[0]))
        if row[4] == 'Query':
            if row[5] >= int(args.threshold):
                print('slow')
                print(row)

                datasave = {
                    "socket_key"     : '0df684f0-6560-4c47-9f11-75185d3091df',
                    # "hostname"      : socket.gethostname(),
                    "hostname"      : 'Host-002-AP',
                    "ip"            : socket.gethostbyname(socket.gethostname()),
                    "type_db"     : 0,
                    "command"    : row[7],
                    "time"    : row[5],
                    "create"     : datetime.now() ,

                    "host" : args.host,
                    "routing_key" : args.routingrabbit,
                    }

                datajson = {
                    "socket_key"     : '0df684f0-6560-4c47-9f11-75185d3091df',
                    # "hostname"      : socket.gethostname(),
                    "hostname"      : 'Host-002-AP',
                    "ip"            : socket.gethostbyname(socket.gethostname()),
                    "type_db"     : 0,
                    "command"    : row[7],
                    "time"    : row[5],
                    "create"     : format(datetime.now() ),
                    }

                # print(datajson)

                API_ENDPOINT = "http://127.0.0.1:8000/app/db"
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
