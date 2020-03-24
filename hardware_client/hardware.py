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

#*******************************************************************************************************88
parser = argparse.ArgumentParser()
parser.add_argument("-i", "--interval",  help="set interval")
parser.add_argument("-host", "--host",  help="set host")
parser.add_argument("-rr","--routingrabbit", help="routing rabbit")
parser.add_argument("-sk","--socketkey", help="socket vm")

args = parser.parse_args()

# call back example : args.interval
#*******************************************************************************************************88
while True:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=args.host))

    channel1 = connection.channel()
    channel1.queue_declare(queue='%s.hwstats' % str(args.routingrabbit) , durable=True)

#*******************************************************************************************************88

#Disk
    total_disk, used_disk, free_disk = shutil.disk_usage("/")
    data_total_disk = "%d" % (total_disk // (2**30))
    data_used_disk =  "%d" % (used_disk // (2**30))
    data_free_disk =  "%d" % (free_disk // (2**30))

    #Memory
    total_memory, used_memory, free_memory = map(int, os.popen('free -t -g').readlines()[-1].split()[1:])
    data_total_memory = "%d" % (total_memory)
    data_used_memory =  "%d" % (used_memory)
    data_free_memory =  "%d" % (free_memory)

    #Cpu
    CPU=int(round(float(os.popen('''grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage }' ''').readline()),2))
    data_cpu = CPU

    #*******************************************************************************************************88

    #Json
    datajson = {
        # "id"            : new_id + 1,
        # "vm_id"         :  2,
        "socket_key"     : str(args.socketkey),
        "hostname"      : socket.gethostname(),
        # "hostname"      : 'Host-AP-001',
        "ip"            : socket.gethostbyname(socket.gethostname()),
        "cpu"           : data_cpu,
        "disk_total"    : data_total_disk,
        "disk_used"     : data_used_disk,
        "disk_available": data_free_disk,
        "ram_total"     : data_total_memory,
        "ram_used"      : data_used_memory,
        "ram_available" : data_free_memory,
        }
    #*******************************************************************************************************88
    datasave = {
        # "id"            : new_id + 1,
        # "vm_id"         :  2,
        "socket_vm"     : str(args.socketkey),
        "hostname"      : socket.gethostname(),
        # "hostname"      : 'Host-AP-001',
        "ip"            : socket.gethostbyname(socket.gethostname()),
        "cpu"           : data_cpu  ,
        "disk_total"    : total_disk // (2**30),
        "disk_used"     : used_disk // (2**30),
        "disk_available": free_disk // (2**30),
        "ram_total"     : total_memory,
        "ram_used"      : used_memory,
        "ram_available" : free_memory,
        # "create"        : datetime.now()
        }

    API_ENDPOINT = "https://autopay.env-playground.online/api/app/socket"
    r = requests.post(url = API_ENDPOINT, data = datasave)
    socket_url = r.text
    print("The socket URL is:%s"%socket_url)

    #*******************************************************************************************************88

    channel1.basic_publish(
        exchange='amq.topic',
        routing_key='%s.hwstats' % str(args.routingrabbit),
        body=json.dumps(datajson),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))

    print(" [x] Sent %r" % datajson)
    connection.close()
    time.sleep (float(args.interval))
