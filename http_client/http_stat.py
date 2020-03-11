import re
from collections import Counter
from collections import namedtuple
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
from collections import Counter
import datetime
from datetime import datetime
from datetime import timedelta
import psycopg2

import time
from optparse import OptionParser
# chmod a+x "name".py

open('count.log', 'r+').truncate(0)              #kosongkan "count.log"
open('date.log', 'r+').truncate(0)              #kosongkan "date.log"
open('real_date.log', 'r+').truncate(0)

SLEEP_INTERVAL = 1.0

def readlines_then_tail(fin):
    "Iterate through lines and then tail for further lines."
    while True:
        line = fin.readline()
        if line:
            yield line
        else:
            with open('real_date.log','r') as reader:
                from datetime import datetime
                date_x =  reader.read()
                if date_x == '':
                    with open('real_date.log','w+') as f:
                        f.write("%s" % datetime.now())
                else:
                    with open('real_date.log','r') as reader:
                        from datetime import datetime
                        date_read =  reader.read()
                        date_s = datetime.strptime(date_read, "%Y-%m-%d  %H:%M:%S.%f")

                        if date_s.year == datetime.now().year:
                            pass
                            if date_s.month == datetime.now().month:
                                pass
                                if date_s.day == datetime.now().day:
                                    pass
                                else:
                                    print('ganti tanggal')
                                    open('real_date.log', 'r+').truncate(0)
                                    quit()
                            else:
                                print('ganti bulan')
                                open('real_date.log', 'r+').truncate(0)
                                quit()
                        else:
                            print('ganti tanggal')
                            open('real_date.log', 'r+').truncate(0)
                            quit()

            tail(fin)

def tail(fin):
    "Listen for new lines added to file."
    while True:
        where = fin.tell()
        line = fin.readline()
        if not line:
            time.sleep(SLEEP_INTERVAL)
            fin.seek(where)
        else:
            yield line

def kosongkan(data):
    open('date.log', 'r+').truncate(0)              #kosongkan "date.log"
    with open('date.log','w+') as f:
         f.write("%s" % data['date'])

def send_data(kirim_hitungan , kirim_waktu):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='0.0.0.0'))

    channel_http = connection.channel()
    channel_http.queue_declare(queue='autopay.httpstats', durable=True)
    datajson = {
        "socket_key"     : '34c9efc9-8996-4a8e-b58e-2c1dfa3a56e8',
        # "hostname"      : socket.gethostname(),
        "hostname"      : 'Host-AP-001',
        "ip"            : socket.gethostbyname(socket.gethostname()),
        "req_total"     : int(kirim_hitungan),
        "req_succes"    : int(kirim_hitungan),
        "req_failed"    : 0,
        "timestamp"     : format(datetime.strptime(kirim_waktu, '%d/%b/%Y:%H:%M:%S')),
        }
    datasave = {
        "socket_key"     : '34c9efc9-8996-4a8e-b58e-2c1dfa3a56e8',
        # "hostname"      : socket.gethostname(),
        "hostname"      : 'Host-AP-001',
        "ip"            : socket.gethostbyname(socket.gethostname()),
        "req_total"     : int(kirim_hitungan),
        "req_succes"    : int(kirim_hitungan),
        "req_failed"    : 0,
        "timestamp"     : datetime.strptime(kirim_waktu, '%d/%b/%Y:%H:%M:%S'),
        }

    API_ENDPOINT = "http://127.0.0.1:8000/app/http"
    r = requests.post(url = API_ENDPOINT, data = datasave)

    channel_http.basic_publish(
        exchange='amq.topic',
        routing_key='autopay.httpstats',
        body=json.dumps(datajson),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))

def main():
    p = OptionParser("usage: tail.py file")
    (options, args) = p.parse_args()
    if len(args) < 1:
        p.error("must specify a file to watch")

    #parsing dan replace path sesuai datetime.now() !!!
    import datetime
    from datetime import datetime
    waktu_set = ["{date}","{month}", "{year}"]
    waktu_now = [str(datetime.now().day), str(datetime.now().month), str(datetime.now().year)]
    # waktu_now = [datetime.now().day, datetime.now().month, datetime.now().year]
    waktu_dynamic = args[0]
    for l, s in enumerate(waktu_set):
        waktu_dynamic = waktu_dynamic.replace(s,waktu_now[l])

    # print(args[0])
    with open(waktu_dynamic, 'r') as fin:
        for line in readlines_then_tail(fin):
            # print (line.strip())

            LOG_REGEX = '(?P<ip>.*) - - \[(?P<date>.*?) +(.*?)\] "(?P<method>\w+) (?P<request_path>.*?) HTTP/(?P<http_version>.*?)" (?P<status_code>\d+) (?P<response_size>.*?) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'
            compiled = re.compile(LOG_REGEX)

            import datetime
            match = compiled.match(line.strip())
            data = match.groupdict()

            with open('date.log','r') as reader:
                patokan_waktu =  reader.read()

                xx_date = datetime.datetime.strptime(data['date'], '%d/%b/%Y:%H:%M:%S')
                if patokan_waktu == '' :
                    #menulis di date.log daftar clien
                    with open('date.log','w+') as f:
                         f.write("%s" % data['date'])
                    with open('date.log','r') as reader:
                        patokan_waktu =  reader.read()
                    date_now_request = datetime.datetime.strptime(patokan_waktu, '%d/%b/%Y:%H:%M:%S')
                    print('kosong')
                    if date_now_request.year == xx_date.year :
                        if date_now_request.month == xx_date.month :
                            if  date_now_request.day == xx_date.day :
                                if date_now_request.hour == xx_date.hour and date_now_request.minute == xx_date.minute :
                                    print('sama ' , date_now_request , xx_date)
                                    with open('count.log','w+') as f:
                                         f.write("%s" % 1)
                                    with open('count.log','r') as reader:
                                        hitungan =  reader.read()
                                else:
                                    kosongkan(data['date'])
                                    print('beda')
                        else:
                            kosongkan(data['date'])
                            print('beda')
                    else:
                        kosongkan(data['date'])
                        print('beda')
                else:
                    date_now_request = datetime.datetime.strptime(patokan_waktu, '%d/%b/%Y:%H:%M:%S')
                    if date_now_request.year == xx_date.year :
                        # print('as')
                        pass
                        if date_now_request.month == xx_date.month :
                            # print('ass')
                            pass
                            if  date_now_request.day == xx_date.day :
                                # print('asss')
                                pass
                                if date_now_request.hour == xx_date.hour and date_now_request.minute == xx_date.minute :
                                    # print('sama ' , date_now_request , xx_date)
                                    with open('count.log','r') as reader:
                                        hitungan =  reader.read()
                                    open('count.log', 'r+').truncate(0)              #kosongkan "count.log"
                                    with open('count.log','w+') as f:
                                         f.write("%s" % str(int(hitungan)+1))
                                    # print(hitungan)
                                else:
                                    print('beda1' , data['date'])
                                    with open('count.log','r') as reader:
                                        kirim_hitungan =  reader.read()
                                    with open('date.log','r') as reader:
                                        kirim_waktu =  reader.read()
                                    # print(line.strip() , 'cek cek cek ')
                                    if datetime.datetime.strptime(str(datetime.datetime.strptime(kirim_waktu, '%d/%b/%Y:%H:%M:%S')), '%Y-%m-%d  %H:%M:%S') < datetime.datetime.now():
                                        print('aasas', kirim_hitungan , kirim_waktu)
                                    else:
                                        send_data(kirim_hitungan , kirim_waktu)

                                    open('date.log', 'r+').truncate(0)              #kosongkan "date.log"
                                    with open('date.log','w+') as f:
                                         f.write("%s" % data['date'])

                                    open('count.log', 'r+').truncate(0)              #kosongkan "count.log"
                                    with open('count.log','w+') as f:
                                         f.write("%s" % 1)
                            else:
                                print('beda2')
                                with open('count.log','r') as reader:
                                    kirim_hitungan =  reader.read()
                                with open('date.log','r') as reader:
                                    kirim_waktu =  reader.read()
                                # print(line.strip() , 'cek cek cek ')
                                if datetime.datetime.strptime(str(datetime.datetime.strptime(kirim_waktu, '%d/%b/%Y:%H:%M:%S')), '%Y-%m-%d  %H:%M:%S') < datetime.datetime.now():
                                    print('aasas', kirim_hitungan , kirim_waktu)
                                else:
                                    send_data(kirim_hitungan , kirim_waktu)

                                open('date.log', 'r+').truncate(0)              #kosongkan "date.log"
                                with open('date.log','w+') as f:
                                     f.write("%s" % data['date'])

                                open('count.log', 'r+').truncate(0)              #kosongkan "count.log"
                                with open('count.log','w+') as f:
                                     f.write("%s" % 1)
                        else:
                            print('beda3')
                            with open('count.log','r') as reader:
                                kirim_hitungan =  reader.read()
                            with open('date.log','r') as reader:
                                kirim_waktu =  reader.read()
                            # print(line.strip() , 'cek cek cek ')
                            if datetime.datetime.strptime(str(datetime.datetime.strptime(kirim_waktu, '%d/%b/%Y:%H:%M:%S')), '%Y-%m-%d  %H:%M:%S') < datetime.datetime.now():
                                print('aasas', kirim_hitungan , kirim_waktu)
                            else:
                                send_data(kirim_hitungan , kirim_waktu)

                            open('date.log', 'r+').truncate(0)              #kosongkan "date.log"
                            with open('date.log','w+') as f:
                                 f.write("%s" % data['date'])

                            open('count.log', 'r+').truncate(0)              #kosongkan "count.log"
                            with open('count.log','w+') as f:
                                 f.write("%s" % 1)
                    else:
                        if date_now_request.year != xx_date.year :
                            print('beda4')

                            with open('count.log','r') as reader:
                                kirim_hitungan =  reader.read()
                            with open('date.log','r') as reader:
                                kirim_waktu =  reader.read()
                            # print(line.strip() , 'cek cek cek ')
                            if datetime.datetime.strptime(str(datetime.datetime.strptime(kirim_waktu, '%d/%b/%Y:%H:%M:%S')), '%Y-%m-%d  %H:%M:%S') < datetime.datetime.now():
                                print('aasas', kirim_hitungan , kirim_waktu)
                            else:
                                send_data(kirim_hitungan , kirim_waktu)

                            open('date.log', 'r+').truncate(0)              #kosongkan "date.log"
                            with open('date.log','w+') as f:
                                 f.write("%s" % data['date'])

                            open('count.log', 'r+').truncate(0)              #kosongkan "count.log"
                            with open('count.log','w+') as f:
                                 f.write("%s" % 1)

if __name__ == '__main__':
    main()
