import os
import socket
import json
import base64
import shutil
import time
import argparse

#*******************************************************************************************************88
parser = argparse.ArgumentParser()
parser.add_argument("-i", "--interval",  help="set interval")
parser.add_argument("-ssi", "--server_socket_ip",  help="set server-socket-ip")
parser.add_argument("-ssp", "--server_socket_port",  help="set server-socket-port")

# read arguments from the command line
args = parser.parse_args()

# call back example : args.interval
#*******************************************************************************************************88

while True:
    s = socket.socket()
    host = args.server_socket_ip #'127.0.1.1'
    port = int(args.server_socket_port) #12345
    s.connect((host, port))

    #Disk
    total_disk, used_disk, free_disk = shutil.disk_usage("/")
    data_total_disk = "%d GB" % (total_disk // (2**30))
    data_used_disk =  "%d GB" % (used_disk // (2**30))
    data_free_disk =  "%d GB" % (free_disk // (2**30))

    #Memory
    total_memory, used_memory, free_memory = map(int, os.popen('free -t -g').readlines()[-1].split()[1:])
    data_total_memory = "%d GB" % (total_memory)
    data_used_memory =  "%d GB" % (used_memory)
    data_free_memory =  "%d GB" % (free_memory)

    #Cpu
    CPU=str(round(float(os.popen('''grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage }' ''').readline()),2))
    data_cpu = CPU + "%"

    #Json
    datajson = {
        "HOSTNAME"      : socket.gethostname(),
        "IP"            : socket.gethostbyname(socket.gethostname()),
        "CPU"           : data_cpu,
        "DISK_TOTAL"    : data_total_disk,
        "DISK_USED"     : data_used_disk,
        "DISK_AVAILABLE": data_free_disk,
        "RAM_TOTAL"     : data_total_memory,
        "RAM_USED"      : data_used_memory,
        "RAM_AVAILABLE" : data_free_memory,
        }

    s.sendall(json.dumps(datajson).encode("utf-8"))

    # s.send('data from client!'.encode()) # kirim data ke server
    message_from_server = s.recv(1024).decode() #message from server
    print (message_from_server)

    s.close()
    time.sleep (float(args.interval)) #looping setiap berapa detik

#*******************************************************************************************************88
