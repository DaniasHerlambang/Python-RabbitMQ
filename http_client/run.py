from subprocess import call
import subprocess
import argparse
from optparse import OptionParser
from datetime import datetime
import time

p = OptionParser("usage: run.py path")
(options, args) = p.parse_args()
if len(args) < 1:
    p.error("must specify a file to watch")

while True:
  print("Launching process..")
  p = subprocess.Popen("python3.7 http_stat.py %s" % args[0], shell=True)
  p.wait()
  print("Process change the date, search file. Restarting in 2 seconds..")
  time.sleep(2)
