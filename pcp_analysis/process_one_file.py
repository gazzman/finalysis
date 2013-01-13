#!/usr/bin/python
from time import sleep
import os
import socket
import sys

HOST = sys.argv[1]
PORT = sys.argv[2]
fname = sys.argv[3]

fname = os.path.abspath(fname)
if len(sys.argv) == 5: conprocs = int(sys.argv[4])
else: conprocs = 1

if len(sys.argv) == 6: size_mb = int(sys.argv[5])/(1024.0*1024.0) # byte to Mi
else: size_mb = 1.0

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))
sock.sendall(fname + '\n')
sock.close()

# Estimated processing time for an Intel core i5-2400 with 16Gi RAM
sleep_time = (4.02*conprocs 
            - 2.66*size_mb
            + 0.1*conprocs**2 
            + 0.54*size_mb**2
            + .3*size_mb*conprocs
            - 1.9)

print 'Processing %s and sleeping for %0.2f' % (fname, sleep_time)
sleep(sleep_time)
