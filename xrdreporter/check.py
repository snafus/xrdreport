import os
import socket
import sys

from enum import Enum
# check that a "pong" is return from a "ping" to the server

class Response(Enum):
    OK=0
    TIMEOUT=1
    FAILURE=2

def send_ping(addr: tuple):
    #Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #Set a timeout value of 1 second
    sock.settimeout(1)
    message = 'ping'.encode('utf-8')
    response_check = 'pong'.encode('utf-8')

    sock.sendto(message, addr)
    try:
        data, server = sock.recvfrom(1024)
    #If data is not received back from server, print it has timed out  
    except socket.timeout:
        print('Failed to ping')
        return Response.TIMEOUT

    if data.strip() != response_check:
        print("missmatch message")
        return Response.FAILURE
    sock.close()

    return Response.OK


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])

    addr = (host, port)

    reponse = send_ping(addr)
    sys.exit(reponse.value)
