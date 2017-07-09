#!/usr/bin/env python3

"""Key-value based P2P client"""

import sys
import socket
import struct

__author__ = "João Francisco Martins, Victor Bernardo Jorge and Lorena Cerbino"

#==================================FUNCTIONS==================================#

def create_msg(key):
    """Generate CLIREQ type message with given key.
    
    Arguments:
        key -- Key assigned to the wanted value.
    """

    CLIREQ = struct.pack("!H", 1)
    key = key.encode("UTF-8")
    msg = CLIREQ + key + b"\0"

    return msg


def send_msg(sock, msg, ip, port):
    """Sends UDP message with at most one retransmission.
    
    Arguments:
        sock -- UDP socket.
        msg -- Message to be sent.
        ip -- Destination IP.
        port -- Destination PORT.
    """

    retransmission = False
    while True:
        sock.sendto(msg, (ip, port))
        responses = rcv_msg(sock)
        if not responses:  # 0 responses received for query
            if not retransmission:
                retransmission = True
            else:
                return
        else:
            return

def rcv_msg(sock):
    """Receives messages with timeout of 4 seconds
    
    Arguments:
        sock -- UDP socket.
    """
    
    responses = 0
    while True:
        try:
            data, addr = sock.recvfrom(2 + 40 + 1 + 160 + 1)  # Max len = 204
            responses += 1
            response = data[2:].decode("UTF-8")
            print(addr, "response:", response)
        except socket.timeout:
            if responses > 0:
                print("No more responses.")
            else:
                print("No responses received.")
            return responses

#====================================MAIN=====================================#

def main(args):
    servent_addr = args[0] 
    servent_addr = servent_addr.split(":")
    servent_ip = servent_addr[0]
    servent_port = int(servent_addr[1])
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.settimeout(4)  # 4 seconds timeout

    while True:
        key = input("Type a key to be retrieved: ")
         
        msg = create_msg(key)

        send_msg(sock, msg, servent_ip, servent_port)

    sock.close()


if __name__ == "__main__":
    main(sys.argv[1:])