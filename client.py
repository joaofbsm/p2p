#!/usr/bin/env python3

"""Key-value based P2P client"""

import sys
import socket
import struct

__author__ = "João Francisco Martins, Victor Bernardo Jorge and Lorena Cerbino"

#==================================FUNCTIONS==================================#

def create_msg(key):
    msg = struct.pack("!H", 1) + key
    return msg


def send_msg(sock, msg, ip, port):
    retransmission = False

    while True:
        try:
            sock.sendto(msg, (ip, port))
        except socket.timeout:
            if not retransmission:
                retransmission = True
            else:
                break


def rcv_msg(sock):
    while True:
        try:
            data, addr = sock.receivefrom()
            print(addr, "response:", data)
        except socket.timeout:
            print("No more responses.")
            break

#====================================MAIN=====================================#

def main(args):
    servent_addr = args[0] 
    servent_addr = servent_addr.split(":")
    servent_ip = servent_addr[0]
    servent_port = servent_addr[1]
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.settimeout(4)  # 4 seconds timeout

     key = input("Type a key to be retrieved: ")
     
     msg = create_msg(key)

     send_msg(sock, msg, servent_ip, servent_port)

     rcv_msg(sock)
     

if __name__ == "__main__":
    main(sys.argv[1:])