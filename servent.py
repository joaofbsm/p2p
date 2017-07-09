#!/usr/bin/env python3

"""Key-value based P2P servent"""

import sys
import socket
import struct

__author__ = "João Francisco Martins, Victor Bernardo Jorge and Lorena Cerbino"

# TODO
# - Maybe use argparse

#==================================FUNCTIONS==================================#

def parse_keyvalues(keyvalues_file):
    pass


def create_msg():
    pass


def update_msg():
    pass

#====================================MAIN=====================================#

def main(args):
    # Parse args
    port = int(args[0])
    keyvalues_file = args[1]
    keyvalues = parse_keyvalues(keyvalues_file)
    peers = []
    for i in range(2, len(args)):
        peers.append(args[i])

    readable = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 
                             socket.IPPROTO_UDP)
    readable.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    readable.bind(("", port))

    writable = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 
                             socket.IPPROTO_UDP)

    seq_number = 0
    


if __name__ == "__main__":
    main(sys.argv[1:])