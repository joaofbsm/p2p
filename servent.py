#!/usr/bin/env python3

"""Key-value based P2P servent"""

import sys
import socket
import struct

__author__ = "João Francisco Martins, Victor Bernardo Jorge and Lorena Cerbino"

# TODO
# - Maybe use argparse
# - Change how it is being done to check seq_number

# QUESTIONS
# - Does python needs to set null char at the end of strings?
# - Wouldn't it be 202 bytes tops?
# - Why does the received port number does not match?

#==================================FUNCTIONS==================================#

def parse_keyvalues(keyvalues_file):
    keyvalues = {}
    with open(keyvalues_file, "r") as f:
        for line in f:
            if line[0] != "#":
                key, value = line.strip().split(maxsplit=1)
                keyvalues[key] = value

    print(keyvalues)

    return keyvalues


def create_query(msg):
    QUERY = struct.pack("!H", 2)
    ttl = struct.pack("!H", msg["ttl"])
    ip = socket.inet_aton(msg["ip"])
    port = struct.pack("!H", msg["port"])
    seq_number = struct.pack("!I", msg["seq_number"])
    key = msg["key"].encode("UTF-8")
    query = QUERY + ttl + ip + port + seq_number + key
    
    return query


def create_response(key, value):
    RESPONSE = struct.pack("!H", 3)
    response = (RESPONSE + key.encode("UTF-8") + b" " + value.encode("UTF-8")
               + b"\0")

    return response


def send_msg(sock, msg, ip, port):
    sock.sendto(msg, (ip, port))


def rcv_msg(sock):
    data, addr = sock.recvfrom(2 + 2 + 4 + 2 + 4 + 40)  # Max len = 54 bytes

    msg_type = struct.unpack("!H", data[0:2])[0]

    if msg_type == 1:  # Message has type CLIREQ
        msg = {
            "type": msg_type,
            "ip": addr[0],
            "port": addr[1],
            "seq_number": -1,  # If new query, seq_number starts at -1
            "key": data[2:].decode("UTF-8")
        }
    else:  # Message has type QUERY
        msg = {
            "type": msg_type,
            "ttl": struct.unpack("!H", data[2:4])[0],
            "ip": socket.inet_ntoa(data[4:8])[0],
            "port": struct.unpack("!H", data[8:10])[0], 
            "seq_number": struct.unpack("!I", data[10:14])[0], 
            "key": data[14:].decode("UTF-8")
        }

    return msg


def flood_reliably(sock, query, peers, source):
    print("Flooding peers")
    for peer in peers:
        if peer != source:
            ip, port = peer.split(":")
            send_msg(sock, query, ip, int(port))

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
    # Prevents "Address already in use" error
    readable.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    readable.bind(("", port))

    writable = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 
                             socket.IPPROTO_UDP)

    seen = set()  # Seen queries
    seq_number = 0

    while True:
        msg = rcv_msg(readable)
        query = (msg["ip"] + str(msg["port"]) + str(msg["seq_number"]) 
                 + msg["key"])
        if query not in seen:
            seen.add(query)
            print("New query for key \"", msg["key"], "\" from ", msg["ip"], 
                  ":", msg["port"], sep="")

            if msg["type"] == 1:
                msg["ttl"] = 3
                msg["seq_number"] = seq_number
            elif msg["type"] == 2:
                print("Current query has TTL", msg["ttl"])
                msg["ttl"] -= 1

            if msg["ttl"] > 0:
                new_query = create_query(msg)
                flood_reliably(writable, new_query, peers,
                               (msg["ip"] + ":" + str(msg["port"])))
                
            if msg["key"] in keyvalues:
                response = create_response(msg["key"], keyvalues[msg["key"]])
                send_msg(writable, response, msg["ip"], msg["port"])
        else:
            print("Query already seen.")

    readable.close()
    writable.close()

if __name__ == "__main__":
    main(sys.argv[1:])