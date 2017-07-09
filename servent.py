#!/usr/bin/env python3

"""Key-value based P2P servent"""

import sys
import socket
import struct
from pprint import pprint

__author__ = "João Francisco Martins, Lorena Cerbino and Victor Bernardo Jorge"

#==================================FUNCTIONS==================================#

def parse_keyvalues(keyvalues_file):
    """Read the correspondent file and parse the key-values.
    
    Arguments:
        keyvalues_file -- Key-values file name.
    """

    keyvalues = {}
    with open(keyvalues_file, "r") as f:
        for line in f:
            if line[0] != "#":
                key, value = line.strip().split(maxsplit=1)
                keyvalues[key] = value

    return keyvalues


def create_query(msg):
    """Generate QUERY type message with given parameters.
    
    Arguments:
        msg -- Dictionary that contains the message parameters.
    """

    QUERY = struct.pack("!H", 2)
    ttl = struct.pack("!H", msg["ttl"])
    ip = socket.inet_aton(msg["ip"])
    port = struct.pack("!H", msg["port"])
    seq_number = struct.pack("!I", msg["seq_number"])
    key = msg["key"].encode("UTF-8")
    query = QUERY + ttl + ip + port + seq_number + key + b"\0"
    
    return query


def create_response(key, value):
    """Generate RESPONSE type message with given key-value pair.
    
    Arguments:
        key -- Received key.
        value -- Requested value.
    """

    RESPONSE = struct.pack("!H", 3)
    response = (RESPONSE + key.encode("UTF-8") + b"\t" + value.encode("UTF-8")
                + b"\0")

    return response


def send_msg(sock, msg, ip, port):
    """Sends message msg to IP ip, PORT port via UDP socket sock.
    
    Arguments:
        sock -- UDP socket.
        msg -- Message to be sent.
        ip -- Destination IP.
        port -- Destination PORT.
    """

    sock.sendto(msg, (ip, port))


def rcv_msg(sock):
    """Receives message via UDP socket sock.
    
    Arguments:
        sock -- UDP socket.
    """

    data, addr = sock.recvfrom(2 + 2 + 4 + 2 + 4 + 40 + 1) # Max len = 55 bytes

    msg_type = struct.unpack("!H", data[0:2])[0]

    if msg_type == 1:  # Message has type CLIREQ
        msg = {
            "type": msg_type,
            "ip": addr[0],
            "port": addr[1],
            "key": data[2:].decode("UTF-8").split("\0")[0]
        }

        print("Client request for key \"{}\" from {}:{}.".format(
              msg["key"], msg["ip"], msg["port"]))
    else:  # Message has type QUERY
        msg = {
            "type": msg_type,
            "ttl": struct.unpack("!H", data[2:4])[0],
            "ip": socket.inet_ntoa(data[4:8]),
            "port": struct.unpack("!H", data[8:10])[0], 
            "seq_number": struct.unpack("!I", data[10:14])[0], 
            "key": data[14:].decode("UTF-8").split("\0")[0]
        }

        print("New query for key \"{}\" from servent {}:{}. Received query"
              " has sequence number {} and TTL {}.". format(msg["key"], 
              addr[0], addr[1], msg["seq_number"], msg["ttl"]))

    return msg


def flood_reliably(sock, query, peers, source):
    """OSPF like reliable flooding to assigned peers.
    
    Arguments:
        sock -- UDP socket.
        query -- QUERY type message.
        peers -- Peers assigned to this servent.
        source -- Address for servent that sent the query.
    """

    if peers:
        print("Flooding peers")
    for peer in peers:
        if peer != source:  # Prevents loops in the network
            ip, port = peer.split(":")
            print("QUERY sent to {}:{}".format(ip, port))
            send_msg(sock, query, ip, int(port))


def retrieve_value(msg, keyvalues, sock):
    """Lookup key in personal dictionary and send it to client if present.

    Arguments:
        msg -- Received query.
        keyvalues -- Key-values dictionary.
        sock -- UDP socket.
    """

    if msg["key"] in keyvalues:
        response = create_response(msg["key"], 
                                   keyvalues[msg["key"]])
        send_msg(sock, response, msg["ip"], msg["port"])
        print("RESPONSE sent to {}:{}".format(msg["ip"], msg["port"]))

#====================================MAIN=====================================#

def main(args):
    # Parse args
    port = int(args[0])
    keyvalues_file = args[1]
    keyvalues = parse_keyvalues(keyvalues_file)
    pprint(keyvalues)
    peers = []
    for i in range(2, len(args)):
        peers.append(args[i])

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 
                             socket.IPPROTO_UDP)
    # Prevents "Address already in use" error
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", port))

    seen = set()  # Seen queries
    seq_number = 0 

    while True:
        try:
            msg = rcv_msg(sock)

            if msg["type"] == 1:  # Message has type CLIREQ
                msg["ttl"] = 3
                msg["seq_number"] = seq_number
                seq_number += 1

                query = create_query(msg)
                flood_reliably(sock, query, peers, "{}:{}".format(msg["ip"], 
                                                                  msg["port"]))

                retrieve_value(msg, keyvalues, sock)
            elif msg["type"] == 2:  # Message has type QUERY
                query = "{},{},{},{}".format(msg["ip"], msg["port"], 
                                             msg["seq_number"], msg["key"])
                if query not in seen:
                    seen.add(query)
                    msg["ttl"] -= 1

                    if msg["ttl"] > 0:  # Time to live didn't expire
                        updated_query = create_query(msg)
                        flood_reliably(sock, updated_query, peers,
                                       "{}:{}".format(msg["ip"], msg["port"]))
                    else:
                        print("Time to live expired.")

                    retrieve_value(msg, keyvalues, sock)
                else:
                    print("Query already seen.")

        except KeyboardInterrupt:
            sock.close()
            break


if __name__ == "__main__":
    main(sys.argv[1:])