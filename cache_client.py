import sys
import socket
from collections import OrderedDict

from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_GET, serialize_PUT, serialize_DELETE, serialize, hash_code_hex
from node_ring import NodeRing

from bloom_filter import BloomFilter
from lru_cache import lru_cache


BUFFER_SIZE = 1024

class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)       

    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()

bloomfilter = BloomFilter(4000, 0.05)

# only sends GET to node if user is not in cache
@lru_cache(5)
def get(userHashcode, client_ring):
    # only send GET if user is in bloom filter
    if(bloomfilter.is_member(userHashcode)):
        data_bytes, key = serialize_GET(userHashcode)
        response = client_ring.get_node(key).send(data_bytes)
        return response
    else:
        return None
    
# puts user on node
def put(user, client_ring):
    userBytes = serialize(user)
    userHashcode = hash_code_hex(userBytes)
    bloomfilter.add(userHashcode)

    data_bytes, key = serialize_PUT(user)
    response = client_ring.get_node(key).send(data_bytes)
    return response

# delete user from node
def delete(userHashcode, client_ring):
    if(bloomfilter.is_member(userHashcode)):
        data_bytes, key = serialize_DELETE(userHashcode)
        response = client_ring.get_node(key).send(data_bytes)
        return response
    else:
        return None

def process(udp_clients):
    client_ring = NodeRing(udp_clients)
    hash_codes = set()

    # PUT all users.
    for u in USERS:
        response = put(u, client_ring)
        print(response)
        hash_codes.add(str(response.decode()))

    print(f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")
    
    # GET all users.
    for hc in hash_codes:
        print(hc)
        response = get(hc, client_ring)
        print(response)
    
    # DELETE all users
    for hc in hash_codes:
        print(hc)
        response = delete(hc, client_ring)
        print(response)


if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])
        for server in NODES
    ]
    process(clients)
