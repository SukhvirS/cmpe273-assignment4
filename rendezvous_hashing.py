from pickle_hash import hash_code_hex

from server_config import NODES

class RendezvousHashing():
    def __init__(self, nodes):
        self.nodes = nodes
    
    # gets node with highest random weight (hrw)
    def get_node(self, key_hex):
        hrw = 0
        bestNode = None
        for node in self.nodes:
            # combines key and node with a delimeter (,) and converts it into a string for hashing
            key_node_combo = key_hex + ',' + str(node)
            weight_hex = hash_code_hex(key_node_combo.encode())
            weight = int(weight_hex, 16)
            if(weight > hrw):
                hrw = weight
                bestNode = node
        return bestNode


def test():
    rh = RendezvousHashing(NODES)
    x = hash_code_hex('dog'.encode())
    print(rh.get_node(x))

# test()
