from pickle_hash import hash_code_hex
import sys
from server_config import NODES


class ConsistentHashNodeRing():
    
    def __init__(self, nodes, totalNumOfVirtualNodes = 32, replicationFactor = 2):
        assert len(nodes) > 0
        self.nodes = nodes
        self.m = 2**32
        self.totalNumOfVirtualNodes = totalNumOfVirtualNodes
        self.virtual_to_original_mapping = {}
        self.hash_ring = []
        self.add_virtual_nodes(totalNumOfVirtualNodes)
        self.replicationFactor = self.setReplicationFactor(replicationFactor)
        
    # makes sure replication factor is not higher than number of original nodes (NODES)
    def setReplicationFactor(self, num):
        if num > len(self.nodes):
            return len(self.nodes)
        return num
    
    # divides up total number of virtual nodes evenly amongst original nodes
    def add_virtual_nodes(self, total):
        numOfVirtualNodesEach = int(self.totalNumOfVirtualNodes / len(self.nodes))
        for node in self.nodes:
            currentKey = node
            # create virtual nodes and map them to corresponding original nod
            for i in range(numOfVirtualNodesEach):
                nodeHash = hash_code_hex(str(currentKey).encode())
                nodeIndex = int(nodeHash, 16) % self.m
                self.hash_ring.append(nodeIndex)
                self.virtual_to_original_mapping[str(nodeIndex)] = node
                currentKey = nodeHash
    
    # gets original node based on given virtual node from mapping
    def getOriginalFromVirtual(self, virtualNode):
        for key, val in self.virtual_to_original_mapping.items():
            if str(virtualNode) == key:
                return val
    
    # gets node(s) based on replication factor
    def get_nodes(self, key_hex):
        keyIndex = int(key_hex, 16) % self.m
        sortedHashRing = sorted(self.hash_ring)
        lengthOfSortedHashRing = len(sortedHashRing)

        nodes = []      # list of all nodes 
        for i in range(len(sortedHashRing)):
            # goes clockwise aroung hash_ring until finds the correct virtual node
            if keyIndex <= sortedHashRing[i]:
                # adds corresponding original node to list of nodes
                nodes.append(self.getOriginalFromVirtual(sortedHashRing[i]))
                i += 1
                # continues going around the hash_ring until its has same number of nodes as replication factor
                while len(nodes) < self.replicationFactor:
                    if i >= lengthOfSortedHashRing:
                        i = 0
                        
                    nextNode = self.getOriginalFromVirtual(sortedHashRing[i])
                    # don't add node if its already in list
                    if nextNode not in nodes:
                        nodes.append(nextNode)
                    i += 1
                break
        return nodes

    
def test():
    ring = ConsistentHashNodeRing(NODES, 400, 3)
    # print(len(ring.hash_ring))
    # print(ring.hash_ring)

    x = hash_code_hex('sdfsdf'.encode())
    print(ring.get_nodes(x))

# test()
