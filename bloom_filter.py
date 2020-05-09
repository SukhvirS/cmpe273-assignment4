import math
from bitarray import bitarray
import mmh3


class BloomFilter():
    def __init__(self, numKeys, falsePosProb):
        self.falsePosProb = falsePosProb
        self.numKeys = numKeys
        self.size = self.calculateM(numKeys, falsePosProb)
        self.hashCount = self.getCount(self.size, self.numKeys)

        self.arr = bitarray(self.size)
        self.arr.setall(0)

    # calculates the bit array size (m)
    def calculateM(self, numKeys, falsePosProb):
        m = - (numKeys * math.log(falsePosProb)) / (math.log(2)**2)
        return int(m)
    
    # calculate the hash count (k)
    def getCount(self, size, numKeys):
        k = (size / numKeys) * math.log(2)
        return int(k)

    # adds new item to bloom filter
    def add(self, newItem):
        for i in range(self.hashCount):
            index = mmh3.hash(newItem, i) % self.size
            self.arr[index] = True
        
    # checks if item is in bloom filter
    def is_member(self, item):
        for i in range(self.hashCount):
            index = mmh3.hash(item, i) % self.size
            if(self.arr[index] == False):
                return False
        return True
