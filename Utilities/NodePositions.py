import math

#n should be square
#map addresses to integer coordinates within square
def createPos(n):
    pos = {}

    for i in xrange(n):
        y = i / int(math.sqrt(n))
        x = i - y * int(math.sqrt(n))
        pos[i] = (x,y)
    

    return pos

