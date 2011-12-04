"""  Visualize.py
       Usage: python Visualize.py NetworkType inputgraph.x [outputplot.x.png 'PlotTitle']
"""

import sys
import time
import networkx as nx
import matplotlib.pyplot as plt
from Network.Node import Node, TabSeparatedNodeSerializer
from Network.InfectionStatus import InfectionStatus
import Utilities.NodePositions

###  define parameters
networkTypes = ['Network256', 'NetworkGraphable']
nodePlotSize = 15
plotSideLength = 12
###  end parameters

numArgs = len(sys.argv)
if numArgs < 3 or numArgs > 5:
    print "Usage: python Visualize.py NetworkType input-graph-filename [output-filename.png 'PlotTitle']"
    exit(1)

networkType = sys.argv[1]
if networkType not in networkTypes:
    print networkType + " not graphable (only Network256 and NetworkGraphable are currently valid). Check network type."
    exit(1)

plotAllNodes = True
if networkType != 'Network256':
    plotAllNodes = False

inputGraph = sys.argv[2]
if numArgs == 3:
    outputPlot = -1
    plotTitle = inputGraph
elif numArgs == 4:
    outputPlot = sys.argv[3]
    plotTitle = inputGraph
else:
    outputPlot = sys.argv[3]
    plotTitle = sys.argv[4]

SIZE = 256
G = nx.DiGraph()
f = open(inputGraph, 'r')

#initialize lists of nodes by type
nodetypes = {}
nodetypes['vulnerable'] = []
nodetypes['immune'] = []

#infected is a dict of lists for different hit_list lengths
nodetypes['infected'] = {}
maxHitLength = 0

#draw edges when node is infecting another
infectingEdges = []

#keep track of immune nodes if plotting all nodes
universe = []
if plotAllNodes:
    for i in xrange(SIZE):
        universe.append(i)
        G.add_node(i)

#want to sort nodes in file by address
allNodes = {}
successful = {}
infecting = {}
for line in f:
    #file parser
    Node.serializer = TabSeparatedNodeSerializer
    node = Node.serializer.deserialize(line)

    if node.status == InfectionStatus.SUCCESSFUL:
        successful[node.address] = {'status': node.status, 'hit_list':node.hit_list, 'source':node.source}
    elif node.status == InfectionStatus.INFECTING:
        infecting[node.address] = {'status': node.status, 'hit_list':node.hit_list, 'source':node.source}
    else:
        allNodes[node.address] = {'status': node.status, 'hit_list':node.hit_list, 'source':node.source}
f.close()

#add sorted nodes to appropriate lists
nodeCount = 0
addressToNodeCount = {}
for node in sorted(allNodes.iterkeys()):
    address = node
    status = allNodes[node]['status']
    hit_list = allNodes[node]['hit_list']
    source = allNodes[node]['source']
    addressToNodeCount[address] = nodeCount

    if not plotAllNodes:
        address = nodeCount
        universe.append(nodeCount)
        G.add_node(nodeCount)
    
    #lists for plotting
    if status == InfectionStatus.VULNERABLE:
        nodetypes['vulnerable'].append(address)

    elif status == InfectionStatus.INFECTED:
        l = len(hit_list)
        #add to list for given length
        if l not in nodetypes['infected']:
            nodetypes['infected'][l] = []
            nodetypes['infected'][l].append(address)
        else:
            nodetypes['infected'][l].append(address)

            
        if l > maxHitLength:
            maxHitLength = l

    #remove from rest of universe
    if plotAllNodes:
        try:
            universe.remove(address)
        except ValueError:
            print "Check Size of Universe\n"
            exit()
            
    nodeCount += 1

if plotAllNodes:
    for address in sorted(infecting.iterkeys()):
        status = infecting[address]['status']
        hit_list = infecting[address]['hit_list']
        source = infecting[address]['source']
        edge = (source, address)
        G.add_edge(edge[0],edge[1])
        infectingEdges.append(edge)
else:
    for address in sorted(successful.iterkeys()):
        status = successful[address]['status']
        hit_list = successful[address]['hit_list']
        source = successful[address]['source']
        edge = (addressToNodeCount[address], addressToNodeCount[source])
        G.add_edge(edge[0],edge[1])
        infectingEdges.append(edge)



#nx plotting
startTime = time.time()

if plotAllNodes:
    pos = Utilities.NodePositions.createPos(SIZE)
else:
    pos = Utilities.NodePositions.createPos(nodeCount)

fig = plt.figure(figsize=(plotSideLength,plotSideLength))
nx.draw_networkx_nodes(G, pos, nodelist=nodetypes['vulnerable'], node_color='yellow', node_size=nodePlotSize)

#plotting by cardinality of hit_list
for length in nodetypes['infected'].keys():
    #infected nodes without hit lists are black
    if length == 0:
        nx.draw_networkx_nodes(G, pos, nodelist=nodetypes['infected'][length], node_color=(0,0,0), node_size=nodePlotSize)

    #shade of red varies with length of hit list
    else:
        if maxHitLength == 0:
            shadeOfRed = 1
        else:
            shadeOfRed = (float(length)/maxHitLength)/2.0 + 0.5

        nx.draw_networkx_nodes(G, pos, nodelist=nodetypes['infected'][length], node_color=(shadeOfRed,0,0), node_size=nodePlotSize)

#draw all other nodes
if plotAllNodes:
    nx.draw_networkx_nodes(G, pos, nodelist=universe, node_color='green', node_size=nodePlotSize)

#draw edges
nx.draw_networkx_edges(G, pos, edgelist=infectingEdges, arrows=True, width=1.0, edge_color='red')
endTime = time.time()

print "Graphing time: " + str(endTime-startTime)

plt.title(plotTitle)
plt.axis('off')
if outputPlot != -1:
    plt.savefig(outputPlot, bbox_inches='tight', dpi=100)
else:
    plt.show()
