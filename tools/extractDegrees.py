
import sys, os
import math
from datetime import datetime, date, time
import time as _time

knowsFile = open(sys.argv[1],'r')

edgesPerPerson={}
SEPARATOR="|"
index=0
numEdges=0;
for line in knowsFile.readlines():
    if index > 0:
        edge = line.split(SEPARATOR)
        if int(edge[0]) in edgesPerPerson:
            edgesPerPerson[int(edge[0])]+=1
        else:
            edgesPerPerson[int(edge[0])]=1

        if int(edge[1]) in edgesPerPerson:
            edgesPerPerson[int(edge[1])]+=1
        else:
            edgesPerPerson[int(edge[1])]=1
        numEdges+=2
    index+=1   
knowsFile.close()

outputFile = open(sys.argv[2],'w')
histogram = {}
for person in edgesPerPerson:
    degree = edgesPerPerson[person]
    outputFile.write(str(degree)+"\n")
    #if degree in histogram:
    #    histogram[degree]+=1
    #else:
    #    histogram[degree]=1

outputFile.close()
    

#print("Average degree: "+str(numEdges/float(len(edgesPerPerson))))
#for degree in histogram:
#    outputFile.write(str(degree)+" "+str(histogram[degree])+" \n")
