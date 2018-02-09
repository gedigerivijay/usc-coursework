from pyspark import SparkContext
import sys

sc = SparkContext(appName='inf553')

def readRatings(fileName):
    rowData = str(fileName).split(',')   
    return (rowData[1], rowData[2])

def convertData(rowData):
    return (int(rowData[0]), float(rowData[1]))

def processFile(name):
    global sc
    #filename = str(name) + '/' + 
    csvdata   = sc.textFile(name+'/'+'ratings.csv').map(lambda fileLine : readRatings(fileLine))
    #csvdata   = sc.textFile('./ml-latest-small/ratings.csv').map(lambda fileLine : readRatings(fileLine))
    header = csvdata.first()
    ratingsRDD = csvdata.filter(lambda row: row != header)
    ratingsRDD = ratingsRDD.map(lambda row: convertData(row))#.groupByKey()
    reducedRDD = ratingsRDD.aggregateByKey((0,0), lambda U,b: (U[0] + b, U[1] + 1),
                                      lambda U,V: (U[0] + V[0], U[1] + V[1]))

    resultRDD = reducedRDD.map(lambda (x,(y,z)): (x,float(y)/z)).sortByKey()
    
    if name == 'ml-20m':
        written_file = 'Vijayakumar_Gedigeri_result_task1_big.txt'
    else:
        written_file = 'Vijayakumar_Gedigeri_result_task1_small.txt'
        
    f = open(written_file,'w')
    for itr in resultRDD.collect():
        line = str(itr[0]) + ',' + str(itr[1]) + '\n'
        f.write(line)
    f.close()
    
if len(sys.argv) < 2:
    print "Please pass the input directory as an argument"
    sys.exit()# Exit when no argument is passed

fileName = sys.argv[1]
processFile(fileName)
