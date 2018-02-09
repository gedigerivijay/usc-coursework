from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.context import SQLContext
import pandas
import sys

sc = SparkContext(appName='inf553')
def readFile(fileName):
    rowData = fileName.split(',')   
    return (rowData[1], rowData[2])

def convertData(rowData):
    return (int(rowData[0]), float(rowData[1]))

def convertTags(rowData):
    return (int(rowData[0]), rowData[1].encode('ascii','replace'))

def transformData(joinedData): 
    return (joinedData[1][0],joinedData[1][1])

def convertList(row):
    return [row[0], row[1]]
    
def processData(fileName):
    global sc
    csvdata   = sc.textFile(fileName+'/'+'ratings.csv').map(lambda fileLine : readFile(fileLine))
    header = csvdata.first()
    ratingsRDD = csvdata.filter(lambda row: row != header)
    ratingsRDD = ratingsRDD.map(lambda row: convertData(row))#.groupByKey()
    
    csvtags   = sc.textFile(fileName+'/'+'tags.csv').map(lambda fileLine : readFile(fileLine))
    header1 = csvtags.first()
    tagsRDD = csvtags.filter(lambda row: row != header1)
    tagsRDD = tagsRDD.map(lambda row: convertTags(row))

    joinRDD = tagsRDD.join(ratingsRDD)#.sortByKey()

    joinRDDTransformed = joinRDD.map(lambda joinedData: transformData(joinedData))
    
    reducedRDD = joinRDDTransformed.aggregateByKey((0,0), lambda U,b: (U[0] + b, U[1] + 1),
                                      lambda U,V: (U[0] + V[0], U[1] + V[1]))

    resultRDD = reducedRDD.map(lambda (x,(y,z)): (x,float(y)/z)).sortByKey(False)
    
    sqlc = SQLContext(sc)
    #finalResultRDD = resultRDD.map(lambda row: convertList(row))#Row(tag = row[0], rating_avg=row[1]))
    dataFrame = sqlc.createDataFrame(resultRDD)#,['tag', 'rating_avg'])#
    pandas_df = dataFrame.toPandas()
    
    if fileName == 'ml-2om':
        written_file = 'Vijayakumar_Gedigeri_result_task2_big.csv'
    else:
        written_file = 'Vijayakumar_Gedigeri_result_task2_small.csv'
    #print pandas_df
    pandas_df.to_csv(written_file, encoding = 'utf-8', header = ['tag', 'rating_avg'], index = False)
    #dataFrame.write.format("com.databricks.spark.csv").option("header", "true").saveFile("result_tags.csv")

if len(sys.argv) < 2:
    print "Please pass the input directory as an argument"
    sys.exit()# Exit when no argument is passed

fileName = sys.argv[1]
processData(fileName)