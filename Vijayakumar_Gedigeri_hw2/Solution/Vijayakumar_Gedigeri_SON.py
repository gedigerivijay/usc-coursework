from pyspark import SparkContext
import sys
import itertools
from operator import add
import math

sc = SparkContext(appName='inf553')

def readFile(fileName):
    rowData = str(fileName).split(',')
    return (rowData[0], rowData[1])
    
def readUserMovies(rowData):
    return (int(rowData[0]), int(rowData[1]))

def readMovieUsers(rowData):
    return (int(rowData[1]), int(rowData[0]))

def implementSON(part):
    baskets_set = [set(itemset[1]) for itemset in part]
    nps = int(math.floor(supportValue * (len(baskets_set) / float(itemcount))))
    if start_num == 1:    
        single_List = []
        baskets_comb = set.union(*baskets_set)
        [single_List.append(single) for single in baskets_comb if len([1 for x in baskets_set if single in x]) >= nps]
        return single_List
    elif start_num == 2:
        baskets_list = [set.intersection(x,set(frequent_set)) for x in baskets_set if len(x) >=2]
        baskets_list = [x for x in baskets_list if len(x) >= 2]
        frequent_items = []
        if len(baskets_list) >= nps: 
            dictionary_return = {}
            pairs_comb = list(itertools.combinations(frequent_set,2))
            for double in pairs_comb:
                break_it = False
                for items in baskets_list:
                    if set(double).issubset(items):
                        if double not in dictionary_return.keys():
                            dictionary_return[double] = 1
                        else:
                            dictionary_return[double] += 1
                            if dictionary_return[double] >= nps:
                                break_it = True
                    if break_it:
                        break
            frequent_items = [k for k, v in dictionary_return.iteritems() if v >=nps]
        return frequent_items
    else:
        baskets = [set.intersection(x, set(frequent_set)) for x in baskets_set if len(x) >= start_num ]
        baskets = [x for x in baskets if len(x) >= start_num]
        frequent_items = []
        if len(baskets) >= nps:
            candid_frequents = list(itertools.combinations(frequent_set,start_num))
            dictionary_return = {}
            for candid_frequent in candid_frequents:
                candids = set(itertools.combinations(candid_frequent,start_num-1))
                break_it = False
                if candids.issubset(frequent_items_list):
                    for items in baskets:
                        if set(candid_frequent).issubset(items):
                            if candid_frequent not in dictionary_return.keys():
                                dictionary_return[candid_frequent] = 1
                            else:
                                dictionary_return[candid_frequent] += 1
                                if dictionary_return[candid_frequent] >= nps:
                                    break_it = True
                        if break_it:
                            break
            frequent_items = [k for k, v in dictionary_return.iteritems() if v >=nps]
        return frequent_items
    
def secondMap(partition_data):
    baskets = [set(itemset[1]) for itemset in partition_data]
    frequent_items_dict = {}
    if start_num ==1:
        [frequent_items_dict.update({key:len([1 for x in baskets if key in x])}) for key in frequents]
    else:
        [frequent_items_dict.update({key:len([1 for x in baskets if set(key).issubset(x)])}) for key in frequents]
    frequent_items = [(k,v) for k, v in frequent_items_dict.iteritems()]
    return frequent_items
    
def processFile(caseNum, name, support):
    global sc, frequents, itemcount, start_num, frequent_set, frequent_items_list, supportValue
    csvdata = sc.textFile(name)
    csvdata = csvdata.map(lambda fileLine : readFile(fileLine))
    numPartitions = csvdata.getNumPartitions()
    header = csvdata.first()
    allDataRDD = csvdata.filter(lambda row: row != header)
    supportValue = support
    if caseNum == 1:
        userMoviesRDD = allDataRDD.map(lambda fileLine : readUserMovies(fileLine))
        bucketsRDD = userMoviesRDD.groupByKey()
    elif caseNum == 2:
        movieUsersRDD = allDataRDD.map(lambda fileLine : readMovieUsers(fileLine))
        bucketsRDD = movieUsersRDD.groupByKey()
    itemcount = bucketsRDD.count()
    written_file = 'output_case'+str(caseNum)+str('_s')+str(support)+'.txt'
    f = open(written_file,'w')
    start_num = 1
    runAlgo = True
    break_it = False
    total_count = 0
    while runAlgo:
        frequents = list(set(bucketsRDD.mapPartitions(implementSON).collect()))
        if not frequents:
            break
        resultRDDNew = bucketsRDD.mapPartitions(secondMap).reduceByKey(add).filter(lambda row: row[1] >= support)
        results = resultRDDNew.collect()
        if not results:
            break
        else:
            total_count += len(results)
            printList = [x[0] for x in results]
            if start_num == 1:
                printList.sort()
                printLists = ['('+str(x)+'), ' for x in printList[:-1]]
                file_string = "".join(printLists)+'('+str(printList[-1])+')'
                f.write(file_string)
                if len(printList) >= start_num +1:
                    f.write('\r\r')
                    frequent_set = list(printList)
                    start_num += 1
                else:
                    break_it = True
            else:            
                printList.sort(key=lambda tup: tup)
                printLists = [str(x)+', ' for x in printList[:-1]]
                file_string = "".join(printLists)+str(printList[-1])
                f.write(file_string)
                if len(printList) >= start_num +1:
                    f.write('\r\r')
                    frequent_items_list = list(printList)
                    frequent_set = list(set.union(*[set(a) for a in printList]))
                    frequent_set.sort()
                    start_num += 1
                else:
                    break_it = True
        if break_it:
            break
    print        
    print "Written to file => "+written_file 
    
if len(sys.argv) < 4:
    print "Please pass the input: <Case> <File> <Support>"
    sys.exit()# Exit when no argument is passed

processFile(int(sys.argv[1]),sys.argv[2], int(sys.argv[3]))