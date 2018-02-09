from pyspark import SparkContext
import sys
import math
import random
from itertools import combinations
import os

sc = SparkContext(appName='inf553')

num_bands = 28
row_size = 6 
bin_size = 6
hash_size = num_bands * row_size

def readFile(fileName):
    rowData = str(fileName).split(',')
    return (rowData[0], rowData[1])

def readMovies(fileString):
    rowData = fileString.split(',')
    return (int(rowData[0]),int(rowData[1]))
    
def readUserMovieData(fileName):
    global movieUsers
    user_movies_csv = sc.textFile(fileName).map(lambda f: readFile(f))
    first = user_movies_csv.first()
    movieUsers = user_movies_csv.filter(lambda row: row != first).map(lambda r: (int(r[0]), int(r[1]))).persist()

def generateUserMovieMatrix():
    global num_rows, userMovieMatrix, moviesList, moviesDict, num_movies
    movieUsersGrouped = movieUsers.groupByKey().sortByKey().collect()
    moviesList = movieUsers.map(lambda r: r[1]).distinct().collect()
    num_movies = len(moviesList)
    moviesList.sort()
    moviesDict = {x:i for i, x in enumerate(moviesList)}
    #usersList = movieUsers.map(lambda r: r[0]).distinct().collect().sort()
    usersList = []
    userMovieMatrix = []
    
    for userMovies in movieUsersGrouped:
        movies = [0] * num_movies
        usersList.append(userMovies[0])
        for movie in list(userMovies[1]):
            movies[moviesDict[movie]] = 1
        userMovieMatrix.append(movies)
        
    num_rows = len(userMovieMatrix)
    #print list(movieUsersGrouped[0][1])
 
def generateHash():
    global hashTable
    # 3x + 8 % 5
    hashTable = []
    hashFunctions = []
    primes = [5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61]
    for i in range(hash_size):
        hashValues = []
        a = random.randint(1,70)
        b = random.randint(1,100)
        p = random.choice(primes)
        hashFunctions.append([a,b,p])
        for x in range(num_rows):
            exp = ((a * x + b) % p) % bin_size
            hashValues.append(exp)
        hashTable.append(hashValues)
    
def generateSignaturesMatix():
    global signatureMatrix
    signatureMatrix = []
    for userMovies in userMovieMatrix[:1]:
        ratedMovies = [i for i, x in enumerate(userMovies) if x == 1]
        hashValues = [hash[0] for hash in hashTable]
        for val in hashValues:
            movies = [None] * num_movies
            for i in ratedMovies:
                movies[i] = val
            signatureMatrix.append(movies)
    c = 1
    for userMovies in userMovieMatrix[1:]:
        ratedMovies = [i for i, x in enumerate(userMovies) if x == 1]
        hashValues = [hash[c] for hash in hashTable]
        k = 0
        for val in hashValues:
            for i in ratedMovies:
                if signatureMatrix[k][i] == None or val < signatureMatrix[k][i]:
                    signatureMatrix[k][i] = val
            k +=1
        c += 1

def generateBandsAndVectors(start,end):
    vectors = []
    for j in range(num_movies):
        vectors.append([])
    i = start
    while i < end:
        k = 0
        for j in signatureMatrix[i]:
            vectors[k].append(j)
            k +=1
        i +=1
    return vectors    

def compareVectors(moviesRDD,start,end):
    vectors = generateBandsAndVectors(start,end)
    interimRDD = moviesRDD.filter(lambda pair: vectors[moviesDict[pair[0]]] == vectors[moviesDict[pair[1]]])
    return interimRDD
    
def generateCandidPairs():  
    start = 0
    end = row_size
    movie_pairs = list(combinations(moviesList,2))
    movies_RDD = sc.parallelize(movie_pairs)
    resultsRDDList = []
    while end <= len(signatureMatrix):
        interimRDD = compareVectors(movies_RDD,start,end)
        resultsRDDList.append(interimRDD)
        start = end
        end = start + row_size
    return resultsRDDList

def collectCandidatePairs():
    global candidate_pairs_RDD
    resultsRDDList = generateCandidPairs()
    resultRDD = resultsRDDList[0]
    for rdd in resultsRDDList[1:]:
        resultRDD = resultRDD.union(rdd)#.distinct()
    candidate_pairs_RDD = resultRDD.distinct()
    #print "Candidate pairs", candidate_pairs_RDD.count()
    
def findJaccardSimilarity(part):
    movie_pairs = list(part)
    final_pairs = []
    for pair in movie_pairs:
        movie1Users = userMovieMatrix[moviesDict[pair[0]]]
        movie2Users = userMovieMatrix[moviesDict[pair[1]]]
        num_set = len(set.intersection(movie1Users,movie2Users))
        denom_set = len(set.union(movie1Users,movie2Users))
        jaccard = (1.0 * num_set) / denom_set
        if jaccard >= 0.5:
            final_pairs.append((pair,jaccard))
    #final_pairs.sort()
    return final_pairs
    
def findSimilarMovies(output_path):
    global userMovieMatrix, similarMovies
    userMovieMatrix = []
    userMoviesGrouped = movieUsers.map(lambda r: (r[1],r[0])).groupByKey().sortByKey().collect()
    for users in userMoviesGrouped:
        userMovieMatrix.append(set(users[1]))
    similarMovies = candidate_pairs_RDD.mapPartitions(findJaccardSimilarity).collect()
    similarMovies.sort()
    dir_name,file_name = os.path.split(output_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    f = open(output_path,'w')
    printList = [str(r[0][0])+', '+str(r[0][1])+', '+str(r[1]) for r in similarMovies]
    for fileString in printList:
        f.write(fileString+'\r')
    print "Writing similar movies to file complete"
    
if len(sys.argv) < 3:
    print "Please pass the input: <input_file> <output_path>" 
    sys.exit()# Exit when no argument is passed
    
readUserMovieData(sys.argv[1])   
generateUserMovieMatrix()
generateHash()
generateSignaturesMatix()
collectCandidatePairs()
findSimilarMovies(sys.argv[2])