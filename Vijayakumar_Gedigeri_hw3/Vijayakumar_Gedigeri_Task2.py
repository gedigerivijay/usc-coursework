from pyspark import SparkContext
import sys
import math
from collections import Counter

sc = SparkContext(appName='inf553')

userMoviesDict = {}
movie_user_ratings = {}
pearsonWeights = {} 

def readFile(fileName, test):
    rowData = str(fileName).split(',')
    if test:
        return [rowData[0], rowData[1]]
    return [rowData[0], rowData[1],rowData[2]]

def readTestData(fileName):
    global test_data, testuserMovies, test_movie_list
    test_data_csv = sc.textFile(fileName).map(lambda f: readFile(f, True))
    first = test_data_csv.first()
    test_data = test_data_csv.filter(lambda row: row != first).map(lambda l: (int(l[0]), int(l[1])))
    test_data.persist()  
    testuserMovies = test_data.map(lambda l: (l[1],l[0])).groupByKey().collect()
    test_movie_list = list(set(test_data.map(lambda row: row[1]).collect()))

def readTrainData(fileName):
    global train_data
    train_data_csv = sc.textFile(fileName).map(lambda f: readFile(f,False))
    first = train_data_csv.first()
    train_data = train_data_csv.filter(lambda row: row != first).map(lambda r: ((int(r[0]), int(r[1])), float(r[2])))    
    
def separateTestData():
    global training_movies, training_movie_ratings
    test_user_movies = set(test_data.collect())
    training_movies = train_data.filter(lambda row: row[0] not in test_user_movies)
    training_movie_ratings = train_data.filter(lambda row: row[0] in test_user_movies)

def groupTrainData():
    global userMoviesDict, movie_user_ratings
    userMovieGrouped = training_movies.map(lambda r: r[0]).groupByKey().collect()
    [userMoviesDict.update({r[0]:list(r[1])}) for r in userMovieGrouped]
    
    movie_user_list = training_movies.map(lambda r: (r[0][1],(r[0][0],r[1]))).groupByKey().collect()
  
    for movie in movie_user_list:
        user_dict = {}
        [user_dict.update({x[0]:x[1]}) for x in list(movie[1])]
        movie_user_ratings[movie[0]] = user_dict
        
def findPearsonCorr(movieOneRatings, movieTwoRatings):
    pass
    length = float(len(movieOneRatings))
    avg_RatingOne = sum(movieOneRatings) / length
    avg_RatingTwo = sum(movieTwoRatings) / length 
    ratingsOne = [float(x)-avg_RatingOne for x in movieOneRatings]
    ratingsTwo = [float(y)-avg_RatingTwo for y in movieTwoRatings]
    numExp = sum([x*y for x,y in zip(ratingsOne,ratingsTwo)])
    denomExp = math.sqrt(sum([x**2 for x in ratingsOne]) * sum([y**2 for y in ratingsTwo]))
    #if numExp ==0:
    #    return 0
    if denomExp == 0:
        return 0
    return numExp / denomExp

def calculateRatings(movieRatings, movieWeights):
    pass
    numerator = sum([rating * weight for rating,weight in zip(movieRatings,movieWeights)])
    denomExp = sum(movieWeights)
    if denomExp == 0:
        return 0
    movieRating = numerator / denomExp
    return movieRating

def findSimilarMovies():
    global pearsonWeights
    print "Finding co-rated movies"    
    count = 0
    rated_movies = set(movie_user_ratings.keys())
    for movieItem in test_movie_list:
        if movieItem in rated_movies:
            if count % 600 == 0:
                print ".",
            pearsonWeights[movieItem] = {}
            movieOneUsers = set(movie_user_ratings[movieItem].keys())
            for movieTwo in movie_user_ratings.keys():
                if movieItem != movieTwo:
                    movieTwoUsers = movie_user_ratings[movieTwo].keys()
                    commonUsers = set.intersection(movieOneUsers,set(movieTwoUsers))
                    if not commonUsers:
                        continue
                    movieOneRatings = []
                    movieTwoRatings = []
                    [movieOneRatings.append(movie_user_ratings[movieItem][user]) for user in commonUsers]
                    [movieTwoRatings.append(movie_user_ratings[movieTwo][user]) for user in commonUsers]
                    weight = findPearsonCorr(movieOneRatings, movieTwoRatings)
                    pearsonWeights[movieItem][movieTwo] = weight
        count +=1

def predictRatings():
    global test_movie_ratings, test_missing, rating_missing
    print
    ratingsDict = {}
    missing_movies = []
    missing_ratings = []
    count = 0
    print "Predicting "

    rated_movies = set(movie_user_ratings.keys())
    for user_movie_pair in testuserMovies:
        if count % 700 == 0:
            print '.',
        movie_id = user_movie_pair[0]
        movie_users = list(user_movie_pair[1])
        if movie_id in pearsonWeights.keys():
            pears_weights = pearsonWeights[movie_id]
            for user_id in movie_users:
                user_rated_movies = userMoviesDict[user_id]
                corated_movies = set.intersection(set(user_rated_movies),set(pears_weights.keys()))
                corated_movies = list(set.intersection(corated_movies,rated_movies))
                corated_movie_weights = {k:pears_weights[k] for k in corated_movies}
                sortedWeights = [(pears_weights[k],k) for k in corated_movies]
                sortedWeights.sort(key = lambda tup: tup[0],reverse = True)
                max_corrated = int(len(corated_movie_weights) * 0.45)#len(corated_movie_weights) / 2    
                similar_movies = {k[1]:k[0] for k in sortedWeights[:max_corrated]}
                train_movie_names  = similar_movies.keys()
                movie_ratings = [movie_user_ratings[movie_name][user_id] for movie_name in train_movie_names]
                movie_weights = similar_movies.values()
                if all([val == 0 for val in movie_weights]):
                    missing_movies.append((user_id,movie_id))
                    continue
                predicted_rating = calculateRatings(movie_ratings, movie_weights)
                ratingsDict[(user_id,movie_id)] = predicted_rating
        
        else:
            [missing_movies.append((user,movie_id)) for user in movie_users]
        count+=1  
    
    test_ratings = [(k,v) for k,v in ratingsDict.iteritems()]
    test_movie_ratings = sc.parallelize(test_ratings)
    test_missing = sc.parallelize(missing_movies)

def predictMissingRatings():
    global predictedRatings
    userRatings = test_movie_ratings.map(lambda r: (r[0][0],r[1]))
    meanRatings = userRatings.aggregateByKey((0,0), lambda U,b: (U[0] + b, U[1] + 1),
                                          lambda U,V: (U[0] + V[0], U[1] + V[1])).mapValues(lambda res: 1.0 * res[0] / res[1])
    userMovieRatings = test_missing.join(meanRatings).map(lambda r: ((r[0],r[1][0]),r[1][1]))
    predictionsUnion1 = test_movie_ratings.union(userMovieRatings)

    predictedRatings = training_movie_ratings.join(predictionsUnion1).sortByKey()

def collectAndWrite():
    results = predictedRatings.map(lambda r: (r[0][0],r[0][1],r[1][1])).collect()
    cc = 0
    f = open("Vijayakumar_Gedigeri_result_task2.txt","w")
    f.write("UserId,MovieId,Pred_rating\r")
    printList = [str(r[0])+","+str(r[1])+","+str(r[2])+"\r" for r in results]
    [f.write(line) for line in printList]

def calculateDifference():
    absDiff = predictedRatings.map(lambda r: abs(r[1][0] - r[1][1]))
    diffRatings = absDiff.collect()
    zero_one = len([x for x in diffRatings if x >= 0 and x <1.0])
    print ">=0 and <1:", zero_one
    one_two = len([x for x in diffRatings if x >= 1.0 and x <2.0])
    print ">=1 and <2:", one_two
    two_three = len([x for x in diffRatings if x >= 2.0 and x <3.0])
    print ">=2 and <3:", two_three
    three_four = len([x for x in diffRatings if x >= 3.0 and x <4.0])
    print ">=3 and <4:", three_four
    four = len([x for x in diffRatings if x >= 4.0])
    print ">=4:",four
    diffRatings = [x**2 for x in diffRatings]
    print "RMSE =",math.sqrt(sum(diffRatings) / float(len(diffRatings)))    
    
if len(sys.argv) < 3:
    print "Please pass the input: <training_file> <testing_file>"
    sys.exit()# Exit when no argument is passed

readTestData(sys.argv[2])
readTrainData(sys.argv[1])   
separateTestData()
groupTrainData()
findSimilarMovies()
predictRatings()
predictMissingRatings()
collectAndWrite()
calculateDifference()