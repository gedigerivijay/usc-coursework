/**
  * Created by vijay on 3/19/2017.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object Vijayakumar_Gedigeri_Task1 {

  def readTestData(fileName: String): (String, String) = {
    val rowData = fileName.split(",").map(_.trim)
    val row = (rowData{0},rowData{1})
    return  row
  }

  def readTrainData(fileName: String): (String, String, String) = {
    val rowData = fileName.split(",").map(_.trim)
    val row = (rowData{0},rowData{1},rowData{2})
    return  row
  }

  def main(args: Array[String]) = {

    if( args.length < 2)
    {
      println("Please pass the input: <training_file> <testing_file>")
      System.exit(0)
    }

    val sparkConf = new SparkConf().setAppName("movies").setMaster("local[*]")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)

    val test_data_csv = sc.textFile(args(1)).map(line => readTestData(line))
    val first = test_data_csv.first()
    val test_data = test_data_csv.filter(row =>  row != first).map(line => (line._1.toInt, line._2.toInt)).persist()

    val train_data_csv = sc.textFile(args(0)).map(line => readTrainData(line))
    val first2 = train_data_csv.first()
    val train_data = train_data_csv.filter(row =>  row != first2).map(line => ((line._1.toInt, line._2.toInt),line._3.toDouble))

    val user_movies = train_data.map(row => row._1)
    val train_movies = user_movies.subtract(test_data).map(r => (r,0))
    val training_movies = train_data.join(train_movies).map(r => (r._1,r._2._1))

    val test_movies = test_data.map(r => (r,0))
    val test_movie_rates = train_data.join(test_movies)
    val test_movie_ratings = train_data.join(test_movie_rates).map(r => (r._1,r._2._1))

    val ratings = training_movies.map( r =>  Rating(r._1._1,r._1._2,r._2))
    val rank = 5
    val iters = 16
    val model = ALS.train(ratings, rank, iters)
    val predictions = model.predict(test_data)
    val predictionsAll = predictions.map(r => ((r.user, r.product), r.rating))

    val outliers = predictionsAll.filter(r => r._2 < 0.0 || r._2 > 5.0)
    val actualPredictions = predictionsAll.subtract(outliers)

    val predictedMoviesRDD = predictionsAll.map(r => r._1)
    val missingMoviesRDD = test_data.subtract(predictedMoviesRDD)

    val missingAndOutliers = outliers.map(r => r._1).union(missingMoviesRDD)

    val userRatings = predictionsAll.map( r =>  (r._1._1,r._2))
    val meanRatings = userRatings.aggregateByKey((0.0,0.0)) ((U, b) => (U._1 + b, U._2 + 1),
                      (U,V) =>  (U._1 + V._1, U._2 + V._2)).mapValues(res => 1.0 * res._1 / res._2)

    val userMovieRatings = missingAndOutliers.join(meanRatings).map(r => ((r._1,r._2._1),r._2._2))
    val predictionsUnion = actualPredictions.union(userMovieRatings)
    val ratesAndPreds = test_movie_ratings.join(predictionsUnion).sortByKey()

    val absDiff = ratesAndPreds.map( r => Math.abs(r._2._1 - r._2._2))
    val zero = absDiff.filter(r => r >= 0 && r <1.0).count()
    val one = absDiff.filter(r => r >= 1.0 && r < 2.0).count()
    val two = absDiff.filter(r => r >= 2.0 && r < 3.0).count()
    val three = absDiff.filter(r => r >= 3.0 && r < 4.0).count()
    val four = absDiff.filter(r => r >= 4.0).count()
    println()
    println(">=0 and <1: "+zero)
    println(">=1 and <2: "+one)
    println(">=2 and <3: "+two)
    println(">=3 and <4: "+three)
    println(">=4: "+four)

    val squared_error = ratesAndPreds.map(r => Math.pow((r._2._1 - r._2._2),2)).mean()
    println("RMSE = "+Math.sqrt(squared_error))

    val headers = Array(("UserId","MovieId","Pred_rating"))
    val header = sc.parallelize(headers)
    val resultRDD = ratesAndPreds.map( r => (r._1._1.toString,r._1._2.toString,r._2._2.toString)).repartition(1)
    val fileRDD = header.union(resultRDD).map(r => r._1+","+r._2+","+r._3)
    fileRDD.repartition(1).saveAsTextFile("progOutput")
  }
}
