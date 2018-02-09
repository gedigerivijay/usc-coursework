Program is written in Python for Spark
Prerequistes are libraries pyspark, pandas are available

Important Note: The folders/directories containing ratings.csv and tags.csv need to be passed as program arguments

1. There are two Python files available Vijayakumar_Gedigeri_task1.py and Vijayakumar_Gedigeri_task2.py

2. To run these programs, please follow below guideline

  a. Task1:
     bin/spark-submit Vijayakumar_Gedigeri_task1.py <directory_name>
     
     <directory_name> here means either 'ml-20m' containing big data sets or 'ml-latest-small' containing small data sets
     
     e.g.
     bin/spark-submit Vijayakumar_Gedigeri_task1.py ml-20m
     
     This produces a single output text file 'Vijayakumar_Gedigeri_result_task1_small.txt' or 'Vijayakumar_Gedigeri_result_task1_big.txt' depending on directory passed
     
   
  b. Task2:
     bin/spark-submit Vijayakumar_Gedigeri_task2.py <directory_name>
     
     <directory_name> here means either 'ml-20m' containing big data sets or 'ml-latest-small' containing small data sets
     
     e.g.
     bin/spark-submit Vijayakumar_Gedigeri_task2.py ml-latest-small
     
     This produces a single output csv file 'Vijayakumar_Gedigeri_result_task2_small.csv' or 'Vijayakumar_Gedigeri_result_task2_big.csv' depending on directory passed