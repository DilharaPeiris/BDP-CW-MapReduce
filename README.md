  MSc. Big Data Programming Course Work

	MapReduce
			1. docker run -p 8088:8088 -p 40060:40060 -v C:\Users\Dilhara\Downloads\bda-CW-2019258:/resources --name hadoop-hive-pig-cw -d suhothayan/hadoop-hive-pig:2.7.1
			2. docker exec -it hadoop-hive-pig-cw bash
			3. cd resources 
			4. hdfs dfs -put listings.csv listings.csv
 
 	Q1. Total number of rentals that are available 365 days a year.
 		yarn jar demo-0.0.1-SNAPSHOT.jar com.example.map.Availability listings.csv output/wordCount1n
	
	Q2. Number of rentals per neighbourhood_group
		yarn jar demo-0.0.1-SNAPSHOT.jar com.example.map.Neighbourhood listings.csv output/wordCount1n
