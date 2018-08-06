package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
	//	String startWord;
		
		inputPath=args[0];
		outputPath=args[1];
	//	startWord=args[2];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark ");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the input file  
		JavaRDD<String> logRDD = sc.textFile(inputPath);
		// Filter the rows that start by 'startWord'
		JavaRDD<String> resultRDD = logRDD.filter(new isHeadersRow()) ;//TODO: filter the RD
		JavaPairRDD<String,String> UserProductPairRDD = resultRDD.mapToPair(new UserProduct());
		
		//group by key to groups all IDs of products which the user reviewed;
		JavaPairRDD<String,Iterable<String>> UserProductListPairRdd=UserProductPairRDD.groupByKey();
	
		//non mi interessano piu gli utenti voglio solo i prodotti messi insieme per trovarne le corrrelazioni
		
		JavaRDD<Iterable<String>> userProductList = UserProductListPairRdd.values();
		
		//creo coppie con key=coppia prodotti e value=1 =numero occorrenze
		JavaPairRDD<String,Integer> productPair=userProductList.flatMapToPair(new createPair());
		//sommo le occorrenze
		JavaPairRDD<String,Integer> productPairFrequencies=productPair.reduceByKey(new ProductPairSum());
		
		//filtro pair con frequenza maggiore di 1
		JavaPairRDD<String,Integer> productPairsFiltered=productPairFrequencies.filter(new FreqFilter());
		
		JavaPairRDD<Integer,String> swappedRDD=productPairsFiltered.mapToPair(new SwappingClass());
		
		JavaPairRDD<Integer,String> sortedRDD=swappedRDD.sortByKey();
		
		// Store the result in the output folder
		sortedRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}
