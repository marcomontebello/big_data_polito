package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.List;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		
		inputPath=args[0];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab 6 Bonus Task");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file
		// Each element/string of the logRDD corresponds to one line of the input file  
		JavaRDD<String> logRDD = sc.textFile(inputPath);
		
		// Filter the rows that start by 'startWord'
		JavaRDD<String> resultRDD = logRDD.filter(new IsHeadersRow()) ;
		//TODO: filter the RD
		JavaPairRDD<String,String> UserProductPairRDD = resultRDD.mapToPair(new UserProduct());
		
		//group by key to groups all IDs of products which the user reviewed;
		JavaPairRDD<String,Iterable<String>> UserProductListPairRdd=UserProductPairRDD.groupByKey();
	
		//non mi interessano piu gli utenti voglio solo i prodotti messi insieme per trovarne le corrrelazioni
		
		JavaRDD<Iterable<String>> userProductList = UserProductListPairRdd.values();
		
		//creo coppie con key=coppia prodotti e value=1 =numero occorrenze
		JavaPairRDD<String,Integer> productPair=userProductList.flatMapToPair(new CreatePair());
		//sommo le occorrenze
		JavaPairRDD<String,Integer> productPairFrequencies=productPair.reduceByKey(new ProductPairSum());
		
		//filtro pair con frequenza maggiore di 1
		JavaPairRDD<String,Integer> productPairsFiltered=productPairFrequencies.filter(new FreqFilter());
		
		JavaPairRDD<Integer,String> swappedRDD=productPairsFiltered.mapToPair(new SwappingClass());
		
		List<Tuple2<Integer,String>> topOrderedRDD=swappedRDD.top(10, new FreqComparator());	
		
		for(Tuple2<Integer,String> pair:topOrderedRDD)
			
			System.out.println(pair._1()+" "+pair._2());			

		// Close the Spark context
		sc.close();
	}
}
