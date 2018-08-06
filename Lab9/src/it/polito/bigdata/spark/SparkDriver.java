package it.polito.bigdata.spark;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	
	public static void main(String[] args) {

		
		String outputPathPrefix;
		String inputFolder;

		inputFolder = args[0];
		outputPathPrefix = args[1];
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Streaming Lab 9");
				
		// Create a Spark Streaming Context object
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));		
		
		// Set the checkpoint folder (it is needed by some window transformations)
		jssc.checkpoint("checkpointfolder");   
		
		// TODO
		// Read the "streaming" data from inputFolder and process it.
		// Everytime a new file is uploaded  in inputFolder a new set of streaming data 
		// is generated 
		// ...
		//Read streming data from inputFolder.
		JavaDStream<String> tweets=jssc.textFileStream(inputFolder);
		//emit pairs <#hashtag,1>
		JavaPairDStream<String,Integer> hashtagOnesRdd=tweets.flatMapToPair(new TweetOne());
		JavaPairDStream<String,Integer> hashtagSum=hashtagOnesRdd.reduceByKeyAndWindow(new Sum(), Durations.seconds(30), Durations.seconds(10));
		JavaPairDStream<Integer,String> sumHashtagRdd=hashtagSum.mapToPair(new Swap());
		JavaPairDStream<Integer,String> sortedHashtag=sumHashtagRdd.transformToPair(new Sort());
		
		sortedHashtag.print();
		sortedHashtag.dstream().saveAsTextFiles(outputPathPrefix, "");
		// Start the computation of the streaming data
		jssc.start();              
		
		// Run the application for at most 120000 ms
		jssc.awaitTerminationOrTimeout(120000);
		
		jssc.close();
		
	}
}
