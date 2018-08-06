package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.List;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputNameFileKML;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = new Double(args[2]);
		outputNameFileKML = args[3];		
		
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);
		JavaRDD<String> inputStationRDD = sc.textFile(inputPath2);


		JavaRDD<String> inputFiltered= inputRDD.filter(new InitialFilter());

		JavaPairRDD<String,PairCounts> siTsFrequencies =inputFiltered.mapToPair(new CreatePairFrequencies());
		
		JavaPairRDD<String,PairCounts> siTjFreqTotRDD= siTsFrequencies.reduceByKey(new SumFrequencies());
		
		JavaPairRDD<String,Double> siTjCriticality=siTjFreqTotRDD.mapValues(new CountsToCriticality());
		
		JavaPairRDD<String,Double> siTjAboveTreshold=siTjCriticality.filter(new FilterAboveCriticality(threshold));
		
		JavaPairRDD<String,String> siIntermediateRDD=siTjAboveTreshold.mapToPair(new ChangePairs());
		
		JavaPairRDD<String, String> siMaxCriticalRDD=siIntermediateRDD.reduceByKey(new MostCriticalTimeSlot());
		
		JavaPairRDD<String,String> stationCoordRDD=inputStationRDD.mapToPair(new createPairSiCoordinate());
		
		JavaPairRDD<String,Tuple2<String,String>> joinPairRDD = siMaxCriticalRDD.join(stationCoordRDD);
		
		//joinPairRDD.saveAsTextFile(outputPath);
		
		// Store in resultKML one String, representing a KML marker, for each station 
		// with a critical timeslot
		
		JavaRDD<String> resultKML = joinPairRDD.map(new createString());
		
		// There is at most one string for each station. We can use collect and
		// store the returned list in the main memory of the driver.
		List<String> localKML = resultKML.collect();
		
		// Store the result in one single file stored in the distributed file
		// system
		// Add header and footer, and the content of localKML in the middle
		Configuration confHadoop = new Configuration();

		try {
			URI uri = URI.create(outputNameFileKML);

			FileSystem file = FileSystem.get(uri, confHadoop);
			FSDataOutputStream outputFile = file.create(new Path(uri));

			BufferedWriter bOutFile = new BufferedWriter(new OutputStreamWriter(outputFile, "UTF-8"));

			// Header
			bOutFile.write("<kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
			bOutFile.newLine();

			// Markers
			for (String lineKML : localKML) {
				bOutFile.write(lineKML);
				bOutFile.newLine();
			}

			// Footer
			bOutFile.write("</Document></kml>");
			bOutFile.newLine();

			bOutFile.close();
			outputFile.close();      
			
			

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// Close the Spark context
		
		sc.close();
	}
}
