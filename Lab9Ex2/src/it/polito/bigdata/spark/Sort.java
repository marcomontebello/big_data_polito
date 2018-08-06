package it.polito.bigdata.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
//JavaPairRDD input ->JavaPairRDD output in ordine sono i Tipi di Function
public class Sort implements Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>> {


	@Override
	public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> rdd) throws Exception {
	
		return rdd.sortByKey();
	}

}
