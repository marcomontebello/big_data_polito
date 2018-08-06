package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class createPairSiCoordinate implements PairFunction<String, String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, String> call(String record) throws Exception {

	String[] fields=record.split("\\t");
	String key=fields[0];
	String lon=fields[1];
	String lat=fields[2];
	String value=lon+"-"+lat;
	
	return new Tuple2<String,String>(key,value);
	
	}

}
