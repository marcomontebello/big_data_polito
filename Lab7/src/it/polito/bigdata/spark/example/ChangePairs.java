package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ChangePairs implements PairFunction<Tuple2<String, Double>, String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, String> call(Tuple2<String, Double> reading) throws Exception {
		String oldKey=reading._1();
		String[] oldKeyFields=oldKey.split(" ");
		String si=oldKeyFields[0];
		String timeslot=oldKeyFields[1]+oldKeyFields[2]+oldKeyFields[3];
		
		return new Tuple2<String,String>(si,timeslot+" "+reading._2().toString());
	}

}
