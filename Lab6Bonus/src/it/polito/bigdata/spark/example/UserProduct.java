package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class UserProduct implements PairFunction <String,String,String> {


	@Override
	public Tuple2<String, String> call(String row) throws Exception {
		
		String array[]=row.split(",");
		
		String userID=array[2];
		
		String productID=array[1];
	
		Tuple2<String,String> pair=new Tuple2<String,String>(userID,productID);
		return pair;
		
	}
}
