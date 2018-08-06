package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class SwappingClass implements PairFunction<Tuple2<String, Integer>, Integer, String> {

 static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Integer, String> call(Tuple2<String, Integer> row) throws Exception {

		Tuple2<Integer,String> result;
		result= new Tuple2<Integer,String>(row._2(),row._1());
		return result;
	}

}
