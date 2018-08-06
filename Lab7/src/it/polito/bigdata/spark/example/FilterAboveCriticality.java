package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class FilterAboveCriticality implements Function<Tuple2<String, Double>, Boolean> {


	private static final long serialVersionUID = 1L;

	public Double threshold;
	
	public FilterAboveCriticality(Double threshold) {

		this.threshold=threshold;
		
	}

	@Override
	public Boolean call(Tuple2<String, Double> record) throws Exception {

		if(record._2()>threshold)
			return true;
		
		return false;
			
	
	
	}

}
