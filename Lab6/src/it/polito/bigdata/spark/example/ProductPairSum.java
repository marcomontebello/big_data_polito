package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class ProductPairSum implements Function2<Integer, Integer, Integer> {

	@Override
	public Integer call(Integer arg0, Integer arg1) throws Exception {

		return new Integer(arg0+arg1);
		
	}

}
