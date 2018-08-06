package it.polito.bigdata.spark;

import org.apache.spark.api.java.function.Function2;

public class Sum implements Function2<Integer, Integer, Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Integer call(Integer val1, Integer val2) throws Exception {

		return new Integer(val1+val2);
	}

}
