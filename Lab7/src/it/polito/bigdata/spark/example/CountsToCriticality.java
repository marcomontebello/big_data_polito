package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function;

public class CountsToCriticality implements Function<PairCounts, Double> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3048391434377117962L;

	@Override
	public Double call(PairCounts pair) throws Exception {

		return new Double(pair.numCriticalReadings/(double)pair.numReadings);
	}

}
