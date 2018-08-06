package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function2;

public class SumFrequencies implements Function2<PairCounts, PairCounts, PairCounts> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public PairCounts call(PairCounts counts1, PairCounts counts2) throws Exception {

		
		return new PairCounts(counts1.numReadings+counts2.numReadings,counts1.numCriticalReadings+counts2.numCriticalReadings);
	}

}
