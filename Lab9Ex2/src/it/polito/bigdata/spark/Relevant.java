package it.polito.bigdata.spark;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class Relevant implements Function<Tuple2<String, Integer>, Boolean> {

	@Override
	public Boolean call(Tuple2<String, Integer> hashtagOccurr) throws Exception {
	if(hashtagOccurr._2().intValue()>=100)
		return true;
	
	else return false;
	}

}
