package it.polito.bigdata.spark;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class TweetOne implements PairFlatMapFunction<String, String, Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Iterable<Tuple2<String, Integer>> call(String line) throws Exception {

		ArrayList<Tuple2<String,Integer>> hashtagOne=new ArrayList<Tuple2<String,Integer>>();
		
		//field[0]=userid
		//field[1]=tweet
		String [] tweetFields=line.split("\\t");
		
		String tweetText=tweetFields[1];
		String[] tweetWords=tweetText.split("\\s");
		
		for(String word:tweetWords)
		{		
			if(word.startsWith("#"))
				hashtagOne.add(new Tuple2<String,Integer>(word,new Integer(1)));		
		}
				
		return hashtagOne;
	}

}
