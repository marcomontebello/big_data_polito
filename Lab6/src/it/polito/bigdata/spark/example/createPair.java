package it.polito.bigdata.spark.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class createPair implements PairFlatMapFunction<Iterable<String>, String, Integer> {

	@Override
	public Iterable<Tuple2<String, Integer>> call(Iterable<String> record) throws Exception {
		
		List<Tuple2<String,Integer>> result=new ArrayList<Tuple2<String,Integer>>();
		
		for(String s1:record){
			for(String s2:record){
				
				if(s1.compareToIgnoreCase(s2)>0)
					result.add(new Tuple2<String,Integer>(s1+" "+s2,1));
				
			}
				
		}
		
		
		return result;
		
		
	}

}
