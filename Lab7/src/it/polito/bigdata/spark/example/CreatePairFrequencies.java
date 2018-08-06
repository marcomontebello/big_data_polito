package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class CreatePairFrequencies implements PairFunction<String, String, PairCounts> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<String, PairCounts> call(String reading) throws Exception {

		String[] fields=reading.split("\\t");
		
		String sI=fields[0];
		
		String timestamp=fields[1];
		String[] dateFields=timestamp.split(" ");
		String dow=DateTool.DayOfTheWeek(dateFields[0]);
		String[] time=dateFields[1].split(":");
		String hour=time[0];
		String timeslot=dow+" - "+hour; 
		
		if(Integer.parseInt(fields[3])==0)
			
			return new Tuple2<String,PairCounts>(sI+" "+timeslot,new PairCounts(1,1));
		
		else
			return new Tuple2<String,PairCounts>(sI+" "+timeslot,new PairCounts(1,0));

	}

}
