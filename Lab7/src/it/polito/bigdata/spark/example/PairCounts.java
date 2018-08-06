package it.polito.bigdata.spark.example;

public class PairCounts {
	
	public int numReadings;
	public int numCriticalReadings;
	
	public PairCounts(int num, int numCritical){
		
		this.numReadings=num;
		this.numCriticalReadings=numCritical;
		
	}

	public String toString(){
		
		return new String("total: "+this.numReadings+" critical: "+this.numCriticalReadings);
		
	}
}
