package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function;

public class FilterKeys implements Function<String, Boolean> {

	@Override
	public Boolean call(String line) throws Exception {

		return line.startsWith(this.startWordToFilter);
	}
	
	public FilterKeys(String startWord){
		
		this.startWordToFilter=startWord;
			
	}
	
	private String startWordToFilter;

}
