package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function;

public class InitialFilter implements Function<String, Boolean> {


	private static final long serialVersionUID = 1L;

	@Override
	public Boolean call(String row) throws Exception {
		
			if(row.startsWith("station"))
				return false;
			
			String[] words=row.split("\\t");
			if (words[2]=="0" && words[3]=="0")
				return false;
			
			return true;
	
	}

}
