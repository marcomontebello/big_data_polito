package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class IsHeadersRow implements Function<String, Boolean> {

	@Override
	public Boolean call(String row) throws Exception {

		return !row.startsWith("Id");
	}

}
