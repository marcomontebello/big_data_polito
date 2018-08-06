package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class createString implements Function<Tuple2<String, Tuple2<String, String>>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(Tuple2<String, Tuple2<String, String>> row) throws Exception {

		String dayCrit=row._2()._1();
		String coordinates=row._2()._2();
		
		String[] dCvalues=dayCrit.split(" ");
		String criticality=dCvalues[1];
		
		String[] dayWeek_hour=dCvalues[0].split("-");
		String dayWeek=dayWeek_hour[0];
		String hour=dayWeek_hour[1];

		String[] cooValues=coordinates.split("-");
	
		String coord=cooValues[0]+","+cooValues[1];
		
		String name="<name>"+row._1()+"</name>";
		String kmlDayWeek="<Data name=\"DayWeek\"><value>"+dayWeek+"</value></Data>";
		String kmlHour="<Data name=\"Hour\"><value>"+hour+"</value></Data>";
		String kmlCriticality="<Data name=\"Criticality\"><value>"+criticality+"</value></Data>";

		String extendedData="<ExtendedData>"+kmlDayWeek+kmlHour+kmlCriticality+"</ExtendedData>";
		String point="<Point><coordinates>"+coord+"</coordinates></Point>";
		
		return new String("<Placemark>"+name+extendedData+point+"</Placemark>");
		
	}

}
