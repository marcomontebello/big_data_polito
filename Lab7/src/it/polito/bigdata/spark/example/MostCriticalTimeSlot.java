package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.function.Function2;

public class MostCriticalTimeSlot implements Function2<String, String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(String value1, String value2) throws Exception {
		
		String[] parts1=value1.split(" ");
		String[] parts2=value2.split(" ");
		Double criticality_1=Double.parseDouble(parts1[1]);
		Double criticality_2=Double.parseDouble(parts2[1]);
		
		String timeslot_1=parts1[0];
		String timeslot_2=parts2[0];


		if(criticality_1>criticality_2)
			return timeslot_1+" "+criticality_1.toString();
		if(criticality_2>criticality_1)
			return timeslot_2+" "+criticality_2.toString();
		
		else{
			
			String[] timeslot1_fields=timeslot_1.split("-");
			String[] timeslot2_fields=timeslot_2.split("-");

			int hour_1=Integer.parseInt(timeslot1_fields[1]);
			int hour_2=Integer.parseInt(timeslot2_fields[1]);
			
			if(hour_1<hour_2)
				return timeslot_1+" "+criticality_1.toString();
			if(hour_2<hour_1)
				return timeslot_2+" "+criticality_2.toString();

			else{
				
				String ts1=timeslot1_fields[0];
				String ts2=timeslot2_fields[0];
				if(ts1.compareTo(ts2)<0)
					return timeslot_1+" "+criticality_1.toString();
				else
					return timeslot_1+" "+criticality_1.toString();
		
			}			
			
		}	
		
	}

}
