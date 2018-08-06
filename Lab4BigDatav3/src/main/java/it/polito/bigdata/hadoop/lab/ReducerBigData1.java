package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer #1
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
					Text,           // Input key type
					Text,    // Input value type
					Text,           // Output key type
					Text> {  // Output value type

@Override
	protected void reduce(
					Text key, // Input key type
					Iterable<Text> values, // Input value type
					Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
		//viene creato un buffer ogni qual volta viene chiamato il metodo reduce
		//in modo che mantenga tutti i prodotti assiciati alla key=userID e i voti associati
		HashMap<String,Integer> buffer=new HashMap<String,Integer>();
		int count=0;
		int sum=0;

		
		for(Text value:values){
				if(key.charAt(0)!='a') continue;
				
				String valueString=value.toString();
				String[] valueFields=valueString.split("_");
				String IdProduct=valueFields[0];
				Integer rate=Integer.parseInt(valueFields[1]);
				buffer.put(IdProduct, rate);
				sum=sum+rate;
				count=count+1;

		}

		float average=(float)sum/count;
		System.out.println(average);
		
		
		for(Map.Entry<String, Integer> entry : buffer.entrySet()){
			float difference=Float.parseFloat(String.format("%.2f", (entry.getValue()-average)));
			context.write(key,new Text(entry.getKey()+"_"+difference));

		}


	}
}

