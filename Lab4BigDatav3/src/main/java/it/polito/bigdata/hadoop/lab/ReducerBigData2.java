package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer #2
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	float sum=0;
    	int count=0;
    	
    	for(FloatWritable value:values){
    		
    		sum=sum+value.get();
    		count=count+1;
    		
    	}

    	context.write(new Text(key), new FloatWritable(Float.parseFloat(String.format("%.2f",(float)sum/count))));
    	
    }
}
