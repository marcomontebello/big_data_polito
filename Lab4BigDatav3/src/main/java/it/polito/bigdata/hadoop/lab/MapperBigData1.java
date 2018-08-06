package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper #1
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    	
    		String record=value.toString();
    		String[] fields=record.split(",");
    		
    		String IdUser=fields[2].toLowerCase(); //key
    		String IdProd_rate=fields[1]+"_"+fields[6];	//value
    		
    		context.write(new Text(IdUser), new Text(IdProd_rate));
		

    
    
    }
}
