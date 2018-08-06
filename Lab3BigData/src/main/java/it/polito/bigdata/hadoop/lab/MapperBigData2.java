package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type
    
	
	TopKVector<WordCountWritable> top100local;
	
	protected void setup(Context context){
		
		top100local=new TopKVector<WordCountWritable>(100);
	}
	
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    		top100local.updateWithNewElement(new WordCountWritable(key.toString(),new Integer(Integer.parseInt(value.toString()))));
    		
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException{
    	
    	for(WordCountWritable wordCounter:top100local.getLocalTopK())
    		
    		context.write(NullWritable.get(), new WordCountWritable(wordCounter.getWord(),wordCounter.getCount()));
    	
    	
    }
    
    
}
