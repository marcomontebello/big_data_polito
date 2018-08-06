package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
   
	TopKVector<WordCountWritable> top100;
	
	protected void setup(Context context){
		
		top100=new TopKVector<WordCountWritable>(100);
		
	}
	
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	
    	for( WordCountWritable wordCount: values){
    		    	
    
    			top100.updateWithNewElement(new WordCountWritable(wordCount));
    			
    		
    	}

    }
    
    protected void cleanup(Context context) throws IOException,InterruptedException{
    	
    	for( WordCountWritable pair: top100.getLocalTopK())

    		context.write(new Text(pair.getWord()+" "+pair.getCount()), NullWritable.get());
    }
}
