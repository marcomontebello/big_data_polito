package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
	
	HashMap<String,Integer> wordsCounts;

	protected void setup(Context context){
		
		wordsCounts=new HashMap<String,Integer>();
		
		
	}

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    		Integer currentFreq;
    		String[] words = value.toString().split(",");
    		
    		for(int i=1;i<words.length; i++){
    			String cleanedWord1=words[i].toLowerCase();
    					
    			for(int j=i+1;j<words.length;j++){
        			
    				
    				String cleanedWord2=words[j].toLowerCase();		
    				
    				String pair;
    				
    				if(cleanedWord1.compareTo(cleanedWord2)<0)
    					pair=cleanedWord1+","+cleanedWord2;
    				
    				else pair=cleanedWord2+","+cleanedWord1;
    				//ordiniamo coppia per renderla univoca
    				currentFreq=wordsCounts.get(pair);
    				if(currentFreq==null)
    					wordsCounts.put(new String(pair), new Integer(1));
    				
    				else {
    					
    					currentFreq =currentFreq+1;
    					wordsCounts.put(new String(pair), new Integer(currentFreq));
    					
    				}
		
    			}  			
    			
    		}
    	
    }
    
    protected void cleanup(Context context) throws IOException,InterruptedException{
		
		for(Entry<String,Integer> entryPair:wordsCounts.entrySet()){
			
			context.write(new Text(entryPair.getKey()), new IntWritable(entryPair.getValue()));
			
		}
		
		
		
		
	}		
}
