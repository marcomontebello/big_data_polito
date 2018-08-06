package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


class ReducerBigData1
  extends Reducer<Text, IntWritable, Text, Text>
{
  ReducerBigData1() {}
  
  protected void reduce(
		  Text key, 
		  Iterable<IntWritable> values,
		  Context context) throws IOException, InterruptedException
  {
    Integer occurrances = Integer.valueOf(0);
    
    for (IntWritable value : values)
    {
      occurrances = Integer.valueOf(occurrances.intValue() + value.get());
    }
    
    context.write(key, new Text(occurrances.toString()));
  }
}
