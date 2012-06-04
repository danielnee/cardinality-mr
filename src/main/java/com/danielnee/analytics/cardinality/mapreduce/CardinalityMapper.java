package com.danielnee.analytics.cardinality.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for cardinality estimation.
 *  
 * Uses in-mapper combining, see:
 * 
 * J. Lin and C. Dyer. Data-Intensive Text Processing with MapReduce
 * 
 */
public class CardinalityMapper extends Mapper<LongWritable, Text, Text, HyperLogLogWritable>
{
    private HyperLogLogWritable counter;
    
    private static final double REQUIRED_STANDARD_ERROR = 0.01;
    
    public CardinalityMapper()
    {
        super();
        counter = new HyperLogLogWritable(REQUIRED_STANDARD_ERROR);
    }
    
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        counter.offer(value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        context.write(new Text("value"), counter);
    }
}