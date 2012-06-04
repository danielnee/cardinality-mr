package com.danielnee.analytics.cardinality.mapreduce;

import com.danielnee.analytics.cardinality.CardinalityMergeException;
import com.danielnee.analytics.cardinality.HyperLogLog;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for cardinality estimation. 
 * 
 * Outputs the final cardinality estimate
 */
public class CardinalityReducer extends Reducer<Text, HyperLogLogWritable, Text, LongWritable>
{
    @Override
    protected void reduce(Text key, Iterable<HyperLogLogWritable> values, Context context) throws IOException, 
        InterruptedException
    {
        ArrayList<HyperLogLog> list = new ArrayList();
        for (HyperLogLogWritable counter : values)
        {
            list.add(counter);
        }
        HyperLogLogWritable combined = null;
        try
        {
            // Down cast the result to writable so we can write it out in the context
            combined = (HyperLogLogWritable) HyperLogLog.mergeEstimators(list);
        }
        catch (CardinalityMergeException ex)
        {
            Logger.getLogger(CardinalityReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        context.write(key, new LongWritable(combined.cardinality()));
    }
}
