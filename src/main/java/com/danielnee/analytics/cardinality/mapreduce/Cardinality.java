package com.danielnee.analytics.cardinality.mapreduce;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hadoop map-reduce job for estimating the cardinality of a set of items.
 * 
 * Provides a fully-distributed method for estimating the cardinality to within a certain
 * degree of accuracy. Increased accuracy can be achieved by using more memory. One limitation
 * is the final cardinality estimation object must be able to fit in the memory of a single reducer.
 * 
 * The input is expected to be a single string identifier per line.
 * 
 * The output will be the estimated number of unique strings in the input.
 */
public class Cardinality
{
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        if (args.length != 2)
        {
            System.err.println("Usage: Cardinality <input-path> <output-path>");
            System.exit(-1);
        }
        
        Job job = new Job();
        job.setJarByClass(Cardinality.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(CardinalityMapper.class);
        job.setReducerClass(CardinalityReducer.class);
        // No combiner class as key/values of mapper and reducer are not the same, hence a combiner
        // class cannot be used. Combiner is largely unnecessary as we are using in-mapper combining
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HyperLogLogWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
          
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
