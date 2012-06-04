package com.danielnee.analytics.cardinality.mapreduce;

import com.danielnee.analytics.cardinality.HyperLogLog;
import com.danielnee.analytics.cardinality.RegisterSet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * A serialisable version of the HyperLogLog class compatible with Hadoop
 */
public class HyperLogLogWritable extends HyperLogLog implements Writable
{
    /**
     * Provide a no argument constructor to help with the serialisation/deserialisation.
     * It is made private as it should not be used outside of this class.
     */
    private HyperLogLogWritable()
    {
        super(4);
    }
    
    public HyperLogLogWritable(int k)
    {
        super(k);
    }
    
    public HyperLogLogWritable(double rse)
    {
        super(rse);
    }

    public void write(DataOutput output) throws IOException
    {
        output.writeInt(m);
        output.writeInt(k);
        output.writeDouble(alphaMM);
        output.writeInt(M.getCount());
        int noBuckets = M.getNumberBuckets();
        int buckets[] = M.getBuckets();
        output.writeInt(noBuckets);
        for (int i = 0; i < noBuckets; ++i)
        {
            output.writeInt(buckets[i]);
        }
    }

    public void readFields(DataInput input) throws IOException
    {
        m = input.readInt();
        k = input.readInt();
        alphaMM = input.readDouble();
        int count = input.readInt();
        int noBuckets = input.readInt();
        int buckets[] = new int[noBuckets];
        for (int i = 0; i < noBuckets; ++i)
        {
            buckets[i] = input.readInt();
        }
        M = new RegisterSet(count, buckets);
    }
    
    public static HyperLogLogWritable read(DataInput input) throws IOException 
    {
         HyperLogLogWritable l = new HyperLogLogWritable();
         l.readFields(input);
         return l;
    }
}
