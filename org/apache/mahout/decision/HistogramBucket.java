package org.apache.mahout.decision;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class HistogramBucket implements Writable {

    private ArrayList<DoubleWritable> splits;
    private LongWritable attribute;

    public HistogramBucket(LongWritable attr) {
        splits = new ArrayList<DoubleWritable>();
        attribute = attr;
    }

    public DoubleWritable[] getArray() {
        return (DoubleWritable[])splits.toArray();
    }
    
    public LongWritable getAttr() {
        return attribute;
    }

    public void add(DoubleWritable element) {
        splits.add(element);
    }

    public int size() {
        return splits.size();
    }

    @Override
    public void write(DataOutput d) throws IOException {
        attribute.write(d);
        LongWritable arraySize = new LongWritable(splits.size());
        arraySize.write(d);
        for(DoubleWritable w : splits) {
            w.write(d);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        attribute.readFields(di);
        LongWritable arraySize = new LongWritable();
        arraySize.readFields(di);
        splits = new ArrayList<DoubleWritable>();
        for(int i = 0; i < Integer.parseInt(arraySize.toString()); i++) {
            DoubleWritable d = new DoubleWritable();
            d.readFields(di);
            splits.add(d);
        }
    }

}