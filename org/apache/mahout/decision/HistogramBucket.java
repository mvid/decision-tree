/*
 *  Copyright 2010 redbeard.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */


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

    public boolean contains(double d) {
        for(DoubleWritable dw : splits) {
            if(Double.parseDouble(dw.toString()) == d)
                return true;
        }
        return false;
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