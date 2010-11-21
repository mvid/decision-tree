package org.apache.mahout.decision;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.df.DFUtils;
import org.apache.mahout.df.node.Node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Split implements Writable {

    private Node node;
    private Long splitId;
    private int attribute;

    public Split(Node node, Long id, int attr) {
        this.node = node;
        this.splitId = id;
        this.attribute = attr;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        node.write(out);
        out.writeLong(splitId);
        out.writeInt(attribute);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        node = Node.read(in);
        splitId = in.readLong();
        attribute = in.readInt();
    }
}
