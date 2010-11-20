package org.apache.mahout.decision;

import org.apache.mahout.df.data.Instance;
import org.apache.mahout.df.node.Node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UnknownNode extends Node {

    private boolean inMem;

    private int attr;

    public UnknownNode() {
        attr = -1;
    }
    
    @Override
    public int classify(Instance instance) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long nbNodes() {
        return 1;  // we are the only node in our tree
    }

    @Override
    public long maxDepth() {
        return 1; // because we are a leaf
    }

    @Override
    protected Type getType() {
        return Type.LEAF; // this node will be replaced when it is discovered
    }

    @Override
    protected String getString() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void writeNode(DataOutput out) throws IOException {
        out.writeInt(-1);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        attr = in.readInt();
    }
}
