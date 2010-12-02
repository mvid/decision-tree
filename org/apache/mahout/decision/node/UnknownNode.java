/*
 *  Copyright 2010 Antonio.
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

package org.apache.mahout.df.node;


import org.apache.mahout.df.data.Instance;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UnknownNode extends Node {

    private boolean inMem;

    private int attr;

    public UnknownNode(boolean inMem) {
        attr = -1;
        this.inMem = inMem;
    }

    @Override
    public int classify(Instance instance) {
        return -1;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Node findUnknownNode(Instance instance) {
        return this;  //To change body of implemented methods use File | Settings | File Templates.
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
        out.writeInt(attr);
        out.writeBoolean(inMem);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        attr = in.readInt();
        inMem = in.readBoolean();
    }
}
