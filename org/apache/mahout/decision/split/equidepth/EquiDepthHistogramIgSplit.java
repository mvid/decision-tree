package org.apache.mahout.decision.split.equidepth;

import org.apache.mahout.df.data.Data;
import org.apache.mahout.df.data.Dataset;
import org.apache.mahout.df.split.IgSplit;
import org.apache.mahout.df.split.Split;

public class EquiDepthHistogramIgSplit extends IgSplit{
    
    public EquiDepthHistogramIgSplit(Dataset dataset) {
        // TODO: use dataset to create histogram
    }

    @Override
    public Split computeSplit(Data data, int attr) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
