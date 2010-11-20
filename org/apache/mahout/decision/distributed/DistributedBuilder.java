package org.apache.mahout.decision.distributed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.df.DecisionForest;
import org.apache.mahout.df.builder.TreeBuilder;
import org.apache.mahout.df.callback.PredictionCallback;
import org.apache.mahout.df.data.Data;
import org.apache.mahout.df.data.Instance;
import org.apache.mahout.df.data.conditions.Condition;
import org.apache.mahout.df.mapreduce.Builder;
import org.apache.mahout.df.node.CategoricalNode;
import org.apache.mahout.df.node.Leaf;
import org.apache.mahout.df.node.Node;
import org.apache.mahout.df.node.NumericalNode;
import org.apache.mahout.df.split.IgSplit;
import org.apache.mahout.df.split.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class DistributedBuilder implements TreeBuilder {

    private static final Logger log = LoggerFactory.getLogger(DistributedBuilder.class);

    private Integer threshold;

    private String outputDirName;

    /** indicates which CATEGORICAL attributes have already been selected in the parent nodes */
    private boolean[] selected;

    /** IgSplit implementation */
    private IgSplit igSplit;

    public DistributedBuilder(IgSplit split) {
        igSplit = split;
    }

    public void setThreshold(Integer threshold) {
        this.threshold = threshold;
    }

    public void setOutputDirName(String name) {
        outputDirName = name;
    }

    @Override
    public Node build(Random rng, Data data) {
        if (selected == null) {
            selected = new boolean[data.getDataset().nbAttributes()];
        }

        if (data.isEmpty()) {
            return new Leaf(-1);
        }
        if (isIdentical(data)) {
            return new Leaf(data.majorityLabel(rng));
        }
        if (data.identicalLabel()) {
            return new Leaf(data.get(0).getLabel());
        }

        int[] attributes = data.getDataset();

        // find the best split
        Split best = null;
        for (int attr : attributes) {
            Split split = igSplit.computeSplit(data, attr);
            if ((best == null) || (best.getIg() < split.getIg())) {
                best = split;
            }
        }

        boolean alreadySelected = selected[best.getAttr()];

        if (alreadySelected) {
            // attribute already selected
            log.warn("attribute {} already selected in a parent node", best.getAttr());
        }

        Node childNode;
        if (data.getDataset().isNumerical(best.getAttr())) {
            Data loSubset = data.subset(Condition.lesser(best.getAttr(), best.getSplit()));
            Node loChild = build(rng, loSubset);

            Data hiSubset = data.subset(Condition.greaterOrEquals(best.getAttr(), best.getSplit()));
            Node hiChild = build(rng, hiSubset);

            childNode = new NumericalNode(best.getAttr(), best.getSplit(), loChild, hiChild);
        } else { // CATEGORICAL attribute
            selected[best.getAttr()] = true;

            double[] values = data.values(best.getAttr());
            Node[] children = new Node[values.length];

            for (int index = 0; index < values.length; index++) {
                Data subset = data.subset(Condition.equals(best.getAttr(), values[index]));
                children[index] = build(rng, subset);
            }

            childNode = new CategoricalNode(best.getAttr(), values, children);

            if (!alreadySelected) {
                selected[best.getAttr()] = false;
            }
        }

        return childNode;
    }

    /**
     * checks if all the vectors have identical attribute values. Ignore selected attributes.
     *
     * @return true is all the vectors are identical or the data is empty<br>
     *         false otherwise
     */
    private boolean isIdentical(Data data) {
        if (data.isEmpty()) {
            return true;
        }

        Instance instance = data.get(0);
        for (int attr = 0; attr < selected.length; attr++) {
            if (selected[attr]) {
                continue;
            }

            for (int index = 1; index < data.size(); index++) {
                if (data.get(index).get(attr) != instance.get(attr)) {
                    return false;
                }
            }
        }

        return true;
    }

    /*
    * Return all attributes
     */
    protected static int[] getAttributes(Random rng, boolean[] selected) {
        int nbNonSelected = 0; // number of non selected attributes
        for (boolean sel : selected) {
            if (!sel) {
                nbNonSelected++;
            }
        }

        if (nbNonSelected == 0) {
            log.warn("All attributes are selected !");
        }

        int[] result;
        if (nbNonSelected <= m) {
            // return all non selected attributes
            result = new int[nbNonSelected];
            int index = 0;
            for (int attr = 0; attr < selected.length; attr++) {
                if (!selected[attr]) {
                    result[index++] = attr;
                }
            }
        } else {
            result = new int[m];
            for (int index = 0; index < m; index++) {
                // randomly choose a "non selected" attribute
                int rind;
                do {
                    rind = rng.nextInt(selected.length);
                } while (selected[rind]);

                result[index] = rind;
                selected[rind] = true; // temporarily set the chosen attribute to be selected
            }

            // the chosen attributes are not yet selected
            for (int attr : result) {
                selected[attr] = false;
            }
        }

        return result;
    }
}
