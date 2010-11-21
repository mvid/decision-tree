package org.apache.mahout.decision.distributed;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.decision.Split;
import org.apache.mahout.df.node.Node;
import org.apache.mahout.math.SparseMatrix;

import java.io.IOException;

public class NodeExpander {

    public class Map extends Mapper <LongWritable, Text, Split, DoubleWritable> {

        private Node tree;
        private SparseMatrix matrix;

        protected void setup(Context context) throws IOException {
            Path treePath = new Path(context.getConfiguration().get("treePath"));
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            FSDataInputStream in = fileSystem.open(treePath);
            
            tree = Node.read(in);
            
            in.close();
            fileSystem.close();
        }

        public void map(LongWritable id, Text record, Context context) throws IOException, InterruptedException {
            context.setStatus("Calculating split for record: " + id.toString());
            context.write(new Split(tree, 0L, 0), new DoubleWritable(0.0));
            
        }
    }

    public class Reduce extends Reducer <Split, Iterable<DoubleWritable>, Split, DoubleWritable> {

        protected void setup(Context context) {
            
        }

        public void reduce(Split split, Iterable<DoubleWritable> yValues, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (DoubleWritable y : yValues) {
                sum += y.get();
                count++;
            }

            double variance = 0;
            double average = sum / count;
            
            for (DoubleWritable y : yValues) {
                variance += Math.pow(y.get() - average, 2);
            }

            variance = variance / count;

            context.write(split, new DoubleWritable(variance));
        }
    }
}
