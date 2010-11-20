package org.apache.mahout.decision.distributed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.df.node.Node;

import java.io.IOException;

public class NodeExpander {

    public class Map extends Mapper {

        private Node tree;

        protected void setup(Mapper.Context context) throws IOException {
            Path treePath = new Path(context.getConfiguration().get("treePath"));
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            FSDataInputStream in = fileSystem.open(treePath);
            
            tree = Node.read(in);
            
            in.close();
            fileSystem.close();
        }

        public void map(Text record, Text value, Context context) {
            
        }
    }

    public class Reduce extends Reducer {

        public void reduce(Node node, Iterable splits, Context context) {
            
        }
    }
}
