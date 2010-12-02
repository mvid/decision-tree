/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package LetsMakeATree;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.mahout.df.builder.DefaultTreeBuilder;
import org.apache.mahout.df.node.Node;
import org.apache.mahout.df.data.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class InMemoryTreeMR {

    public static void main(String[] args) throws Exception {

        Path dataPath;
        //Path datasetPath;

        String dataName = "/Users/Antonio/Documents/BalloonData/Iris.data"; // the instances file
        String datasetName = "/Users/Antonio/Documents/BalloonData/Iris.names"; // the names file

        dataPath = new Path(dataName);
        //datasetPath = new Path(datasetName);
        Scanner scan = new Scanner(new File(datasetName));


        FileSystem fs = dataPath.getFileSystem(new Configuration());
        Dataset dataset = Dataset.loadWithScanner(scan);
        //Dataset dataset = Dataset.load(getConf(), datasetPath);
        Data data = DataLoader.loadData(dataset, fs, dataPath);

        //Build the tree
        DefaultTreeBuilder dtb = new DefaultTreeBuilder();
        Random random = new Random();
        Node tree = dtb.build(random, data);
        Node tree1 = dtb.build(random, data);
        Node tree2 = dtb.build(random, data);


        DataConverter converter = new DataConverter(dataset);

        Instance inst1 = converter.convert(0, "6.7,3.0,5.0,1.9,Iris-versicolor");

        System.out.println(dataset.getLabel(tree.classify(inst1)));

        /*

        Instance inst1 = converter.convert(0,"PURPLE,SMALL,DIP,ADULT,T");

        System.out.println(dataset.getLabel(tree.classify(inst)));

         */
    }

    public class InMemoryTreeMap extends Mapper<LongWritable, Text, LongWritable, Text> {

        Dataset dataset;
        Node tree;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            setup();

            DataConverter converter = new DataConverter(dataset);

            String record = value.toString();
            Instance instance = converter.convert(0, record);

            Node found = tree.findUnknownNode(instance);

            if (found != null) {
                context.write(key, value);
            }
        }

        private void setup() throws IOException {
            setupDataset();
            setupTree();

        }

        private void setupDataset() throws IOException {
            String datasetName = "/Users/Antonio/Documents/BalloonData/Iris.names"; // the names file
            Scanner scan = new Scanner(new File(datasetName));
            dataset = Dataset.loadWithScanner(scan);

        }

        private void setupTree() throws IOException {
            String dataName = "/Users/Antonio/Documents/BalloonData/Iris.data"; // the names file
            Path dataPath = new Path(dataName);
            FileSystem fs = dataPath.getFileSystem(new Configuration());
            Data data = DataLoader.loadData(dataset, fs, dataPath);
            DefaultTreeBuilder dtb = new DefaultTreeBuilder();
            Random random = new Random();
            tree = dtb.build(random, data);
        }
    }
}
