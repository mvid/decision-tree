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

package LetsMakeATree;

import java.io.IOException;
import org.apache.mahout.df.builder.DefaultTreeBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.df.node.Node;
import org.apache.mahout.df.data.*;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

/**
 *
 * @author Corey
 */
public class Test1 extends Configured implements Tool {
    public static void main(String[] args)throws IOException{
        Random random = new Random();
        DefaultTreeBuilder dtb = new DefaultTreeBuilder();
        //Path path = new Path("/Users/Antonio/Documents/small_yellow.dat"); // first try
        Path dataPath = new Path("/Users/Antonio/Documents/balloons.data");
        Path datasetPath = new Path("/Users/Antonio/Documents/balloons.names");
        Node tree;
        Configuration conf = new Configuration();
        //FileSystem fSystem = FileSystem.get(path.toUri(), conf); // first try

        Test1 blah = new Test1();

    
        // load the data
        FileSystem fs = dataPath.getFileSystem(new Configuration());
        Dataset dataset = Dataset.load(blah.getConf(), datasetPath);
        Data data = DataLoader.loadData(dataset, fs, dataPath);


        //Data data = DataLoader.loadData(dataSet, fSystem, path); // first try

        tree = dtb.build(random, data);
    }
    public int run(String[] args){
        return 0;
    }

}
