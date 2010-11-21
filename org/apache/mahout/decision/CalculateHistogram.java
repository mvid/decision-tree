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

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 *
 * @author redbeard
 */
public class CalculateHistogram {

    public static class HistogramMap extends Mapper<LongWritable, Text, LongWritable, Text>{

        private static final int R = 100;
        private static int n = 0;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (n == 0) {
                StringTokenizer tokens = new StringTokenizer(value.toString(), ",");
                int counter = 0;
                while (tokens.hasMoreTokens()) {
                    String token = tokens.nextToken();
                    if (tokens.hasMoreTokens()) {
                        context.write(new LongWritable(counter), new Text(token));
                    }
                    counter++;
                    n++;
                }
            } else {
                n++;
                if (n == R) {
                    n = 0;
                }
                context.write(new LongWritable(-1), new Text(""));
            }
        }
    }

    public static class HistogramReduce extends Reducer<LongWritable, Text, LongWritable, HistogramBucket> {

        private final static int R = 10;

        public void reduce(LongWritable key,
                Iterator<Text> values,
                Context context)
                throws IOException, InterruptedException {
            if(key.toString().equals("-1"));
                context.write(key, new HistogramBucket(key));
            Text t = values.next();
            for(char c : t.toString().toCharArray())
                if(!Character.isDigit(c) && c != '.')
                    context.write(key, new HistogramBucket(key));//if this isnt a numerical attribute we ignore it

            HistogramBucket i = new HistogramBucket(key);
            i.add(new DoubleWritable(Double.parseDouble(t.toString())));
            while(values.hasNext()) {
                for(int j = 0; j < R; j++)
                    t = values.next();
                i.add(new DoubleWritable(Double.parseDouble(t.toString())));
            }
            context.write(key, i);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "MRDT - Generate Histogram");
        job.setJarByClass(CalculateHistogram.class);
        job.setMapperClass(CalculateHistogram.HistogramMap.class);
        job.setReducerClass(CalculateHistogram.HistogramReduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
