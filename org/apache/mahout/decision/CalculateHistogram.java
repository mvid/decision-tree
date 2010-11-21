package org.apache.mahout.decision;

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

        private static final int blockSize = 100;
        private static int counter = 0;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (counter == 0) {
                StringTokenizer tokens = new StringTokenizer(value.toString(), ",");
                int counter = 0;
                while (tokens.hasMoreTokens()) {
                    String token = tokens.nextToken();
                    if (tokens.hasMoreTokens()) {
                        context.write(new LongWritable(counter), new Text(token));
                    }
                    counter++;
                    HistogramMap.counter++;
                }
            } else {
                counter++;
                if (counter == blockSize) {
                    counter = 0;
                }
                context.write(new LongWritable(-1), new Text(""));
            }
        }
    }

    public static class HistogramReduce extends Reducer<LongWritable, Text, LongWritable, HistogramBucket> {

        private final static int blockSize = 10;

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
                for(int j = 0; j < blockSize; j++)
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
