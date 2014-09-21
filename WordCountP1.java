// Fuyong Xing
// Department of Electrical and Computer Engineering
// This code is for Task 1 of Programming Assignment 1 at the course: Cloud Computing and Storage.
// It counts the frequency of one-word given a text input. It is implemented using OpenJDK 6 and Hadoop 0.20.2. 
// It is from http://wiki.apache.org/hadoop/WordCount.

// Usage: $ bin/hadoop jar WordCountP1.jar WordCountP1 inputFolder outputFolder

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountP1 {
 
 /* Mapper implementation. It splits a line of words into tokens and then emits a key-value pair of <word, 1>. */	
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
 } 

 /* Reducer implementation. It sums the values associated with the same key (word). */
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }

 /* Main function. It configures and submits the job.*/
 public static void main(String[] args) throws Exception {
    
    /* Create a new job and set up job parameters. */
    Configuration conf = new Configuration();
    Job job = new Job(conf, "WordCountP1");
    job.setJarByClass(WordCountP1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    /* Set up input and output file paths. */
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /* Submit the job and poll for progress until it is done. */
    job.waitForCompletion(true);
 }

}
