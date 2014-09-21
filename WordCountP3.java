// This code is for Task 3 of Programming Assignment 1 at the course: Cloud Computing and Storage.
// It uses DistributedCache to count the frequency of one-word in another given list.
// It is implemented using OpenJDK 6 and Hadoop 0.20.2. 
// It is based on the code from http://hadoop.apache.org/docs/r0.18.3/mapred_tutorial.html.

// Usage: $ bin/hadoop jar WordCountP3.jar WordCountP3 inputFolder outputFolder patternFile

import java.io.*;
import java.util.*;
	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 	
public class WordCountP3 {

   /* Mapper implementation. It gets the pattern files from the cache, parses the files into single words and saves the words in a String set. It then splits a line of words in the input file into tokens (words), compares them with those in the patterns, and then emits a key-value pair of <word, 1> if this word is found in the pattern file. */ 	
   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
     private final static IntWritable one = new IntWritable(1);
     private Text word = new Text(); 	
     private Set<String> patternsToFind = new HashSet<String>(); 	
     private String inputFile;

     /* Here it gets the pattern files from the cache, parses the files into single words.*/
     public void configure(JobConf job) {
       inputFile = job.get("map.input.file");
         Path[] patternsFiles = new Path[0];
         try {
           patternsFiles = DistributedCache.getLocalCacheFiles(job); // Here it gets the path array of the localized caches.
         } catch (IOException ioe) {
           System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
         }
         for (Path patternsFile : patternsFiles) {
           parseFindFile(patternsFile);
         }
     }
 	
     private void parseFindFile(Path patternsFile) {
       try {
         BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
         String pattern = null;
	 StringTokenizer strtkn;
         while ((pattern = fis.readLine()) != null) {
	   strtkn = new StringTokenizer(pattern);
	   while (strtkn.hasMoreTokens()) {
           	patternsToFind.add(strtkn.nextToken());
	   }
         }
       } catch (IOException ioe) {
         System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
       }
     }

     /* Map function. It compares the words with those in the pattern file, and emits a key-value <word, 1> if they are matching. */
     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       String line = value.toString(); 

       StringTokenizer tokenizer = new StringTokenizer(line);
       String str = "";
       while (tokenizer.hasMoreTokens()) {
	 str = tokenizer.nextToken();
	 if (patternsToFind.contains(str)) {
         	word.set(str);
         	output.collect(word, one);
	 }
      }
     }
   }

   /* Reducer implementation. It sums the values associated with the same key. */	
   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       int sum = 0;
       while (values.hasNext()) {
         sum += values.next().get();
       }
       output.collect(key, new IntWritable(sum));
     }
   }

   public static void main(String[] args) throws Exception {

     /* Create a new job and set up job parameters. */
     JobConf conf = new JobConf(WordCountP3.class);
     conf.setJarByClass(WordCountP3.class);
     conf.setJobName("WordCountP3");
     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(IntWritable.class);	
     conf.setMapperClass(Map.class);
     conf.setCombinerClass(Reduce.class);
     conf.setReducerClass(Reduce.class);	
     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);
     
     /* Add the pattern file to be localized to the conf, and set up input and output file paths. */
     DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

     /* Submit the job. */
     JobClient.runJob(conf);
   }
}
