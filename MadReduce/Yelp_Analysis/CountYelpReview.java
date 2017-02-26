/*
 * Modified existing code to display just top 10 business with highest rating
 */
package YelpWordCount.YelpWordCount;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class CountYelpReview{
	

	public static class ReviewMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from reviews
			String delims = "//^";
			String[] reviewData = StringUtils.split(value.toString(),delims);
			if (reviewData.length == 4) {
					double rating = Double.parseDouble(reviewData[3]);
					context.write(new Text(reviewData[2]), new DoubleWritable(rating));
			}		
		}
	
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
		
		
		}
	}

	public static class ReviewReduce extends Reducer<Text,DoubleWritable,Text,Text> {
		
		private Map<Text, DoubleWritable> countMap = new HashMap<>();
		
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context ) throws IOException, InterruptedException {
		
			int count=0;
			double sum = 0.0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			Double avg =  (sum/(double)count);
			countMap.put(new Text(key), new DoubleWritable(avg));
		}
		
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, DoubleWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                context.write(key, new Text((sortedMap.get(key)).toString()));
            }
        }
    }
	
	//To sort the values in descending order
	 private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
	        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

	        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

	            @Override
	            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
	                return o2.getValue().compareTo(o1.getValue());
	            }
	        });

	        //LinkedHashMap will keep the keys in the order they are inserted
	        //which is currently sorted on natural ordering
	        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

	        for (Map.Entry<K, V> entry : entries) {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }

	        return sortedMap;
	    }
	
// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: CountYelpReview <in> <out>");
			System.exit(2);
		}	 
		  
		Job job = Job.getInstance(conf, "CountYelp");
		job.setJarByClass(CountYelpReview.class);
	   
		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);
		job.setOutputKeyClass(Text.class);
		
		
		// set output value type
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

	
	