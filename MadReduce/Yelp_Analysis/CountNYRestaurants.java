/*
 * Included check for NY and business have Restaurants. Removed count in reduce to display only BusinessId and address
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

import YelpWordCount.YelpWordCount.CountYelpBusiness.BusinessMap;
import YelpWordCount.YelpWordCount.CountYelpBusiness.Reduce;

public class CountNYRestaurants {
public static class BusinessMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from business
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			
			if (businessData.length ==3) {
				if(businessData[1].contains(" NY ") && businessData[2].contains("Restaurants"))
					context.write((new Text(businessData[0]+" , "+businessData[1])), new IntWritable(1));
			}		
		}
	
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends Reducer<Text,IntWritable,Text,Text> {
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
		
			String[] newKey = StringUtils.split(key.toString(), ',');	
			String address= "";
			for (int i = 1; i< newKey.length; i++){
				address += newKey[i];
			}
			context.write(new Text(newKey[0]),new Text(address));
			
		}
	}

	
	
	
// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: CountYelpBusiness <in> <out>");
			System.exit(2);
		}
			  
		Job job = Job.getInstance(conf, "CountYelp");
		job.setJarByClass(CountNYRestaurants.class);
	   
		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		
		
		// set output value type
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
