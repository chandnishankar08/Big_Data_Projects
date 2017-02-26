
package YelpWordCount.YelpWordCount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.*;


public class CountTXBusiness extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CountTXBusiness(), args);
    }

    @Override
    public int run(String[] args) throws Exception{
    	Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: CountTXBusiness <business> <review>  <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Count TX Business");
        job.setJarByClass(CountTXBusiness.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperOne.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapperTwo.class);
       
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(BusinessPartitioner.class);
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
    	
    }
    /**
     * The mapper reads one line at the time, splits it into an array of single zipCodes and emits every
     * word to the reducers with the value of 1.
     */
    public static class MapperTwo extends Mapper<Object, Text, Text, IntWritable>{
    	 @Override
         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         	String delims = "^";
 			String[] reviewData = StringUtils.split(value.toString(),delims);
 			try{
	 			if (reviewData.length == 4) {
	 					context.write(new Text(reviewData[2]+",A"), new IntWritable(1));
	 			}	
 			}
 			catch(Exception e){
 				context.write(new Text(reviewData[2]+",A"), new IntWritable(0));
 			}
         }
    }
    
    public static class MapperOne extends Mapper<Object, Text, Text, IntWritable> {


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			
			if (businessData.length == 3) {
				if(businessData[1].contains("TX"))
					context.write(new Text(businessData[0]+",B"), new IntWritable(1));
			}	
        }
    }
    
    public static class BusinessPartitioner extends Partitioner<Text, Text>{
    
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			// TODO Auto-generated method stub
			String[] keys = StringUtils.split(key.toString(), ",");
			
			return keys[0].hashCode() % numReduceTasks;
		}
    }
      
   
    public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<String, Integer> revCount = new HashMap<>();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        	String[] keys = StringUtils.split(key.toString(), ",");
        	int count = 0;
    		if (keys[1].equals("A")){
    			for (IntWritable val: values){
    				count++;
    			}
    			revCount.put(keys[0], count);
    		}
    		else{
    			if(revCount.containsKey(keys[0])){
	    				context.write(new Text(keys[0]), new IntWritable(revCount.get(keys[0])));
	    			}
    			else{
        			context.write(new Text(keys[0]), new IntWritable(0));
        		}
    			
    		
    		}
	        	
	        	
        	}
        }
        
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}

	

}