/*
 * Edited the WordCount3 class to read a stop words file from HDFS path and exclude the words from the input files while prcessing
 */

package WordCount.WordCount;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WordCount3 extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount3(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
	    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
	    conf.set("mapreduce.framework.name", "yarn");
	    if (args.length > 2){
	    	conf.setBoolean("wordcount.skip.patterns", true);
	    	conf.set("wordcount.skip.patterns", args[2]);
	        LOG.info("Added file to the args: " + args[2]);
	    }
    Job job = Job.getInstance(conf, "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);    
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private boolean caseSensitive = false;
    private long numRecords = 0;
    private String input;
    private Set<String> patternsToSkip = new HashSet<String>();
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    @Override 
    protected void setup(Mapper.Context context)
        throws IOException,
        InterruptedException {
      if (context.getInputSplit() instanceof FileSplit) {
        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
      } else {
        this.input = context.getInputSplit().toString();
      }
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
      if (config.getBoolean("wordcount.skip.patterns", false)) {
    	  String abspath = config.get("wordcount.skip.file", "not set");
          LOG.info("Added file to the distributed cache: " + abspath.toString());
          System.out.println("Added file to the distributed cache: " + abspath.toString());
          //parseSkipFile(abspath);
      }
      parseSkipFile("assignment1/stopWords.txt");
    }

    private void parseSkipFile(String patternsURI) {
      LOG.info("Added file to the distributed cache: " + patternsURI);
      try {
		  Path pt=new Path(patternsURI);//Location of file in HDFS
          FileSystem fs = FileSystem.get(new Configuration());
          BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		  String pattern = br.readLine();
		  while(pattern!= null){
			  patternsToSkip.add(pattern.toLowerCase());
			  pattern = br.readLine();
		  }
	    } catch (IOException ioe) {
	        System.err.println("Caught exception while parsing the cached file '"
	            + patternsURI + "' : " + StringUtils.stringifyException(ioe));
	    }
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      if (!caseSensitive) {
        line = line.toLowerCase();
      }
      Text currentWord = new Text();
      for (String word : WORD_BOUNDARY.split(line)) {
    	  if(patternsToSkip.isEmpty()){
    	      LOG.info("skip set empty: ");  
    	  }
    	  else{
    		  LOG.info("Skip:"+patternsToSkip.size());
    		  System.out.println("Skip:"+patternsToSkip.size());
    	  }
        if (word.isEmpty() || patternsToSkip.contains(word)) {
            continue;
        }
            currentWord = new Text(word);
            context.write(currentWord,one);
        }             
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}

