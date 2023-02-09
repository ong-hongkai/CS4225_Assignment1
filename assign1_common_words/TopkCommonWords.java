// Matric Number: 
// Name: 
// WordCount.java
import java.io.IOException;
import java.io.File;
import java.util.StringTokenizer;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopkCommonWords {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private ArrayList<String> stopWordList = new ArrayList<>();

    @Override
    protected void setup(Context context) throws java.io.IOException, InterruptedException {
      try {
        URI stopWordFileURI = context.getCacheFiles()[0];
        File stopWordFile = new File(stopWordFileURI);
        if (stopWordFile != null) {
          BufferedReader br = new BufferedReader(new FileReader(stopWordFile));
          String stopWord = null;
          while ((stopWord = br.readLine()) != null) {
            stopWordList.add(stopWord);
			    }
        }
      } catch (IOException e) {
        System.err.println("Exception reading stop word file: " + e);
      }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n\t\r\f ");
      while (itr.hasMoreTokens()) {
        String curr = itr.nextToken();
        if (curr.length() > 4 && !stopWordList.contains(curr)) {
        //   if (curr.length() > 4) {
            word.set(curr);
            context.write(word, one);
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TopkCommonWords.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    // Temporary test code
    job.addCacheFile(new Path(args[1]).toUri());
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
