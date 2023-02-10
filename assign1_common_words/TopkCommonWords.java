// Matric Number: 
// Name: 
// WordCount.java
import java.io.IOException;
import java.io.File;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.SortedMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;


public class TopkCommonWords {

    public static class TokenizerMapper1 extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private ArrayList<String> stopWordList = new ArrayList<>();

        @Override
        protected void setup(Context context) throws java.io.IOException, InterruptedException {
            try {
                Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (localCacheFiles != null) {
                    BufferedReader br = new BufferedReader(new FileReader(new File(localCacheFiles[0].toUri())));
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
                    word.set(curr);
                    context.write(word, new Text("file_1"));
                }
            }
        }
    }

    public static class TokenizerMapper2 extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private ArrayList<String> stopWordList = new ArrayList<>();

        @Override
        protected void setup(Context context) throws java.io.IOException, InterruptedException {
            try {
                Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (localCacheFiles != null) {
                    BufferedReader br = new BufferedReader(new FileReader(new File(localCacheFiles[0].toUri())));
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
                    word.set(curr);
                    context.write(word, new Text("file_2"));
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, IntWritable> {
    private TreeMap<Integer, String> kList = new TreeMap<>();
    
    public void reduce(org.w3c.dom.Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Integer> counterMap = new HashMap<String, Integer>();
        for (Text val : values) {
            String fileName = val.toString();
            if (counterMap.containsKey(fileName)) {
                counterMap.put(fileName, counterMap.get(fileName) + 1);
            } else {
                counterMap.put(fileName, 1);
            }
        }

        if (counterMap.size() == 2) {
            Integer count = Collections.min(counterMap.values());
            IntWritable result = new IntWritable(Collections.min(counterMap.values())); 
            context.write(key, result);
            if (kList.size() < 10) {
                kList.put(count, key.toString());
            } else {
                boolean isGreaterCount = count.compareTo(kList.firstKey()) > 0;
                boolean isSameCount = count.compareTo(kList.firstKey()) == 0;
                boolean isHigherLexi = kList.get(kList.firstKey()).compareTo(key.toString()) < 0;
                if (isGreaterCount || (isSameCount && isHigherLexi)) {
                    //Remove lowest key
                    kList.pollFirstEntry();
                    //Add new key value mapping
                    kList.put(count, key.toString());
                }
            }
        }
    }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        NavigableMap<Integer, String> nMap = kList.descendingMap();
        Map.Entry<Integer, String> entry = nMap.pollLastEntry();
        while (entry != null) {
            context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            entry = nMap.pollLastEntry();
        }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(TopkCommonWords.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    // Place stopwords into distributed cache
    job.addCacheFile(new Path(args[2]).toUri());
    // Add the two files as input
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper1.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class);

    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
