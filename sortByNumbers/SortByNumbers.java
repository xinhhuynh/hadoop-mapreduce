import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Sorts an input file by a column (of type integer). 
// Input format: [ID field] <tab> [integer field]
// Output format: [ID field] <tab> [integer field]
public class SortByNumbers {

  public static class LineSplitterMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private Text idField = new Text();
    private IntWritable sortByField = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	String line = value.toString();
	String[] parts = line.split("\t");
	idField.set(parts[0]);
	sortByField.set(Integer.parseInt(parts[1]));
        context.write(sortByField, idField);
    }
  }

  public static class WriteValKeyReducer
       extends Reducer<IntWritable,Text,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
      	context.write(val, key);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "sort by numbers");
    job.setJarByClass(SortByNumbers.class);
    job.setMapperClass(LineSplitterMapper.class);
    job.setReducerClass(WriteValKeyReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

