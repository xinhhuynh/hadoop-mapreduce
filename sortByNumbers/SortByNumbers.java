import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Sorts an input file by a column (of type integer), descending order. 
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

  public static class IntDescComparator extends WritableComparator {
    
    public IntDescComparator() {
      super(IntWritable.class);
    }

    @Override
    // the optimized compare method.
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
      Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

      return v1.compareTo(v2) * (-1);
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
    // Need to set map output key/value because different from final output key/value.
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setSortComparatorClass(IntDescComparator.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

