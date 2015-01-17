import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Joins two input files using CompositeInputFormat.
// Input format: [ID field] <tab> [other data]
// Output format: [ID field] <tab> [joined data]
public class MapsideJoin {

  public static class CombineValuesMapper
       extends Mapper<Text, TupleWritable, Text, Text> {

    private Text combined = new Text();
    private StringBuilder valueBuilder = new StringBuilder();
    private static final String OUTPUT_SEPARATOR = "\t";

    public void map(Text key, TupleWritable values, Context context
                    ) throws IOException, InterruptedException {
        for (Writable writable : values) {
          valueBuilder.append(writable.toString()).append(OUTPUT_SEPARATOR);
        }
        valueBuilder.setLength(valueBuilder.length() - 1);
        combined.set(valueBuilder.toString());
        context.write(key, combined);
        valueBuilder.setLength(0);
    }
  }

  public static class WriteKeyValReducer
       extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
      	context.write(key, val);
      }
    }
  }

  private static Configuration getMapJoinConfiguration(String... paths) {
    Configuration config = new Configuration();
    config.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
    String joinExpression = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, paths);
    config.set("mapred.join.expr", joinExpression);
    return config;
  }

  public static void main(String[] args) throws Exception {
    // args: paths to two input files, one output file
    Configuration conf = getMapJoinConfiguration(args[0], args[1]);
    Job job = Job.getInstance(conf, "map-side join");
    job.setJarByClass(MapsideJoin.class);
    job.setMapperClass(CombineValuesMapper.class);
    job.setReducerClass(WriteKeyValReducer.class);
    job.setInputFormatClass(CompositeInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class); 
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

