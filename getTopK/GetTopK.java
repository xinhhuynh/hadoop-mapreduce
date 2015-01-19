import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Gets the top k records from an input file, ordered by a column (of type integer). 
// Input format: [ID field] <tab> [integer field]
// Output format: [ID field] <tab> [integer field]
public class GetTopK {

  public static class FindTopKMapper
       extends Mapper<Text, Text, IntWritable, Text>{

    private PriorityQueue<Record> currTopK;
    private int k;

    public static class Record implements Comparable<Record> {
      public final int sortBy;
      public final String key; 

      Record(int sortBy, String key) {
        this.sortBy = sortBy;
        this.key = key;
      }

      @Override
      public int compareTo(Record other) {
        if (this.sortBy < other.sortBy ) {
          return -1;
        } else if (this.sortBy > other.sortBy) {
          return 1;
        } else {
          return this.key.compareTo(other.key);
        }
      }

      @Override
      public boolean equals(Object other) {
        if (this == other) return true;

        if (other == null || !(other instanceof Record)) {
          return false;
        }

        Record otherRecord = (Record) other;
        return (this.sortBy == otherRecord.sortBy) &&
               this.key.equals(otherRecord.key);
      }

      @Override
      public int hashCode() {
        int hash = 1;
        hash = hash * 17 + sortBy;
        return hash * 31 + key.hashCode();
      }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      k = Integer.parseInt(context.getConfiguration().get("getTopK.k"));
      currTopK = new PriorityQueue<Record>(k);
    }

    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String recKey = key.toString();
        int recSortBy = Integer.parseInt(value.toString());
        if (currTopK.size() < k) {
          currTopK.add(new Record(recSortBy, recKey));
        } else {
          if (recSortBy > currTopK.peek().sortBy) {
            currTopK.poll();
            currTopK.add(new Record(recSortBy, recKey));
          }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      Text keyField = new Text();
      IntWritable sortByField = new IntWritable();
      for (Record rec : currTopK) {
        keyField.set(rec.key);
        sortByField.set(rec.sortBy);
        context.write(sortByField, keyField);
      }
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
    private int numWritten = 0;
    private int k;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      k = Integer.parseInt(context.getConfiguration().get("getTopK.k"));
    }

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
        if (numWritten < k) {
      	  context.write(val, key);
          numWritten++;
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("getTopK.k", "3");
    Job job = Job.getInstance(conf, "get top k");
    job.setJarByClass(GetTopK.class);
    job.setMapperClass(FindTopKMapper.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setReducerClass(WriteValKeyReducer.class);
    // Need to set map output key/value because different from final output key/value.
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setSortComparatorClass(IntDescComparator.class);
    KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

