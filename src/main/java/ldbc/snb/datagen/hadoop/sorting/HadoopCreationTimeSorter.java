package ldbc.snb.datagen.hadoop.sorting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HadoopCreationTimeSorter {

  public static class CreationSortMapper extends Mapper<Object, Text, LongWritable, Text>{

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
      try{
        String date = line.toString().substring(0,10)+line.toString().substring(11,23); //extract the day of the event
        Date date1=new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date);// and convert it into a date object
        context.write(new LongWritable(date1.getTime()), line); //write the original data along with the epoch for the day
      }
      catch(Exception e) {System.out.println(e.getMessage());}
    }
}

  public void run(String basePath, String toSort, String outputFileName) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "LDBCSort");

    job.setJarByClass(HadoopCreationTimeSorter.class);
    job.setMapperClass(CreationSortMapper.class);
    job.setReducerClass(SortReducer.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setPartitionerClass(SortPartitioner.class);
    job.setNumReduceTasks(4);
    List<Path> inputPaths = new ArrayList<>();
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(basePath + "/" + toSort);
    FileStatus[] listStatus = fs.globStatus(path);
    for (FileStatus fstat : listStatus) {
      System.out.println(fstat.getPath());
      inputPaths.add(fstat.getPath());
    }
    FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
    FileOutputFormat.setOutputPath(job, new Path(outputFileName));
    job.waitForCompletion(true);
  }
}
