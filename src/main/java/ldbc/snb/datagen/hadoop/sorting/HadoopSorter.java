package ldbc.snb.datagen.hadoop.sorting;

import java.io.IOException;

import ldbc.snb.datagen.DatagenParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HadoopSorter {

  public static class SortMapper extends Mapper<Object, Text, LongWritable, Text>{

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
      try{
        String date = line.toString().split(",")[0].substring(0,10)+
                line.toString().split(",")[0].substring(11,23); //extract the day of the event
        Date date1=new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date);// and convert it into a date object
        context.write(new LongWritable(date1.getTime()), line); //write the original data along with the epoch for the day
      }
      catch(Exception e) {System.out.println(e.getMessage());}
    }
}

  public static class SortReducer extends Reducer<LongWritable, Text,Text,NullWritable> {
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for(Text value:values) { //for all values at this time
        context.write(value,NullWritable.get()); //just output
      }
    }
  }

  public static class SortPartitioner extends Partitioner<LongWritable,Text> {
    String startyear = "01/01/"+DatagenParams.startYear; //get the starting year as specified in the params
    int numberOfYears = DatagenParams.numYears; //get the number of years
    long roughmsPerYear = 31536000000L; //rough number of milliseconds in a year
    public int getPartition(LongWritable key, Text value, int numReduceTasks){
      try{
        Long startTime=new SimpleDateFormat("DD/MM/YYYY").parse(startyear).getTime(); //convert the start year to a Epoch
        long increment = (numberOfYears*roughmsPerYear)/numReduceTasks; //define the size of increments by the
        int reducer = (int) ((key.get()-startTime)/increment); //get the chosen reducer by integer divide
        if(reducer > numReduceTasks-1) //just here to make sure that I haven't messed up
          return(numReduceTasks-1);
        else return reducer;
      }
      catch(Exception e) {System.out.println("hello");return 0;} //block has to be here, should never run
    }
  }

  public void run(String basePath, String toSort, String outputFileName) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "LDBCSort");

    job.setJarByClass(HadoopSorter.class);
    job.setMapperClass(SortMapper.class);
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
    if (!fs.exists(path)) {
      return;
    }
    FileStatus[] listStatus = fs.globStatus(path);
    for (FileStatus fstat : listStatus) {
      inputPaths.add(fstat.getPath());
    }
    FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
    FileOutputFormat.setOutputPath(job, new Path(outputFileName));
  }
}
