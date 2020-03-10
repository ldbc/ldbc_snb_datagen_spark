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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HadoopDeletionTimeSorter {

    public static class DeletionSortMapper extends Mapper<Object, Text, LongWritable, Text> {
        String lastDate = "01-01-" + (DatagenParams.startYear + DatagenParams.numYears)+"00:00:00";

        String firstDate = "01-01-" + (DatagenParams.startYear + DatagenParams.numYears)+"00:00:00";
        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            try {
                String date = line.toString().substring(30, 40) + line.toString().substring(41, 53); //extract the day of the event
                Date updateDate = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss.SSS").parse(date);// and convert it into a date object
                Date lastAllowedDate = new SimpleDateFormat("dd-MM-yyyyHH:mm:ss").parse(lastDate);// and convert it into a date object
                Date firstAllowedDate = new SimpleDateFormat("dd-MM-yyyyHH:mm:ss").parse(lastDate);// and convert it into a date object
                if(updateDate.before(lastAllowedDate) && updateDate.after(firstAllowedDate))
                    context.write(new LongWritable(updateDate.getTime()), line); //write the original data along with the epoch for the day
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public void run(String basePath, String toSort, String outputFileName) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LDBCDeleteSort");

        job.setJarByClass(HadoopCreationTimeSorter.class);
        job.setMapperClass(DeletionSortMapper.class);
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
            inputPaths.add(fstat.getPath());
        }
        FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
        FileOutputFormat.setOutputPath(job, new Path(outputFileName));
        job.waitForCompletion(true);
    }
}
