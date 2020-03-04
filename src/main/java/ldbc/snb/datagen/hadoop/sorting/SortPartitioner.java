package ldbc.snb.datagen.hadoop.sorting;

import ldbc.snb.datagen.DatagenParams;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.text.SimpleDateFormat;

public class SortPartitioner extends Partitioner<LongWritable,Text> {
    String startyear = "01/01/"+ DatagenParams.startYear; //get the starting year as specified in the params
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
