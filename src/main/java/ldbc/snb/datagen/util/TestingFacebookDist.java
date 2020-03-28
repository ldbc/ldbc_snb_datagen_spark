package ldbc.snb.datagen.util;

import ldbc.snb.datagen.LdbcDatagen;
import ldbc.snb.datagen.generator.distribution.DegreeDistribution;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class TestingFacebookDist {

    public static void main(String[] args) throws Exception {

        // init config.
        Configuration conf = ConfigParser.initialize();
        ConfigParser.readConfig(conf, LdbcDatagen.class.getResourceAsStream("/params_default.ini"));
        LdbcDatagen.prepareConfiguration(conf);
        LdbcDatagen.initializeContext(conf);

        String string = conf.get("ldbc.snb.datagen.generator.distribution.degreeDistribution");
        DegreeDistribution degreeDistribution = (DegreeDistribution) Class.forName(string).newInstance();

        degreeDistribution.initialize(conf);

        double[] scaleFactors = {0.1,0.3,1.0,3.0,10.0,30.0,100.0,300.0,1000.0};
        int[] personScaleFactors = {1500, 3500, 11000, 27000, 73000, 182000, 499000, 1250000, 3600000};
        double[] avDegree = new double[scaleFactors.length];

        for (int j = 0; j<scaleFactors.length;j++){
            avDegree[j] = degreeDistribution.mean(personScaleFactors[j]);
        }

        System.out.println(Arrays.toString(avDegree));

        BufferedWriter out;
        try {
            out = new BufferedWriter(new FileWriter("./tools/data/fb_av_degree.csv"));
            StringBuilder sb = new StringBuilder();
            sb.append("sf,persons,av_degree\n");
            for (int j = 0; j < avDegree.length - 1; j++) {
                sb.append(scaleFactors[j]);
                sb.append(",");
                sb.append(personScaleFactors[j]);
                sb.append(",");
                sb.append(avDegree[j]);
                sb.append("\n");
            }
            out.write(sb.toString());
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (double scaleFactor : scaleFactors) {

            conf.set("ldbc.snb.datagen.generator.numPersons", String.valueOf(scaleFactor));
            DegreeDistribution degreeDistribution2 = (DegreeDistribution) Class.forName(string).newInstance();

            degreeDistribution2.initialize(conf);
            int N = 1000;
            double[] results = new double[N];
            for (int j = 0; j < N; j++) {
                results[j] = degreeDistribution.nextDegree();
            }
            BufferedWriter output;
            try {
                output = new BufferedWriter(new FileWriter("./tools/data/fb_degree_dist.csv",true));
                StringBuilder sb = new StringBuilder();
                sb.append(scaleFactor + ",");
                for (int k = 0; k < N - 1; k++) {
                    sb.append(results[k] + ",");
                }
                sb.append(results[N - 1] + "\n");
                output.write(sb.toString());
                output.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }







    }
}
