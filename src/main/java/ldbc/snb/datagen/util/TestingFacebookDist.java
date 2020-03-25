package ldbc.snb.datagen.util;

import ldbc.snb.datagen.LdbcDatagen;
import ldbc.snb.datagen.generator.distribution.DegreeDistribution;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class TestingFacebookDist {

    public static void main(String[] args) throws Exception {

        Configuration conf = ConfigParser.initialize();
        ConfigParser.readConfig(conf, LdbcDatagen.class.getResourceAsStream("/params_default.ini"));
        LdbcDatagen.prepareConfiguration(conf);
        LdbcDatagen.initializeContext(conf);

        String string = conf.get("ldbc.snb.datagen.generator.distribution.degreeDistribution");
        DegreeDistribution degreeDistribution = (DegreeDistribution) Class.forName(string).newInstance();

        degreeDistribution.initialize(conf);

        System.out.println(degreeDistribution.mean(1500));
//              int N = 10000;
//        double[] results = new double[N];

//        for (int i = 0; i < N; i++) {
//            results[i] = degreeDistribution.nextDegree();
//        }
//        BufferedWriter out;
//        try {
//            out = new BufferedWriter(new FileWriter("fb_degree.csv"));
//            StringBuilder sb = new StringBuilder();
//            for (int j = 0; j < N - 1; j++) {
//                sb.append(results[j]);
//                sb.append(",");
//            }
//            sb.append(results[N - 1]);
//            sb.append("\n");
//            out.write(sb.toString());
//            out.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }
}
