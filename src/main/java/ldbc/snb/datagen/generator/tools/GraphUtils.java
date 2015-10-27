package ldbc.snb.datagen.generator.tools;

import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;

import java.util.*;

/**
 * Created by aprat on 17/06/15.
 */
public class GraphUtils {

    public static double ClusteringCoefficient( PersonGraph graph ) {
        double CC = 0.0;
        int numEdges = 0;
        for( Long l : graph.persons()) {
            int triangles = 0;
            Set<Long> neighbors = graph.neighbors(l);
            for( Long n : neighbors){
                Set<Long> neighbors2 = graph.neighbors(n);
                Set<Long> aux = new HashSet<Long>(neighbors);
                aux.retainAll(neighbors2);
                triangles+=aux.size();
                numEdges++;
            }
            int degree = neighbors.size();
            if(degree > 1)
                CC+=triangles / (double)(degree*(degree-1));
        }
        return CC / graph.persons().size();
    }

    public static ArrayList<Double> ClusteringCoefficientList( PersonGraph graph ) {
        ArrayList<Double> CC = new ArrayList<Double>();
        int numEdges = 0;
        for( Long l : graph.persons()) {
            int triangles = 0;
            Set<Long> neighbors = graph.neighbors(l);
            for( Long n : neighbors){
                Set<Long> neighbors2 = graph.neighbors(n);
                Set<Long> aux = new HashSet<Long>(neighbors);
                aux.retainAll(neighbors2);
                triangles+=aux.size();
                numEdges++;
            }
            int degree = neighbors.size();
            double localCC = 0;
            if(degree > 1)
                localCC=triangles / (double)(degree*(degree-1));
            CC.add(localCC);

        }
        return CC;
    }
}
