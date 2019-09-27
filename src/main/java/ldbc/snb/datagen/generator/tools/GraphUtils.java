/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.generator.tools;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by aprat on 17/06/15.
 */
public class GraphUtils {

    public static double clusteringCoefficient(PersonGraph graph) {
        double CC = 0.0;
        for (Long l : graph.persons()) {
            int triangles = 0;
            Set<Long> neighbors = graph.neighbors(l);
            for (Long n : neighbors) {
                Set<Long> neighbors2 = graph.neighbors(n);
                Set<Long> aux = new HashSet<Long>(neighbors);
                aux.retainAll(neighbors2);
                triangles += aux.size();
            }
            int degree = neighbors.size();
            if (degree > 1)
                CC += triangles / (double) (degree * (degree - 1));
        }
        return CC / graph.persons().size();
    }

    public static ArrayList<Double> clusteringCoefficientList(PersonGraph graph) {
        ArrayList<Double> CC = new ArrayList<Double>();
        int numEdges = 0;
        for (Long l : graph.persons()) {
            int triangles = 0;
            Set<Long> neighbors = graph.neighbors(l);
            for (Long n : neighbors) {
                Set<Long> neighbors2 = graph.neighbors(n);
                Set<Long> aux = new HashSet<Long>(neighbors);
                aux.retainAll(neighbors2);
                triangles += aux.size();
                numEdges++;
            }
            int degree = neighbors.size();
            double localCC = 0;
            if (degree > 1)
                localCC = triangles / (double) (degree * (degree - 1));
            CC.add(localCC);

        }
        return CC;
    }
}
