/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.socialnet.dbgen.generator;

import java.io.IOException;
import java.io.ObjectOutputStream;

import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.util.ComposedKey;
import ldbc.socialnet.dbgen.util.MapReduceKey;

import ldbc.socialnet.dbgen.util.TupleKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class MRWriter {
	int windowSize; 
	int cellSize; 
	int numberSerializedObject;
	String baseDir; 
	
	public MRWriter(int _cellSize, int _windowSize, String _baseDir){
		this.cellSize = _cellSize;
		this.windowSize = _windowSize;
		this.baseDir = _baseDir; 
		numberSerializedObject = 0; 
		
	}

	public void writeReducedUserProfiles(int from, int to, int pass,
                                         ReducedUserProfile userProfiles[], Reducer.Context context) {
        try {
            to = to % windowSize;
            for (int i = from; i != to; i = (i+1)%windowSize) {
                context.write(new TupleKey(userProfiles[i].getDicElementId(pass),userProfiles[i].getAccountId()), userProfiles[i]);
                numberSerializedObject++;
            }
        }
        catch (IOException i) {
            i.printStackTrace();

        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
	}
}
