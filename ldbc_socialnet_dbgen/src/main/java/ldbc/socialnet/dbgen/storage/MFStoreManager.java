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
package ldbc.socialnet.dbgen.storage;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import ldbc.socialnet.dbgen.objects.ReducedUserProfile;


public class MFStoreManager extends StorageManager {
	
	boolean 			isMultiFile = true; 
	int 				numFiles;
	int 				lastCell;
	
	int					numCellPerFile;
	int					numObjectPerFile; 
	
	String 				mulpassOutUserProf[]; 
	FileOutputStream 	fos[]; 
	ObjectOutputStream 	oos[]; 
	int					numSerializedObject;
	int					lastfileIdx = -2;
	
	public MFStoreManager(int _cellSize, int _windowSize, int pass, 
						  int _lastCell, int _nFile, String _outUserProfile, String _baseDir){
		numSerializedObject = 0;
		cellSize = _cellSize;
		windowSize = _windowSize;
		numFiles = _nFile;
		lastCell = _lastCell; 
		outUserProfile = _outUserProfile; 
		baseDir = _baseDir; 
		
		numCellPerFile = (lastCell + 1) / numFiles; 
		numObjectPerFile = numCellPerFile * cellSize;
			
		mulpassOutUserProf = new String[numFiles];
		fos = new FileOutputStream[numFiles];
		oos = new ObjectOutputStream[numFiles];
	}
	
	public void initSerialization(int pass) {
		try {
			numberSerializedObject = 0;
			
			for (int i = 0; i < numFiles; i++){
				mulpassOutUserProf[i] = pass + "_" + outUserProfile + "_" + i;
				fos[i] = new FileOutputStream(baseDir + mulpassOutUserProf[i]);
				oos[i] = new ObjectOutputStream(fos[i]);
			}
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	
	public void serialize(int from, int to, int pass, 
			ReducedUserProfile userProfiles[]) 
	{
        int curfileIdx = -1; 

        
        curfileIdx = numberSerializedObject/numObjectPerFile;

        
		// The last file may have more cells than other files
		if (curfileIdx == numFiles)
			curfileIdx = numFiles - 1;
		
		serializeReducedUserProfiles(from, to, pass, userProfiles, oos[curfileIdx]);
		try {
			oos[curfileIdx].flush();
			
			// close the file
			if ((lastfileIdx != curfileIdx) && (lastfileIdx > -1)) {
				oos[lastfileIdx].close();
				fos[lastfileIdx].close();
				lastfileIdx = curfileIdx; 
			}
			// For the first file
			if (lastfileIdx != curfileIdx){
				lastfileIdx = curfileIdx;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error while flushing the cell a users");
			e.printStackTrace();
		}

	}
	public void serializeCellUsers(int pass, 
			ReducedUserProfile userProfiles[]) 
	{
        int curfileIdx = -1; 

        
        curfileIdx = numberSerializedObject/numObjectPerFile;

        
		// The last file may have more cells than other files
		if (curfileIdx == numFiles)
			curfileIdx = numFiles - 1;
		
		serializeReducedUserProfiles(0, cellSize, pass, userProfiles, oos[curfileIdx]);
		
		try {
			oos[curfileIdx].flush();
			
			// close the file
			if ((lastfileIdx != curfileIdx) && (lastfileIdx > -1)) {
				oos[lastfileIdx].close();
				fos[lastfileIdx].close();
				lastfileIdx = curfileIdx; 
			}
			// For the first file
			if (lastfileIdx != curfileIdx){
				lastfileIdx = curfileIdx;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error while flushing the cell a users");
			e.printStackTrace();
		}
	}
	public void endSerialization() {
		try {
			fos[lastfileIdx].close();
			oos[lastfileIdx].close();
		} catch (Exception e) {
			e.printStackTrace();
		}                                                                              
	}
	public String[] getMulpassOutUserProf() {
		return mulpassOutUserProf;
	}

	public void setMulpassOutUserProf(String[] mulpassOutUserProf) {
		this.mulpassOutUserProf = mulpassOutUserProf;
	}

}
