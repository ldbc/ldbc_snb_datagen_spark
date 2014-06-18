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


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import ldbc.socialnet.dbgen.objects.GPS;


public class StreamStoreManager {

	FileOutputStream	fos; 
	ObjectOutputStream 	oos;
	
	FileInputStream		fis;
	ObjectInputStream	ois;
	
	String				outFileName = "";
	String 				sortedFileName = "";

	int					cellSize;
	int					windowSize;
	int 				mapId; 

	int					numberSerializedObject = 0;
	int					numberDeSerializedObject = 0;
	
	String				baseDir; 
	
	public StreamStoreManager(){}
	
	public StreamStoreManager(int _cellSize, int _windowSize, String _outFileName, String _baseDir, int _mapId){
		this.cellSize = _cellSize;
		this.windowSize = _windowSize;
		this.mapId = _mapId; 
		this.outFileName = _mapId + "_" + _outFileName; 
		this.sortedFileName = outFileName + ".sorted";
		this.baseDir = _baseDir; 
	}

	
	public String getOutFileName() {
		return outFileName;
	}

	public void setOutFileName(String outFileName) {
		this.outFileName = outFileName;
	}

	public String getSortedFileName() {
		return sortedFileName;
	}

	public void setSortedFileName(String sortedFileName) {
		this.sortedFileName = sortedFileName;
	}

	public void initSerialization() {
		try {
			numberSerializedObject = 0;
			
			fos = new FileOutputStream(baseDir + outFileName);
			oos = new ObjectOutputStream(fos);
			
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
	
	public void serialize(GPS gps){
		try {
			oos.writeObject(gps);
			numberSerializedObject++;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void endSerialization() {
		try {
			fos.close();
			oos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}                                                                              
	}	

	
	public void initDeserialization(String inputfile) {
		numberDeSerializedObject = 0;
		try {
			fis = new FileInputStream(baseDir + inputfile);
			ois = new ObjectInputStream(fis);
			

		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
	
	public void endDeserialization() {
		try {
			fis.close();
			ois.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public int getCellSize() {
		return cellSize;
	}

	public void setCellSize(int cellSize) {
		this.cellSize = cellSize;
	}
	public int getWindowSize() {
		return windowSize;
	}

	public void setWindowSize(int windowSize) {
		this.windowSize = windowSize;
	}
	
	public int getNumberSerializedObject() {
		return numberSerializedObject;
	}
	public int getNumberDeSerializedObject() {
		return numberDeSerializedObject;
	}

	public void setNumberDeSerializedObject(int numberDeSerializedObject) {
		this.numberDeSerializedObject = numberDeSerializedObject;
	}

}

