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

import ldbc.socialnet.dbgen.objects.ReducedUserProfile;


public class StorageManager {
	FileInputStream		fis;
	ObjectInputStream	ois;
	int					cellSize;
	int					windowSize;
	int					numberDeSerializedObject = 0;

	public StorageManager(int _cellSize, int _windowSize ){
		this.cellSize = _cellSize;
		this.windowSize = _windowSize;
	}
	
	public void deserializeOneCellUserProfile(int startIdex, int cellSize,
			ReducedUserProfile userProfiles[]) {
		try {
			for (int i = 0; i < cellSize; i++) {
/*				if (userProfiles[startIdex + i] != null){
					userProfiles[startIdex + i].clear();				
					userProfiles[startIdex + i] = null;
				}*/
				userProfiles[startIdex + i] = (ReducedUserProfile) ois.readObject();
				numberDeSerializedObject++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);

		}
	}
	public void deserializeOneCellUserProfile(ReducedUserProfile userProfilesCell[]) {
		try {
			for (int i = 0; i < cellSize; i++) {
/*				if (userProfilesCell[i] != null){
					userProfilesCell[i].clear();
					userProfilesCell[i] = null;
				}
				*/
				ReducedUserProfile user = (ReducedUserProfile) ois.readObject();
				userProfilesCell[i] = user;
				numberDeSerializedObject++;
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);

		}
	}	

	public void initDeserialization(String inputfile) {
		numberDeSerializedObject = 0;
		try {
			fis = new FileInputStream( inputfile);
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

	public int getNumberDeSerializedObject() {
		return numberDeSerializedObject;
	}

}
