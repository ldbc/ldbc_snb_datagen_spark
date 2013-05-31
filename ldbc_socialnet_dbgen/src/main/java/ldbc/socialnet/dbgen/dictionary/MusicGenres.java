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
package ldbc.socialnet.dbgen.dictionary;

public class MusicGenres {
	/*
	byte	pop;
	byte	rock;
	byte	metal;
	byte	dance;
	byte	blues;
	byte	classical;
	byte	hiphop;
	byte	rb;

	
	public MusicGenres(byte _pop, byte _rock, byte _metal, byte _dance, byte _blues, 
					byte _classical, byte _hiphop, byte _rb){
		this.pop = _pop; 
		this.rock = _rock;
		this.metal = _metal; 
		this.dance = _dance; 
		this.blues = _blues; 
		this.soul = _soul; 
		this.classical = _classical; 
		this.hiphop = _hiphop;
		this.rb = _rb; 
		this.jazz = _jazz; 
	}
		*/
	
	byte genres[];
	
	public MusicGenres(byte _genres[]){
		genres = _genres;
	}
	public MusicGenres(int numofgenres){
		this.genres = new byte[numofgenres];
		for (int i = 0; i < numofgenres; i++){
			genres[i] = 0;
		}
	}
	public void setValueForGenre(int genreIdx, byte value){
		genres[genreIdx] = value; 
	}
	public byte getValueOfGenre(int genreIdx){
		return genres[genreIdx]; 
	}
	public MusicGenres(byte _pop, byte _rock, byte _metal, byte _dance, byte _blues,  
			byte _classical, byte _hiphop, byte _rb){
		genres = new byte[8];	
		genres[0] = _pop; 
		genres[1] = _rock;
		genres[2] = _metal; 
		genres[3] = _dance; 
		genres[4] = _blues; 
		genres[5] = _classical; 
		genres[6] = _hiphop;
		genres[7] = _rb; 
		
	}
	public byte[] getGenres() {
		return genres;
	}
	public void setGenres(byte[] genres) {
		this.genres = genres;
	}
}
