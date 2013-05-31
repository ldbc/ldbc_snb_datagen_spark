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
package ldbc.socialnet.dbgen.util;
// filename: ExternalSort.java
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import ldbc.socialnet.dbgen.objects.ReducedUserProfile;


/**
* Goal: offer a generic external-memory sorting program in Java.
* 
* It must be : 
*  - hackable (easy to adapt)
*  - scalable to large files
*  - sensibly efficient.
*
* This software is in the public domain.
*
* Usage: 
*  java com/google/code/externalsorting/ExternalSort somefile.txt out.txt
* 
* You can change the default maximal number of temporary files with the -t flag:
*  java com/google/code/externalsorting/ExternalSort somefile.txt out.txt -t 3
*
* For very large files, you might want to use an appropriate flag to allocate
* more memory to the Java VM: 
*  java -Xms2G com/google/code/externalsorting/ExternalSort somefile.txt out.txt
*
* By (in alphabetical order) 
*   Philippe Beaudoin,  Jon Elsas,  Christan Grant, Daniel Haran, Daniel Lemire, 
*  April 2010
* originally posted at 
*  http://www.daniel-lemire.com/blog/archives/2010/04/01/external-memory-sorting-in-java/
*/
public class ExternalSort {
	
	static int 			DEFAULTMAXTEMPFILES = 1024;
	public static int 	passValue = 0;
	static int 			sizeOfaFriendShip = 60;  //bytes; each integer is 4 bytes 
	static int 			numofPasses = 3;  //bytes; each integer is 4 bytes
	
	// we divide the file into small blocks. If the blocks
	// are too small, we shall create too many temporary files. 
	// If they are too big, we shall be using too much memory. 
	public static long estimateBestSizeOfBlocks(File filetobesorted, int maxtmpfiles) {
		//long sizeoffile = filetobesorted.length() * 2;
		long sizeoffile = filetobesorted.length();
		
		System.out.println("Size of file is " + sizeoffile);
		//System.out.println("Total space of file is " + filetobesorted.getTotalSpace());
		
		/**
		* We multiply by two because later on someone insisted on counting the memory
		* usage as 2 bytes per character. By this model, loading a file with 1 character
		* will use 2 bytes.
		*/ 
		// we don't want to open up much more than maxtmpfiles temporary files, better run
		// out of memory first.
		long blocksize = sizeoffile / maxtmpfiles + (sizeoffile % maxtmpfiles == 0 ? 0 : 1) ;
		
		// on the other hand, we don't want to create many temporary files
		// for naught. If blocksize is smaller than half the free memory, grow it.
		long freemem = Runtime.getRuntime().freeMemory();
		/*
		if( blocksize < freemem/2) {
		    blocksize = freemem/2;
		} 
		*/
		if( blocksize < freemem/2) {
		    blocksize = freemem/2;
		}
		/*
		long totalmem = Runtime.getRuntime().totalMemory();
		System.out.println("Freemem is " + freemem/(1024*1024));
		System.out.println("Totalmem is " + totalmem/(1024*1024));
		System.out.println("Block size is " + blocksize/(1024*1024));
		*/
		return blocksize;
	}

	/**
	 * This will simply load the file by blocks of x rows, then
	 * sort them in-memory, and write the result to  
	 * temporary files that have to be merged later.
	 * 
	 * @param file some flat  file
	 * @param cmp string comparator 
	 * @return a list of temporary flat files
	 */
	public static List<File> sortInBatch(File file, Comparator<ReducedUserProfile> cmp) throws IOException {		return sortInBatch(file, cmp,DEFAULTMAXTEMPFILES);	}
	
	
	/**
	 * This will simply load the file by blocks of x rows, then
	 * sort them in-memory, and write the result to 
	 * temporary files that have to be merged later. You can
	 * specify a bound on the number of temporary files that
	 * will be created.
	 * 
	 * @param file some flat  file
	 * @param cmp UserProfle comparator 
	 * @param maxtmpfiles
	 * @return a list of temporary flat files
	 */
	public static List<File> sortInBatch(File file, Comparator<ReducedUserProfile> cmp, int maxtmpfiles) throws IOException {
		List<File> files = new ArrayList<File>();
		ObjectInputStream fbr = new ObjectInputStream(new FileInputStream(file));
		long blocksize = estimateBestSizeOfBlocks(file,maxtmpfiles);// in bytes

		try{
			List<ReducedUserProfile> tmplist =  new ArrayList<ReducedUserProfile>();
			ReducedUserProfile user = new ReducedUserProfile() ;
			try {
					while (user != null){
						long currentblocksize = 0;// in bytes
						while((currentblocksize < blocksize) 
						&&(   (user = (ReducedUserProfile)fbr.readObject()) != null) ){ // as long as you have enough memory
							tmplist.add(user);
							currentblocksize +=  getSizeOfObject(user) ; // Size of the reducedUserProfile object
						}
						files.add(sortAndSave(tmplist,cmp));
						tmplist.clear();
					}
				
			} catch(EOFException oef) {
				System.out.println("End of file --- ");
				if(tmplist.size()>0) {
					files.add(sortAndSave(tmplist,cmp));
					tmplist.clear();
				}
			}
			 catch(Exception e) {
					System.out.println("Unknown error when doing external sorting ");
					e.printStackTrace();
						
			}
		
		} finally {
			fbr.close();
		}
		return files;
	}
	
	public static int getSizeOfObject(ReducedUserProfile user){
		int size = 49 + user.numFriends*(20 + sizeOfaFriendShip) + numofPasses*8;
		
		return size; 
	}

	public static File sortAndSave(List<ReducedUserProfile> tmplist, Comparator<ReducedUserProfile> cmp) throws IOException  {
		Collections.sort(tmplist,cmp);  
		File newtmpfile = File.createTempFile("sortInBatch", "flatfile");
		newtmpfile.deleteOnExit();
		ObjectOutputStream fbw = new ObjectOutputStream(new FileOutputStream(newtmpfile));
		try {
			for(ReducedUserProfile r : tmplist) {
				fbw.writeObject(r);
			}
		} finally {
			fbw.close();
		}
		
		return newtmpfile;
	}
	
	/**
	 * This merges a bunch of temporary flat files 
	 * @param files
	 * @param output file
         * @return The number of lines sorted. (P. Beaudoin)
	 */
	public static int mergeSortedFiles(List<File> files, File outputfile, final Comparator<ReducedUserProfile> cmp) throws IOException {
		PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(11, 
            new Comparator<BinaryFileBuffer>() {
              public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
                return cmp.compare(i.peek(), j.peek());
              }
            }
        );
		for (File f : files) {
			BinaryFileBuffer bfb = new BinaryFileBuffer(f);
			pq.add(bfb);
		}
		ObjectOutputStream fbw = new ObjectOutputStream(new FileOutputStream(outputfile));
		int objectCounter = 0;
		try {
			while(pq.size()>0) {
				BinaryFileBuffer bfb = pq.poll();
				ReducedUserProfile r = bfb.pop();
				fbw.writeObject(r);
				++objectCounter;
				if(bfb.empty()) {
					bfb.fbr.close();
					bfb.originalfile.delete();// we don't need you anymore
				} else {
					pq.add(bfb); // add it back
				}
			}
		} finally { 
			fbw.close();
			for(BinaryFileBuffer bfb : pq ) bfb.close();
		}
		return objectCounter;
	}
	
	public static void verifySorting(File sortedFile){
		try {
			ObjectInputStream oos = new ObjectInputStream(new FileInputStream(sortedFile));
			ReducedUserProfile user; 
			for (int i = 0; i < 10; i ++){
				user = (ReducedUserProfile)oos.readObject();
				System.out.println("User " + i + " " + user.getDicElementId(passValue));
				System.out.println("User " + i + " location: " + user.getDicElementId(0));
				System.out.println("User " + i + " interest: " + user.getDicElementId(1));
				System.out.println("User " + i + " random: " + user.getDicElementId(2));
			}
		} catch (FileNotFoundException e) {
			System.out.println("Cannot find the sorted file");
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	
	public void SortByDimensions(String[] args, int pass) throws IOException {
		
		boolean verbose = false;
		int maxtmpfiles = DEFAULTMAXTEMPFILES;
		String inputfile=null, outputfile=null;
		
		for(int param = 0; param<args.length; ++param) {
			if(args[param].equals("-v") ||  args[param].equals("--verbose"))
			  verbose = true;
			else if ((args[param].equals("-t") ||  args[param].equals("--maxtmpfiles")) && args.length>param+1) {
				param++;
			    maxtmpfiles = Integer.parseInt(args[param]);  
			} else {
				if(inputfile == null) 
				  inputfile = args[param];
				else if (outputfile == null)
				  outputfile = args[param];
				else System.out.println("Unparsed: "+args[param]); 
			}
		}
		
		if(outputfile == null) {
			System.out.println("please provide input and output file names");
			return;
		}
		
		this.passValue = pass; 
		
		Comparator<ReducedUserProfile> comparator = new Comparator<ReducedUserProfile>() {
			public int compare(ReducedUserProfile u1, ReducedUserProfile u2){
				if (u1.getDicElementId(passValue) < u2.getDicElementId(passValue)){
					return -1; 
				}
				else
					return 1; 
				//return (int)(u1.getCreatedDate()-u2.getCreatedDate()) ;
			}
		};
		
		List<File> l = sortInBatch(new File(inputfile), comparator, maxtmpfiles) ;
		
		//if(verbose) System.out.println("created "+l.size()+" tmp files");
		System.out.println("created "+l.size()+" tmp files");
		
		System.out.println("Memory after sort in Batch");

                long heapSize = Runtime.getRuntime().totalMemory();

                long heapMaxSize = Runtime.getRuntime().maxMemory();

                long heapFreeSize = Runtime.getRuntime().freeMemory();

                System.out.println(" ---------------------- ");
                System.out.println(" Current Heap Size: " + heapSize/(1024*1024));
                System.out.println(" Max Heap Size: " + heapMaxSize/(1024*1024));
                System.out.println(" Free Heap Size: " + heapFreeSize/(1024*1024));
                System.out.println(" ---------------------- ");

		mergeSortedFiles(l, new File(outputfile), comparator);
		
		//verifySorting(new File(outputfile));
		
		System.out.println("Done sorting for "+ pass +" pass!!! ==> output file " + outputfile);
	}
	
}



class BinaryFileBuffer  {
	public static int BUFFERSIZE = 2048;
	public ObjectInputStream fbr;
	public File originalfile;
	private ReducedUserProfile cache;
	private boolean empty;
	
	public BinaryFileBuffer(File f) throws IOException {
		originalfile = f;
		//fbr = new ObjectInputStream(new FileInputStream(f), BUFFERSIZE);
		fbr = new ObjectInputStream(new FileInputStream(f));
		reload();
	}
	
	public boolean empty() {
		return empty;
	}
	
	private void reload() throws IOException {
		try {
			this.cache = (ReducedUserProfile)fbr.readObject();
            empty = false;
          
		} catch(EOFException oef) {
			empty = true;
			cache = null;
		} catch(Exception e){
			empty = true;
			cache = null;			
			System.out.println("Exception in Reload ");
			e.printStackTrace();
		}
	}
	
	public void close() throws IOException {
		fbr.close();
	}
	
	
	public ReducedUserProfile peek() {
		if(empty()) return null;
		return cache;
	}
	public ReducedUserProfile pop() throws IOException {
		ReducedUserProfile answer = peek();
		reload();
	  return answer;
	}
}


