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

package ldbc.snb.datagen.generator;

import java.util.TreeSet;
import java.util.Random;

import java.util.ArrayList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;
import ldbc.snb.datagen.objects.Post;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;


abstract public class PostGenerator {
	
	static protected class PostInfo {
		public TreeSet<Integer> tags;
		public long             date;
		public PostInfo() {
			this.tags = new TreeSet<Integer>();
		}
	}
	
	private static final String SEPARATOR = "  ";
	
	/* A set of random number generator for different purposes.*/
	
	public PostGenerator( ){
	}
	
	/** @brief Initializes the post generator.*/
	public void initialize() {
	}
	
	
	/** @brief Creates a set of posts for a user..
	 *  @param[in] user The user which we want to create the posts..
	 *  @param[in] extraInfo The extra information of the user.
	 *  @return The set of posts.*/
	public ArrayList<Post> createPosts(RandomGeneratorFarm randomFarm, Forum forum, ArrayList<ForumMembership> memberships, long numPosts, long startId ){
		long postId = startId;
		ArrayList<Post> result = new ArrayList<Post>();
		for( ForumMembership member : memberships ) {
			double numPostsMember = numPosts / (double)memberships.size();
			if( numPostsMember < 1.0 ) {
				double prob = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST).nextDouble();
				if( prob < numPostsMember ) numPostsMember = 1.0;
			} else {
				numPostsMember = Math.ceil(numPostsMember);
			}
			for( int i = 0; i < (int)(numPostsMember); ++i ) {
				PostInfo postInfo = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.TAG), randomFarm.get(RandomGeneratorFarm.Aspect.DATE), forum, member);
				if( postInfo != null ) {
					
					String content = "";
					int textSize;
					if( member.person().isLargePoster() && randomFarm.get(RandomGeneratorFarm.Aspect.LARGE_TEXT).nextDouble() > (1.0f-DatagenParams.ratioLargePost) ) {
						textSize = Dictionaries.tagText.getRandomLargeTextSize( randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE),DatagenParams.minLargePostSize, DatagenParams.maxLargePostSize );
					} else {
						textSize = Dictionaries.tagText.getRandomTextSize( randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), randomFarm.get(RandomGeneratorFarm.Aspect.REDUCED_TEXT), DatagenParams.minTextSize, DatagenParams.maxTextSize);
					}
					
					content = Dictionaries.tagText.generateText(randomFarm.get(RandomGeneratorFarm.Aspect.TEXT_SIZE), postInfo.tags, textSize );
					if( content.length() != textSize ) {
						System.out.println("ERROR while generating text - content size: "+ content.length()+", actual size: "+ textSize);
						System.exit(-1);
					}
					
					Post post = new Post( SN.formId(SN.composeId(postId,postInfo.date)),
						postInfo.date,
						member.person(),
						forum.id(),
						content,
						postInfo.tags,
						Dictionaries.ips.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP), randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER),member.person().ipAddress(), postInfo.date),
						Dictionaries.browsers.getPostBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER), member.person().browserId()),
						member.person().cityId(),
						forum.language());
					result.add(post);
					postId++;
				}
			}
		}
		return result;
	}
	
	/** @brief Returs the tag and creation date information of a post.
	 *  @param[in] group The group where the post belongs.
	 *  @param[in] membership The membership information of the user that creates the post.
	 *  @return The post info struct containing the information. null if it was not possible to generate.*/
	protected abstract PostInfo generatePostInfo( Random randomTag, Random randomDate, Forum forum, ForumMembership membership );
}
