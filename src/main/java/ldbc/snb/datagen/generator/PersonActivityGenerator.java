
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import ldbc.snb.datagen.serializer.UpdateEventSerializer;
import ldbc.snb.datagen.util.FactorTable;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Random;

public class PersonActivityGenerator {

	private ForumGenerator forumGenerator_ = null;
	private RandomGeneratorFarm randomFarm_ = null;
	private UniformPostGenerator uniformPostGenerator_ = null;
	private FlashmobPostGenerator flashmobPostGenerator_ = null;
	private PhotoGenerator photoGenerator_ = null;
	private CommentGenerator commentGenerator_ = null;
	private LikeGenerator likeGenerator_ = null;
	private PersonActivitySerializer personActivitySerializer_ = null;
	private UpdateEventSerializer updateSerializer_ = null;
	private long forumId = 0;
	private long messageId = 0;
    private FactorTable factorTable_;

	public PersonActivityGenerator( PersonActivitySerializer serializer, UpdateEventSerializer updateSerializer ) {
		personActivitySerializer_ = serializer;
		updateSerializer_ = updateSerializer;
		randomFarm_ = new RandomGeneratorFarm();
		// load generators
		forumGenerator_ = new ForumGenerator();
		//david
		Random random = new Random();
		TextGenerator generator = new LdbcSnbTextGenerator(random, Dictionaries.tags);
		//d: end
		uniformPostGenerator_ = new UniformPostGenerator(generator);
		flashmobPostGenerator_ = new FlashmobPostGenerator(generator);
		photoGenerator_ = new PhotoGenerator();
		commentGenerator_ = new CommentGenerator(generator);
		likeGenerator_ = new LikeGenerator();
        factorTable_ = new FactorTable();
	}

	private void generateActivity( Person person, ArrayList<Person> block ) {
		generateWall(person, block);
		generateGroups(person, block);
		generateAlbums(person, block);
        if(person.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
            factorTable_.extractFactors(person);
        }
	}

	public void reset() {
		personActivitySerializer_.reset();
	}

	private void generateWall( Person person, ArrayList<Person> block ) {
		// generate wall
		Forum wall = forumGenerator_.createWall(randomFarm_, forumId++, person);
		export(wall);
		for( ForumMembership fm : wall.memberships()) {
			export(fm);
		}

		// generate wall posts
		ForumMembership personMembership = new ForumMembership(wall.id(),
			wall.creationDate()+DatagenParams.deltaTime,  new Person.PersonSummary(person)
		);
		ArrayList<ForumMembership> fakeMembers = new ArrayList<ForumMembership>();
		fakeMembers.add(personMembership);
		ArrayList<Post> wallPosts = uniformPostGenerator_.createPosts(randomFarm_, wall, fakeMembers , numPostsPerGroup(randomFarm_, wall, DatagenParams.maxNumPostPerMonth, DatagenParams.maxNumFriends), messageId);
		long aux = messageId + wallPosts.size();
		wallPosts.addAll(flashmobPostGenerator_.createPosts(randomFarm_, wall, fakeMembers, numPostsPerGroup(randomFarm_, wall, DatagenParams.maxNumFlashmobPostPerMonth, DatagenParams.maxNumFriends), aux ));
		messageId+=wallPosts.size();

		for( Post p : wallPosts ) {
			export(p);
			// generate likes to post
			if( randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
                ArrayList<Like> postLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), wall, p, Like.LikeType.POST);
				for( Like l : postLikes ) {
					export(l);
				}
			}
			//// generate comments
			int numComments = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_COMMENT).nextInt(DatagenParams.maxNumComments+1);
			ArrayList<Comment> comments = commentGenerator_.createComments(randomFarm_, wall, p, numComments, messageId);
			messageId+=comments.size();
			for( Comment c : comments ) {
				export(c);
				// generate likes to comments
				if( c.content().length() > 10 && randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
					ArrayList<Like> commentLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), wall, c, Like.LikeType.COMMENT);
					for( Like l : commentLikes ) {
						export(l);
					}
				}
			}
		}

	}

	private void generateGroups( Person person, ArrayList<Person> block ) {
		// generate user created groups
		double moderatorProb = randomFarm_.get(RandomGeneratorFarm.Aspect.FORUM_MODERATOR).nextDouble();
		if (moderatorProb <= DatagenParams.groupModeratorProb) {
			int numGroup = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_FORUM).nextInt(DatagenParams.maxNumGroupCreatedPerUser)+1;
			for (int j = 0; j < numGroup; j++) {
				Forum group = forumGenerator_.createGroup(randomFarm_, forumId++, person, block);
				export(group);

				for( ForumMembership fm : group.memberships()) {
					export(fm);
				}

				// generate uniform posts/comments
				ArrayList<Post> groupPosts = uniformPostGenerator_.createPosts(randomFarm_, group, group.memberships(), numPostsPerGroup(randomFarm_, group, DatagenParams.maxNumGroupPostPerMonth, DatagenParams.maxNumMemberGroup), messageId);
				long aux = messageId+groupPosts.size();
				groupPosts.addAll(flashmobPostGenerator_.createPosts(randomFarm_, group, group.memberships(), numPostsPerGroup(randomFarm_, group, DatagenParams.maxNumGroupFlashmobPostPerMonth, DatagenParams.maxNumMemberGroup),aux));
				messageId += groupPosts.size();
				for( Post p : groupPosts ) {
					export(p);
					// generate likes to post
					if( randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
						ArrayList<Like> postLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), group, p, Like.LikeType.POST);
						for( Like l : postLikes ) {
							export(l);
						}
					}
					// generate comments
					int numComments = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_COMMENT).nextInt(DatagenParams.maxNumComments+1);
					ArrayList<Comment> comments = commentGenerator_.createComments(randomFarm_, group, p, numComments, messageId);
					messageId+=comments.size();
					for( Comment c : comments ) {
						export(c);
						// generate likes to comments
						if( c.content().length() > 10 && randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
							ArrayList<Like> commentLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), group, c, Like.LikeType.COMMENT);
							for( Like l : commentLikes ) {
								export(l);
							}
						}
					}
				}
			}
		}

	}

	private void generateAlbums(Person person, ArrayList<Person> block ) {
		// generate albums
		int numOfmonths = (int) Dictionaries.dates.numberOfMonths(person);
		int numPhotoAlbums = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_PHOTO_ALBUM).nextInt(DatagenParams.maxNumPhotoAlbumsPerMonth+1);
		if (numOfmonths != 0) {
			numPhotoAlbums = numOfmonths * numPhotoAlbums;
		}
		for (int i = 0; i < numPhotoAlbums; i++) {
			Forum album = forumGenerator_.createAlbum(randomFarm_, forumId++, person, i);
			export(album);

			for( ForumMembership fm : album.memberships()) {
				export(fm);
			}

			ForumMembership personMembership = new ForumMembership(album.id(),
				album.creationDate()+DatagenParams.deltaTime,  new Person.PersonSummary(person)
			);
			ArrayList<ForumMembership> fakeMembers = new ArrayList<ForumMembership>();
			fakeMembers.add(personMembership);
			int numPhotos = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_PHOTO).nextInt(DatagenParams.maxNumPhotoPerAlbums+1);
			ArrayList<Photo> photos = photoGenerator_.createPhotos(randomFarm_, album, fakeMembers, numPhotos, messageId);
			messageId+=photos.size();
			for( Photo p : photos ) {
				export(p);
				// generate likes
				if( randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
					ArrayList<Like> photoLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), album, p, Like.LikeType.PHOTO);
					for( Like l : photoLikes ) {
						export(l);
					}
				}
			}
		}
	}
	
	private int numPostsPerGroup( RandomGeneratorFarm randomFarm, Forum forum, int maxPostsPerMonth, int maxMembersPerForum ) {
		Random random = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST);
		int numOfmonths = (int) Dictionaries.dates.numberOfMonths(forum.creationDate());
		int numberPost = 0;
		if (numOfmonths == 0) {
			numberPost = random.nextInt(maxPostsPerMonth+1);
		} else {
			numberPost = random.nextInt(maxPostsPerMonth * numOfmonths+1);
		}
		return (numberPost * forum.memberships().size()) / maxMembersPerForum;
	}
	
	public void generateActivityForBlock( int seed, ArrayList<Person> block, Context context ) {
		randomFarm_.resetRandomGenerators(seed);
		forumId = 0;
		messageId = 0;
		SN.machineId = seed;
		personActivitySerializer_.reset();
		int counter = 0;
		for( Person p : block ) {
            System.out.println("Generating activity for peron"+counter);
			long start = System.currentTimeMillis();
			generateActivity(p, block);
			System.out.println("Time to generate activity for person "+counter+":"+(System.currentTimeMillis() - start)/1000.0f);
			if( DatagenParams.updateStreams ) {
				updateSerializer_.changePartition();
			}
			if( counter % 100 == 0 ) {
				context.setStatus("Generating activity of person "+counter+" of block"+seed);
				context.progress();
			}
			counter++;
		}
	}

	private void export(Forum forum) {
		if(forum.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
			personActivitySerializer_.export(forum);
		} else {
			updateSerializer_.export(forum);
		}
	}

	private void export(Post post) {
		if(post.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
			personActivitySerializer_.export(post);
            factorTable_.extractFactors(post);
		} else {
			updateSerializer_.export(post);
		}
	}

	private void export(Comment comment) {
		if(comment.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
			personActivitySerializer_.export(comment);
            factorTable_.extractFactors(comment);
		} else {
			updateSerializer_.export(comment);
		}
	}

	private void export(Photo photo) {
		if(photo.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
			personActivitySerializer_.export(photo);
            factorTable_.extractFactors(photo);
		} else {
			updateSerializer_.export(photo);
		}
	}

	private void export(ForumMembership member) {
		if(member.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
			personActivitySerializer_.export(member);
            factorTable_.extractFactors(member);
		} else {
			updateSerializer_.export(member);
		}
	}

	private void export(Like like) {
		if(like.date < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams ) {
			personActivitySerializer_.export(like);
            factorTable_.extractFactors(like);
		} else {
			updateSerializer_.export(like);
		}
	}

    public void writeFactors( OutputStream writer) {
        factorTable_.write(writer);
    }
}
