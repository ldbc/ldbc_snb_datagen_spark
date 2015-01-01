
package ldbc.snb.datagen.generator;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Random;
import ldbc.snb.datagen.objects.Comment;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;
import ldbc.snb.datagen.objects.Like;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.Photo;
import ldbc.snb.datagen.objects.Post;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

public class PersonActivityGenerator {
	
	private ForumGenerator forumGenerator_ = null;
	private RandomGeneratorFarm randomFarm_ = null;
	private UniformPostGenerator uniformPostGenerator_ = null;
	private FlashmobPostGenerator flashmobPostGenerator_ = null;
	private PhotoGenerator photoGenerator_ = null;
	private DateGenerator dateTimeGenerator_ = null;
	private CommentGenerator commentGenerator_ = null;
	private LikeGenerator likeGenerator_ = null;
	private PersonActivitySerializer personActivitySerializer_ = null;
	private long forumId = 0;
	private long messageId = 0;
	
	public PersonActivityGenerator( PersonActivitySerializer serializer ) {
		personActivitySerializer_ = serializer;
		randomFarm_ = new RandomGeneratorFarm();
		// load generators
		forumGenerator_ = new ForumGenerator();
		uniformPostGenerator_ = new UniformPostGenerator();
		flashmobPostGenerator_ = new FlashmobPostGenerator();
		photoGenerator_ = new PhotoGenerator();
		commentGenerator_ = new CommentGenerator();
		likeGenerator_ = new LikeGenerator();
		
		dateTimeGenerator_ = new DateGenerator( new GregorianCalendar(DatagenParams.startYear,
			DatagenParams.startMonth,
			DatagenParams.startDate),
			new GregorianCalendar(DatagenParams.endYear,
				DatagenParams.endMonth,
				DatagenParams.endDate),
			DatagenParams.alpha,
			DatagenParams.deltaTime);
	}
	
	private void generateActivity( Person person, ArrayList<Person> block ) {
		
		// generate wall
		Forum wall = forumGenerator_.createWall(randomFarm_, forumId++, person);
		personActivitySerializer_.export(wall);
		ForumMembership personMembership = new ForumMembership(wall.id(),
			wall.creationDate()+DatagenParams.deltaTime,  new Person.PersonSummary(person)
		);
		
		// generate wall posts
		ArrayList<ForumMembership> fakeMembers = new ArrayList<ForumMembership>();
		fakeMembers.add(personMembership);
		ArrayList<Post> wallPosts = uniformPostGenerator_.createPosts(randomFarm_, wall, fakeMembers , numPostsPerGroup(randomFarm_, wall, DatagenParams.maxNumPostPerMonth, DatagenParams.maxNumFriends), messageId);
		long aux = messageId + wallPosts.size();
		wallPosts.addAll(flashmobPostGenerator_.createPosts(randomFarm_, wall, fakeMembers, numPostsPerGroup(randomFarm_, wall, DatagenParams.maxNumFlashmobPostPerMonth, DatagenParams.maxNumFriends), aux ));
		messageId+=wallPosts.size();
		
		for( Post p : wallPosts ) {
			personActivitySerializer_.export(p);
			// generate likes to post
			ArrayList<Like> postLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), wall, p, Like.LikeType.POST);
			if( randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
				for( Like l : postLikes ) {
					personActivitySerializer_.export(l);
				}
			}
			// generate comments
			int numComments = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_COMMENT).nextInt(DatagenParams.maxNumComments+1);
			ArrayList<Comment> comments = commentGenerator_.createComments(randomFarm_, wall, p, numComments, messageId);
			messageId+=comments.size();
			for( Comment c : comments ) {
				personActivitySerializer_.export(c);
				// generate likes to comments
				if( c.content().length() > 10 && randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
					ArrayList<Like> commentLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), wall, c, Like.LikeType.COMMENT);
					
					for( Like l : commentLikes ) {
						personActivitySerializer_.export(l);
					}
					
				}
			}
		}
		
		// generate user created groups
		double moderatorProb = randomFarm_.get(RandomGeneratorFarm.Aspect.FORUM_MODERATOR).nextDouble();
		if (moderatorProb <= DatagenParams.groupModeratorProb) {
			int numGroup = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_FORUM).nextInt(DatagenParams.maxNumGroupCreatedPerUser)+1;
			for (int j = 0; j < numGroup; j++) {
				Forum group = forumGenerator_.createGroup(randomFarm_, forumId++, person, block);
				personActivitySerializer_.export(group);
				// generate uniform posts/comments
				ArrayList<Post> groupPosts = uniformPostGenerator_.createPosts(randomFarm_, group, group.memberships(), numPostsPerGroup(randomFarm_, group, DatagenParams.maxNumGroupPostPerMonth, DatagenParams.maxNumMemberGroup), messageId);
				aux = messageId+groupPosts.size();
				groupPosts.addAll(flashmobPostGenerator_.createPosts(randomFarm_, group, group.memberships(), numPostsPerGroup(randomFarm_, group, DatagenParams.maxNumGroupFlashmobPostPerMonth, DatagenParams.maxNumMemberGroup),aux));
				messageId += groupPosts.size();
				for( Post p : groupPosts ) {
					personActivitySerializer_.export(p);
					// generate likes to post
					if( randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
						ArrayList<Like> postLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), group, p, Like.LikeType.POST);
						for( Like l : postLikes ) {
							personActivitySerializer_.export(l);
						}
					}
					// generate comments
					int numComments = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_COMMENT).nextInt(DatagenParams.maxNumComments+1);
					ArrayList<Comment> comments = commentGenerator_.createComments(randomFarm_, group, p, numComments, messageId);
					messageId+=comments.size();
					for( Comment c : comments ) {
						personActivitySerializer_.export(c);
						// generate likes to comments
						if( c.content().length() > 10 && randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
							ArrayList<Like> commentLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), group, p, Like.LikeType.POST);
							
							for( Like l : commentLikes ) {
								personActivitySerializer_.export(l);
							}
						}
					}
				}
			}
		}
		// generate albums
		int numOfmonths = (int) dateTimeGenerator_.numberOfMonths(person);
		int numPhotoAlbums = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_PHOTO_ALBUM).nextInt(DatagenParams.maxNumPhotoAlbumsPerMonth+1);
		if (numOfmonths != 0) {
			numPhotoAlbums = numOfmonths * numPhotoAlbums;
		}
		for (int i = 0; i < numPhotoAlbums; i++) {
			Forum album = forumGenerator_.createAlbum(randomFarm_, forumId++, person, i);
			personActivitySerializer_.export(album);
			int numPhotos = randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_PHOTO).nextInt(DatagenParams.maxNumPhotoPerAlbums+1);
			ArrayList<Photo> photos = photoGenerator_.createPhotos(randomFarm_, album, fakeMembers, numPhotos, messageId);
			messageId+=photos.size();
			for( Photo p : photos ) {
				personActivitySerializer_.export(p);
				// generate likes
				if( randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1 ) {
					ArrayList<Like> photoLikes = likeGenerator_.generateLikes(randomFarm_.get(RandomGeneratorFarm.Aspect.NUM_LIKE), album, p, Like.LikeType.PHOTO);
					for( Like l : photoLikes ) {
						personActivitySerializer_.export(l);
					}
				}
			}
		}
		
	}
	
	private int numPostsPerGroup( RandomGeneratorFarm randomFarm, Forum forum, int maxPostsPerMonth, int maxMembersPerForum ) {
		Random random = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST);
		int numOfmonths = (int) dateTimeGenerator_.numberOfMonths(forum.creationDate());
		int numberPost = 0;
		if (numOfmonths == 0) {
			numberPost = random.nextInt(maxPostsPerMonth+1);
		} else {
			numberPost = random.nextInt(maxPostsPerMonth * numOfmonths+1);
		}
		return (numberPost * forum.memberships().size()) / maxMembersPerForum;
	}
	
	public void generateActivityForBlock( int seed, ArrayList<Person> block ) {
		randomFarm_.resetRandomGenerators(seed);
		forumId = 0;
		messageId = 0;
		SN.machineId = seed;
		for( Person p : block ) {
			generateActivity(p, block);
		}
	}
}
