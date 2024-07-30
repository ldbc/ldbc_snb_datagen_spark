
package ldbc.snb.datagen.generator.generators.postgenerators;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.generator.generators.CommentGenerator;
import ldbc.snb.datagen.generator.generators.LikeGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;
import ldbc.snb.datagen.util.Iterators;
import ldbc.snb.datagen.util.PersonBehavior;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.util.Streams;
import ldbc.snb.datagen.generator.vocabulary.SN;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.util.*;
import java.util.stream.Stream;


abstract public class PostGenerator {

    private TextGenerator generator;
    private CommentGenerator commentGenerator;
    private LikeGenerator likeGenerator;

    PostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        this.generator = generator;
        this.commentGenerator = commentGenerator;
        this.likeGenerator = likeGenerator;
    }

    public void initialize() {
        // Intentionally left empty
    }

    public Stream<Triplet<Post, Stream<Like>, Stream<Pair<Comment, Stream<Like>>>>> createPosts(RandomGeneratorFarm randomFarm, final Forum forum, final List<ForumMembership> memberships,
                                                                                          long numPostsInForum, Iterator<Long> idIterator, long blockId) {

        Properties properties = new Properties();
        properties.setProperty("type", "post");

        return memberships.stream().flatMap(member -> {
            // generate number of posts by this member
            double numPostsPerMember = numPostsInForum / (double) memberships.size();

            if (numPostsPerMember < 1.0) {
                double prob = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POST).nextDouble();
                if (prob < numPostsPerMember) numPostsPerMember = 1.0;
            } else {
                numPostsPerMember = Math.ceil(numPostsPerMember);
            }

            final int numPostsPerMemberInt = (int) numPostsPerMember;
            // 0 to 20
            int numComments = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_COMMENT).nextInt(DatagenParams.maxNumComments + 1);

            return Streams.stream(Iterators.forIterator(0, i -> i < numPostsPerMemberInt, i -> ++i, i -> {
                // create post core
                PostCore postCore = generatePostInfo(randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_POST), randomFarm.get(RandomGeneratorFarm.Aspect.TAG),
                        randomFarm.get(RandomGeneratorFarm.Aspect.DATE), forum, member, numComments);

                if (postCore == null)
                    return Iterators.ForIterator.CONTINUE();

                // create content, county, ip - sometimes randomise
                String content = this.generator.generateText(member.getPerson(), postCore.getTags(), properties);
                int country = member.getPerson().getCountry();
                IP ip = member.getPerson().getIpAddress();
                Random random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);
                if (PersonBehavior.changeUsualCountry(random, postCore.getCreationDate())) {
                    random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                    country = Dictionaries.places.getRandomCountryUniform(random);
                    random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                    ip = Dictionaries.ips.getIP(random, country);
                }

                Post post = new Post();

                // create post with above information and from post info
                post.initialize(SN.formId(SN.composeId(idIterator.next(), postCore.getCreationDate()), blockId),
                        postCore.getCreationDate(),
                        postCore.getDeletionDate(),
                        member.getPerson(),
                        forum.getId(),
                        content,
                        new ArrayList<>(postCore.getTags()),
                        country,
                        ip,
                        Dictionaries.browsers.getPostBrowserId(
                                randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_BROWSER),
                                randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER),
                                member.getPerson().getBrowserId()),
                        forum.getLanguage(),
                        postCore.isExplicitlyDeleted());

                Stream<Like> likeStream = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1
                    ? likeGenerator.generateLikes(
                        randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_LIKES),
                        randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE),
                        forum, post, Like.LikeType.POST)
                        : Stream.empty();


                Stream<Pair<Comment, Stream<Like>>> commentStream = commentGenerator.createComments(randomFarm, forum, post, numComments, idIterator, blockId);

                return Iterators.ForIterator.RETURN(new Triplet<>(post, likeStream, commentStream));
            }));
        });
    }
    protected abstract PostCore generatePostInfo(Random randomDeletePost, Random randomTag, Random randomDate, final Forum forum, final ForumMembership membership, int numComments);
}
