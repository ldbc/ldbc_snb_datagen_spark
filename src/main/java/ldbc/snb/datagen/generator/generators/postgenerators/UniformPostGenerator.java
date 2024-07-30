package ldbc.snb.datagen.generator.generators.postgenerators;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.generator.generators.CommentGenerator;
import ldbc.snb.datagen.generator.generators.LikeGenerator;
import ldbc.snb.datagen.generator.generators.textgenerators.TextGenerator;

import java.util.Random;


public class UniformPostGenerator extends PostGenerator {

    public UniformPostGenerator(TextGenerator generator, CommentGenerator commentGenerator, LikeGenerator likeGenerator) {
        super(generator, commentGenerator, likeGenerator);
    }

    @Override
    protected PostCore generatePostInfo(Random randomDeletePost, Random randomTag, Random randomDate, final Forum forum, final ForumMembership membership, int numComments) {
        PostCore postCore = new PostCore();

        // add creation date
        long minCreationDate = membership.getCreationDate() + DatagenParams.delta;
        long maxCreationDate = Math.min(membership.getDeletionDate(),Dictionaries.dates.getSimulationEnd());
        if (maxCreationDate - minCreationDate < 0) {
            return null;
        }
        long postCreationDate = Dictionaries.dates.randomDate(randomDate, minCreationDate, maxCreationDate);
        postCore.setCreationDate(postCreationDate);

        // add deletion date
        long postDeletionDate;
        if (membership.getPerson().isMessageDeleter() && randomDeletePost.nextDouble() < DatagenParams.postMapping[numComments]) {

            postCore.setExplicitlyDeleted(true);
            long minDeletionDate = postCreationDate + DatagenParams.delta;
            long maxDeletionDate = Math.min(membership.getDeletionDate(), Dictionaries.dates.getSimulationEnd());

            if (maxDeletionDate - minDeletionDate < 0) {
                return null;
            }
            postDeletionDate = Dictionaries.dates.powerLawDeleteDate(randomDate, minDeletionDate, maxDeletionDate);
        } else {
            postCore.setExplicitlyDeleted(false);
            postDeletionDate = Math.min(membership.getDeletionDate(), Dictionaries.dates.getSimulationEnd());
        }

        postCore.setDeletionDate(postDeletionDate);

        // add tags to post
        for (Integer value : forum.getTags()) {
            if (postCore.getTags().isEmpty()) {
                postCore.getTags().add(value);
            } else {
                if (randomTag.nextDouble() < 0.05) {
                    postCore.getTags().add(value);
                }
            }
        }
        return postCore;
    }
}
