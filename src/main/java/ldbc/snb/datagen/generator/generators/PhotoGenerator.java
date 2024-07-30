package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.util.Iterators;
import ldbc.snb.datagen.util.PersonBehavior;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.util.Streams;
import ldbc.snb.datagen.generator.vocabulary.SN;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Stream;

/**
 * This class generates photos which are used in posts.
 */
class PhotoGenerator {

    private LikeGenerator likeGenerator;
    private Photo photo;

    PhotoGenerator(LikeGenerator likeGenerator) {
        this.likeGenerator = likeGenerator;
        this.photo = new Photo();
    }

    Stream<Pair<Photo, Stream<Like>>> createPhotos(RandomGeneratorFarm randomFarm, final Forum album, long numPhotosInAlbum, Iterator<Long> idIterator, long blockId) {
        int numPopularPlaces = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POPULAR)
                .nextInt(DatagenParams.maxNumPopularPlaces + 1);
        List<Short> popularPlaces = new ArrayList<>();
        for (int i = 0; i < numPopularPlaces; i++) {
            short aux = Dictionaries.popularPlaces.getPopularPlace(randomFarm
                    .get(RandomGeneratorFarm.Aspect.POPULAR), album
                    .getPlaceId());
            if (aux != -1) {
                popularPlaces.add(aux);
            }
        }

        return Streams.stream(Iterators.forIterator(0, i -> i < numPhotosInAlbum, i -> ++i, i -> {
            TreeSet<Integer> tags = new TreeSet<>();

            // creates photo
            long creationDate = album.getCreationDate() + DatagenParams.delta + 1000 * (i + 1);
            if (creationDate >= album.getDeletionDate()) {
                return Iterators.ForIterator.BREAK();
            }

            Random randomDate = randomFarm.get(RandomGeneratorFarm.Aspect.DATE);
            Random randomDeletePost = randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_POST);

            long deletionDate;
            boolean isExplicitlyDeleted;
            if (album.getModerator().isMessageDeleter() && randomDeletePost.nextDouble() < DatagenParams.probPhotoDeleted) {
                isExplicitlyDeleted = true;
                long minDeletionDate = creationDate + DatagenParams.delta;
                long maxDeletionDate = Math.min(album.getDeletionDate(), Dictionaries.dates.getSimulationEnd());
                deletionDate = Dictionaries.dates.powerLawDeleteDate(randomDate, minDeletionDate, maxDeletionDate);
            } else {
                isExplicitlyDeleted = false;
                deletionDate = album.getDeletionDate();
            }

            int country = album.getModerator().getCountry();
            IP ip = album.getModerator().getIpAddress();
            Random random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);


            if (PersonBehavior.changeUsualCountry(random, creationDate)) {
                random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                country = Dictionaries.places.getRandomCountryUniform(random);
                random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                ip = Dictionaries.ips.getIP(random, country);
            }

            long id = SN.formId(SN.composeId(idIterator.next(), creationDate), blockId);
            photo.initialize(id,
                    creationDate,
                    deletionDate,
                    album.getModerator(),
                    album.getId(),
                    "photo" + id + ".jpg",
                    new ArrayList<>(tags),
                    country,
                    ip,
                    album.getModerator().getBrowserId(),
                    isExplicitlyDeleted
            );

            Stream<Like> likeStream = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1
                    ? likeGenerator.generateLikes(
                            randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_LIKES),
                            randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE),
                            album, photo, Like.LikeType.PHOTO)
                    : Stream.empty();

            return Iterators.ForIterator.RETURN(new Pair<>(photo, likeStream)); // (photo, likeStream)
        }));
    }

}
