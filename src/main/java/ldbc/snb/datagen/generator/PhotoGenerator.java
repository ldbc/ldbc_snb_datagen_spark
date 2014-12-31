/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package ldbc.snb.datagen.generator;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.TreeSet;
import ldbc.snb.datagen.dictionary.BrowserDictionary;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.dictionary.IPAddressDictionary;
import ldbc.snb.datagen.dictionary.PlaceDictionary;
import ldbc.snb.datagen.dictionary.PopularPlacesDictionary;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.dictionary.TagTextDictionary;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;
import ldbc.snb.datagen.objects.Photo;
import ldbc.snb.datagen.objects.PopularPlace;
import ldbc.snb.datagen.objects.Post;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

/**
 *
 * @author aprat
 */
public class PhotoGenerator {
	private long postId = 0;
	private static final String SEPARATOR = "  ";
	
	public PhotoGenerator() {
	}
	public ArrayList<Photo> createPhotos(RandomGeneratorFarm randomFarm, Forum album, ArrayList<ForumMembership> memberships, long numPhotos, long startId ){
		long nextId = startId;
		ArrayList<Photo> photos = new ArrayList<Photo>();
		int numPopularPlaces = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POPULAR).nextInt(DatagenParams.maxNumPopularPlaces + 1);
		ArrayList<Short> popularPlaces = new ArrayList<Short>();
		for (int i = 0; i < numPopularPlaces; i++){
			short aux = Dictionaries.popularPlaces.getPopularPlace(randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR),album.place());
			if(aux != -1) {
				popularPlaces.add(aux);
			}
		}
		for( int i = 0; i< numPhotos; ++i ) {
			int locationId = album.place();
			double latt = 0;
			double longt = 0;
			String locationName = "";
			if (popularPlaces.size() == 0){
				locationName = Dictionaries.places.getPlaceName(locationId);
				latt = Dictionaries.places.getLatt(locationId);
				longt = Dictionaries.places.getLongt(locationId);
			} else{
				int popularPlaceId;
				PopularPlace popularPlace;
				if (randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR).nextDouble() < DatagenParams.probPopularPlaces){
					//Generate photo information from user's popular place
					int popularIndex = randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR).nextInt(popularPlaces.size());
					popularPlaceId = popularPlaces.get(popularIndex);
					popularPlace = Dictionaries.popularPlaces.getPopularPlace(album.place(),popularPlaceId);
					locationName = popularPlace.getName();
					latt = popularPlace.getLatt();
					longt = popularPlace.getLongt();
				} else{
					// Randomly select one places from Album location idx
					popularPlaceId = Dictionaries.popularPlaces.getPopularPlace(randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR),locationId);
					if (popularPlaceId != -1){
						popularPlace = Dictionaries.popularPlaces.getPopularPlace(locationId, popularPlaceId);
						locationName = popularPlace.getName();
						latt = popularPlace.getLatt();
						longt = popularPlace.getLongt();
					} else{
						locationName = Dictionaries.places.getPlaceName(locationId);
						latt = Dictionaries.places.getLatt(locationId);
						longt = Dictionaries.places.getLongt(locationId);
					}
				}
			}
			TreeSet<Integer> tags = new TreeSet<Integer>();
			long date = album.creationDate()+DatagenParams.deltaTime+1000*(i+1);
			if( date <= Dictionaries.dates.getEndDateTime() ) {
				long id = SN.formId(SN.composeId(nextId++,date));
				Photo photo = new Photo(id,date,album.moderator().accountId(), album.id(), "photo"+id+".jpg",tags,album.moderator().ipAddress(),album.moderator().browserId(),locationId,latt,longt);
				photos.add(photo);
			}
		}
		return photos;
	}
	
}
