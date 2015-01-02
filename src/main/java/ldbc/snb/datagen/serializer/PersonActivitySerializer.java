/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.dictionary.CompanyDictionary;
import ldbc.snb.datagen.dictionary.PlaceDictionary;
import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.dictionary.UniversityDictionary;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.Comment;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;
import ldbc.snb.datagen.objects.Like;
import ldbc.snb.datagen.objects.Photo;
import ldbc.snb.datagen.objects.Post;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author aprat
 */
abstract public class PersonActivitySerializer {

protected UniversityDictionary universityDictionary_;
protected PlaceDictionary placeDictionary_;
protected CompanyDictionary companyDictionary_;
protected TagDictionary tagDictionary_;

public PersonActivitySerializer() {
	placeDictionary_ = new PlaceDictionary(DatagenParams.numPersons);

        companyDictionary_ = new CompanyDictionary(placeDictionary_, DatagenParams.probUnCorrelatedCompany);

        universityDictionary_ = new UniversityDictionary(placeDictionary_,
                DatagenParams.probUnCorrelatedOrganization,
                DatagenParams.probTopUniv,
                companyDictionary_.getNumCompanies());

        tagDictionary_ = new TagDictionary(  placeDictionary_.getCountries().size(),
                DatagenParams.tagCountryCorrProb);
}

public void export( Forum forum ) {
	serialize(forum);
	for(ForumMembership fm : forum.memberships()) {
		serialize(fm);
	}
}

public void export( Post post ) {
		serialize(post);
}

public void export( Comment comment ) {
	serialize(comment);

}

public void export( Photo photo ) {
	serialize(photo);

}
	
public void export( Like like ) {
	serialize(like);

}


abstract public void initialize(Configuration conf, int reducerId);

abstract public void close();

abstract protected void serialize( Forum forum );

abstract protected void serialize( Post post );

abstract protected void serialize( Comment comment );

abstract protected void serialize( Photo photo );
	
abstract protected void serialize( ForumMembership membership );

abstract protected void serialize( Like like );


}
