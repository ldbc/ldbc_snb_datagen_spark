package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.*;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;

/**
 * Created by aprat on 10/7/14.
 */
public class PersonGenerator {

    private BrowserDictionary browserDictionary         = null;
    private CompanyDictionary companiesDictionary     = null;
    private DateGenerator dateTimeGenerator             = null;
    private FBSocialDegreeGenerator fbDegreeGenerator   = null;
    private IPAddressDictionary ipAddressDictionary     = null;
    private PlaceDictionary placeDictionary             = null;
    private PopularPlacesDictionary popularDictionary   = null;
    private PowerDistGenerator randomTagPowerLaw        = null;
    private RandomGeneratorFarm randomFarm              = null;
    private TagDictionary tagDictionary                 = null;
    private TagMatrix tagMatrix                         = null;
    private UniversityDictionary universityDictionary   = null;
    private UserAgentDictionary userAgentDictionary     = null;
    private int nextId                                  = 0;

    public PersonGenerator() {
        browserDictionary = new BrowserDictionary(DatagenParams.probAnotherBrowser);
        browserDictionary.load(DatagenParams.browserDictonryFile);

        dateTimeGenerator = new DateGenerator( new GregorianCalendar(DatagenParams.startYear,
                                                                     DatagenParams.startMonth,
                                                                     DatagenParams.startDate),
                                               new GregorianCalendar(DatagenParams.endYear,
                                                                     DatagenParams.endMonth,
                                                                     DatagenParams.endDate),
                                               DatagenParams.alpha,
                                               DatagenParams.deltaTime);

        fbDegreeGenerator = new FBSocialDegreeGenerator(DatagenParams.numPersons,
                                                        DatagenParams.fbSocialDegreeFile,
                                                        0);

        placeDictionary = new PlaceDictionary(DatagenParams.numPersons);
        placeDictionary.load(DatagenParams.cityDictionaryFile, DatagenParams.countryDictionaryFile);

        ipAddressDictionary = new IPAddressDictionary(  placeDictionary,
                                                        DatagenParams.probDiffIPinTravelSeason,
                                                        DatagenParams.probDiffIPnotTravelSeason,
                                                        DatagenParams.probDiffIPforTraveller);
        ipAddressDictionary.load(DatagenParams.countryAbbrMappingFile,
                                 DatagenParams.IPZONE_DIRECTORY);

        popularDictionary = new PopularPlacesDictionary(placeDictionary);
        popularDictionary.load(DatagenParams.popularDictionaryFile);

        randomTagPowerLaw = new PowerDistGenerator( DatagenParams.minNumTagsPerUser,
                                                    DatagenParams.maxNumTagsPerUser + 1,
                                                    DatagenParams.alpha);

        randomFarm = new RandomGeneratorFarm();

        tagDictionary = new TagDictionary(  placeDictionary.getCountries().size(),
                                            DatagenParams.tagCountryCorrProb);
        tagDictionary.load( DatagenParams.tagsFile,
                            DatagenParams.popularTagByCountryFile,
                            DatagenParams.tagClassFile,
                            DatagenParams.tagClassHierarchyFile);

        tagMatrix = new TagMatrix(tagDictionary.getNumPopularTags());
        tagMatrix.load(DatagenParams.tagMatrixFile);

        companiesDictionary = new CompanyDictionary(placeDictionary, DatagenParams.probUnCorrelatedCompany);
        companiesDictionary.load(DatagenParams.companiesDictionaryFile);

        universityDictionary = new UniversityDictionary(placeDictionary,
                                                        DatagenParams.probUnCorrelatedOrganization,
                                                        DatagenParams.probTopUniv,
                                                        companiesDictionary.getNumCompanies());
        universityDictionary.load(DatagenParams.universityDictionaryFile);

        userAgentDictionary = new UserAgentDictionary(DatagenParams.probSentFromAgent);
        userAgentDictionary.load(DatagenParams.agentFile);
    }

    /** Composes a user id from its sequential id, its creation date and the percentile.
     *
     * @param id    The sequential id.
     * @param date  The date the person was created.
     * @param spid  The percentile id
     * @return A new composed id.
     */
    private long composeUserId(long id, long date, long spid) {
        long spidMask = ~(0xFFFFFFFFFFFFFFFFL << 7);
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 33);
        long bucket = (long) (256 * (date - SN.minDate) / (double) SN.maxDate);
        return (bucket << 40) | ((id & idMask) << 7) | (spid & spidMask);
    }

    /** Tells if a person is a large poster or not.
     *
     * @param user The person to check.
     * @return True if the person is a large poster. False otherwise.
     */
    private boolean isUserALargePoster(Person user) {
        if (dateTimeGenerator.getBirthMonth(user.birthDay) == GregorianCalendar.JANUARY) {
            return true;
        }
        return false;
    }

    private Person generateUser() {

        long creationDate = dateTimeGenerator.randomDateInMillis(randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        int countryId = placeDictionary.getCountryForUser(randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY));
        Person person = new Person();
        person.creationDate = creationDate;
        person.gender = ((randomFarm.get(RandomGeneratorFarm.Aspect.GENDER).nextDouble() > 0.5) ? (byte) 1 : (byte) 0);
        person.birthDay = (dateTimeGenerator.getBirthDay(randomFarm.get(RandomGeneratorFarm.Aspect.BIRTH_DAY), creationDate));
        person.browserId = (browserDictionary.getRandomBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER)));
        person.countryId = countryId;
        person.cityId = (placeDictionary.getRandomCity(randomFarm.get(RandomGeneratorFarm.Aspect.CITY), countryId));
        person.ipAddress = (ipAddressDictionary.getRandomIPFromLocation(randomFarm.get(RandomGeneratorFarm.Aspect.IP), countryId));
        person.maxNumFriends = (fbDegreeGenerator.getSocialDegree());
        person.accountId = (composeUserId(nextId++, creationDate, fbDegreeGenerator.getIDByPercentile()));
        person.mainInterest = tagDictionary.getaTagByCountry(randomFarm.get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), randomFarm.get(RandomGeneratorFarm.Aspect.TAG), person.countryId);
        short numTags = ((short) randomTagPowerLaw.getValue(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_TAG)));
        person.interests = tagMatrix.getSetofTags(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomFarm.get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), person.mainInterest, numTags);
        person.universityLocationId = universityDictionary.getRandomUniversity(randomFarm, person.countryId);

        // Compute the popular places the user uses to visit.
        byte numPopularPlaces = (byte) randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POPULAR).nextInt(DatagenParams.maxNumPopularPlaces + 1);
        ArrayList<Short> auxPopularPlaces = new ArrayList<Short>();
        for (int i = 0; i < numPopularPlaces; i++) {
            short aux = popularDictionary.getPopularPlace(randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR), person.countryId);
            if (aux != -1) {
                auxPopularPlaces.add(aux);
            }
        }
        return person;
    }

    private void resetState(int blockId){
        fbDegreeGenerator.resetState(blockId);
        randomFarm.resetRandomGenerators((long) blockId);
    }

    public Person[] generateUserBlock( int blockId ) {
        resetState(blockId);
        Person[] block;
        if( this.nextId + DatagenParams.blockSize < DatagenParams.numPersons ) {
            block = new Person[DatagenParams.blockSize];
        } else {
            block = new Person[DatagenParams.numPersons - this.nextId];
        }
        int size = block.length;
        for (int j =0; j < size; ++j) {
            block[j] = generateUser();
        }
        return block;
    }
}
