package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.*;
import ldbc.snb.datagen.objects.ReducedUserProfile;
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

    private long composeUserId(long id, long date, long spid) {
        long spidMask = ~(0xFFFFFFFFFFFFFFFFL << 7);
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 33);
        long bucket = (long) (256 * (date - SN.minDate) / (double) SN.maxDate);
        return (bucket << 40) | ((id & idMask) << 7) | (spid & spidMask);
    }

    private boolean isUserALargePoster(ReducedUserProfile user) {
        if (dateTimeGenerator.getBirthMonth(user.getBirthDay()) == GregorianCalendar.JANUARY) {
            return true;
        }
        return false;
    }

    private ReducedUserProfile generateUser() {

        long creationDate = dateTimeGenerator.randomDateInMillis(randomFarm.get(RandomGeneratorFarm.Aspect.DATE));
        int countryId = placeDictionary.getCountryForUser(randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY));
        ReducedUserProfile userProf = new ReducedUserProfile();
        userProf.setCreationDate(creationDate);
        userProf.setGender((randomFarm.get(RandomGeneratorFarm.Aspect.GENDER).nextDouble() > 0.5) ? (byte) 1 : (byte) 0);
        userProf.setBirthDay(dateTimeGenerator.getBirthDay(randomFarm.get(RandomGeneratorFarm.Aspect.BIRTH_DAY), creationDate));
        userProf.setBrowserId(browserDictionary.getRandomBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER)));
        userProf.setCountryId(countryId);
        userProf.setCityId(placeDictionary.getRandomCity(randomFarm.get(RandomGeneratorFarm.Aspect.CITY), countryId));
        userProf.setIpAddress(ipAddressDictionary.getRandomIPFromLocation(randomFarm.get(RandomGeneratorFarm.Aspect.IP), countryId));
        userProf.setMaxNumFriends(fbDegreeGenerator.getSocialDegree());
        userProf.setAccountId(composeUserId(nextId++, creationDate, fbDegreeGenerator.getIDByPercentile()));

        // Setting tags
        int userMainTag = tagDictionary.getaTagByCountry(randomFarm.get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), randomFarm.get(RandomGeneratorFarm.Aspect.TAG), userProf.getCountryId());
        userProf.setMainTag(userMainTag);
        short numTags = ((short) randomTagPowerLaw.getValue(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_TAG)));
        userProf.setInterests(tagMatrix.getSetofTags(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomFarm.get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), userMainTag, numTags));


        userProf.setUniversityLocationId(universityDictionary.getRandomUniversity(randomFarm, userProf.getCountryId()));

        // Set whether the user has a smartphone or not.
        userProf.setHaveSmartPhone(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT).nextDouble() > DatagenParams.probHavingSmartPhone);
        if (userProf.isHaveSmartPhone()) {
            userProf.setAgentId(userAgentDictionary.getRandomUserAgentIdx(randomFarm.get(RandomGeneratorFarm.Aspect.USER_AGENT)));
        }

        // Compute the popular places the user uses to visit.
        byte numPopularPlaces = (byte) randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POPULAR).nextInt(DatagenParams.maxNumPopularPlaces + 1);
        ArrayList<Short> auxPopularPlaces = new ArrayList<Short>();
        for (int i = 0; i < numPopularPlaces; i++) {
            short aux = popularDictionary.getPopularPlace(randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR), userProf.getCountryId());
            if (aux != -1) {
                auxPopularPlaces.add(aux);
            }
        }

        // Setting popular places
        short popularPlaces[] = new short[auxPopularPlaces.size()];
        Iterator<Short> it = auxPopularPlaces.iterator();
        int i = 0;
        while (it.hasNext()) {
            popularPlaces[i] = it.next();
            ++i;
        }
        userProf.setPopularPlaceIds(popularPlaces);

        // Set whether the user is a large poster or not.
        userProf.setLargePoster(isUserALargePoster(userProf));
        return userProf;
    }

    private void resetState(int blockId){
        fbDegreeGenerator.resetState(blockId);
        randomFarm.resetRandomGenerators((long) blockId);
    }

    public ReducedUserProfile[] generateUserBlock( int blockId ) {
        resetState(blockId);
        ReducedUserProfile[] block = null;
        if( this.nextId + DatagenParams.blockSize < DatagenParams.numPersons ) {
            block = new ReducedUserProfile[DatagenParams.blockSize];
        } else {
            block = new ReducedUserProfile[DatagenParams.numPersons - this.nextId];
        }
        int size = block.length;
        for (int j =0; j < size; ++j) {
            block[j] = generateUser();
        }
        return block;
    }
}
