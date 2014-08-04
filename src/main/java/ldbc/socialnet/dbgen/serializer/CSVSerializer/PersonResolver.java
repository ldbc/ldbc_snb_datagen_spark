package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.LanguageDictionary;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.UserInfo;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Vector;

/**
 * Created by aprat on 4/14/14.
 */
public class PersonResolver  implements EntityFieldResolver<UserInfo> {

    private LanguageDictionary languageDic;
    private BrowserDictionary browserDic;
    GregorianCalendar date;
    public PersonResolver(BrowserDictionary browserDic, LanguageDictionary languageDic) {
        date = new GregorianCalendar();
        this.browserDic = browserDic;
        this.languageDic = languageDic;
    }

    @Override
    public ArrayList<String> queryField(String string, UserInfo userInfo) {

        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id") ) {
            ret.add(Long.toString(userInfo.user.getAccountId()));
            return ret;
        }

        if( string.equals("firstName") ) {
            ret.add(userInfo.extraInfo.getFirstName());
            return ret;
        }

        if( string.equals("lastName") ) {
            ret.add(userInfo.extraInfo.getLastName());
            return ret;
        }

        if( string.equals("gender") ) {
            ret.add(userInfo.extraInfo.getGender());
            return ret;
        }

        if( string.equals("birthDay") ) {
            if (userInfo.user.getBirthDay() != -1 ) {
                date.setTimeInMillis(userInfo.user.getBirthDay());
                ret.add(DateGenerator.formatDate(date));
            } else {
                String empty = "";
                ret.add(empty);
            }
            return ret;
        }

        if( string.equals("creationDate") ) {
            date.setTimeInMillis(userInfo.user.getCreationDate());
            ret.add(DateGenerator.formatDateDetail(date));
            return ret;
        }

        if( string.equals("ip") ) {
            if (userInfo.user.getIpAddress() != null) {
                ret.add(userInfo.user.getIpAddress().toString());
            } else {
                String empty = "";
                ret.add(empty);
            }
            return ret;
        }

        if( string.equals("browser") ) {
            if (userInfo.user.getBrowserId() >= 0) {
                ret.add(browserDic.getName(userInfo.user.getBrowserId()));
            } else {
                String empty = "";
                ret.add(empty);
            }
            return ret;
        }

        if( string.equals("locationId") ) {
            ret.add(Integer.toString(userInfo.extraInfo.getLocationId()));
            return ret;
        }

        if( string.equals("language") ) {
            Vector<Integer> languages = userInfo.extraInfo.getLanguages();
            for (int i = 0; i < languages.size(); i++) {
                ret.add(languageDic.getLanguagesName(languages.get(i)));
            }
            return ret;
        }

        if( string.equals("email") ) {
            Iterator<String> itString = userInfo.extraInfo.getEmail().iterator();
            while (itString.hasNext()){
                String email = itString.next();
                ret.add(email);
            }
            return ret;
        }


        if( string.equals("tag") ) {
            Iterator<Integer> itInteger = userInfo.user.getInterests().iterator();
            while (itInteger.hasNext()){
                Integer interestIdx = itInteger.next();
                ret.add(Integer.toString(interestIdx));
            }
            return ret;
        }

        return null;

    }
}
