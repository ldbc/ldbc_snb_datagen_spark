package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.LanguageDictionary;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.Photo;
import ldbc.socialnet.dbgen.vocabulary.SN;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;

/**
 * Created by aprat on 4/14/14.
 */
public class PhotoResolver implements EntityFieldResolver<Photo> {

    private IPAddressDictionary ipDic;
    private LanguageDictionary languageDic;
    private BrowserDictionary browserDic;
    GregorianCalendar date;
    public PhotoResolver(BrowserDictionary browserDic, LanguageDictionary languageDic, IPAddressDictionary ipDic) {
        date = new GregorianCalendar();
        this.browserDic = browserDic;
        this.languageDic = languageDic;
        this.ipDic = ipDic;
    }

    @Override
    public ArrayList<String> queryField(String string, Photo photo) {

        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id") ) {
            ret.add(SN.formId(photo.getMessageId()));
            return ret;
        }

        if( string.equals("image") ) {
            ret.add(photo.getContent());
            return ret;
        }

        if( string.equals("creationDate") ) {
            date.setTimeInMillis(photo.getCreationDate());
            ret.add(DateGenerator.formatDateDetail(date));
            return ret;
        }

        if( string.equals("ip") ) {
            if (photo.getIpAddress() != null) {
                ret.add(photo.getIpAddress().toString());
            } else {
                ret.add(new String(""));
            }
            return ret;
        }

        if( string.equals("browser") ) {
            if (photo.getBrowserIdx() != -1){
                ret.add(browserDic.getName(photo.getBrowserIdx()));
            } else {
                ret.add(new String(""));
            }
            return ret;
        }

        if( string.equals("language") ) {
            ret.add(new String(""));
            return ret;
        }

        if( string.equals("content") ) {
            ret.add(new String(""));
            return ret;
        }

        if( string.equals("length") ) {
            ret.add(Integer.toString(photo.getTextSize()));
            return ret;
        }

        if( string.equals("creatorId") ) {
            ret.add(Long.toString(photo.getAuthorId()));
            return ret;
        }

        if( string.equals("forumId") ) {
            ret.add(Long.toString(photo.getGroupId()));
            return ret;
        }

        if( string.equals("place") ) {
            ret.add(Integer.toString(ipDic.getLocation(photo.getIpAddress())));
            return ret;
        }

        if( string.equals("tag") ) {
            Iterator<Integer> it = photo.getTags().iterator();
            while (it.hasNext()) {
                Integer tagId = it.next();
                ret.add(Integer.toString(tagId));
            }
            return ret;
        }

        return null;
    }
}
