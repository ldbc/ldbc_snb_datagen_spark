package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.LanguageDictionary;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.vocabulary.SN;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;

/**
 * Created by aprat on 4/14/14.
 */
public class PostResolver implements EntityFieldResolver<Post> {

    private IPAddressDictionary ipDic;
    private LanguageDictionary languageDic;
    private BrowserDictionary browserDic;
    GregorianCalendar date;
    public PostResolver(BrowserDictionary browserDic, LanguageDictionary languageDic, IPAddressDictionary ipDic) {
        date = new GregorianCalendar();
        this.browserDic = browserDic;
        this.languageDic = languageDic;
        this.ipDic = ipDic;
    }
    @Override
    public ArrayList<String> queryField(String string, Post post) {
        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id") ) {
            ret.add(SN.formId(post.getMessageId()));
            return ret;
        }

        if( string.equals("image") ) {
            ret.add(new String(""));
            return ret;
        }

        if( string.equals("creationDate") ) {
            date.setTimeInMillis(post.getCreationDate());
            ret.add(DateGenerator.formatDateDetail(date));
            return ret;
        }

        if( string.equals("ip") ) {
            if (post.getIpAddress() != null) {
                ret.add(post.getIpAddress().toString());
            } else {
                ret.add(new String(""));
            }
            return ret;
        }

        if( string.equals("browser") ) {
            if (post.getBrowserIdx() != -1){
                ret.add(browserDic.getName(post.getBrowserIdx()));
            } else {
                ret.add(new String(""));
            }
            return ret;
        }

        if( string.equals("language") ) {
            ret.add(languageDic.getLanguagesName(post.getLanguage()));
            return ret;
        }

        if( string.equals("content") ) {
            ret.add(post.getContent());
            return ret;
        }

        if( string.equals("length") ) {
            ret.add(Integer.toString(post.getTextSize()));
            return ret;
        }

        if( string.equals("creatorId") ) {
            ret.add(Long.toString(post.getAuthorId()));
            return ret;
        }


        if( string.equals("forumId") ) {
            ret.add(Long.toString(post.getGroupId()));
            return ret;
        }

        if( string.equals("place") ) {
            ret.add(Integer.toString(ipDic.getLocation(post.getIpAddress())));
            return ret;
        }

        if( string.equals("tag") ) {
            Iterator<Integer> it = post.getTags().iterator();
            while (it.hasNext()) {
                Integer tagId = it.next();
                ret.add(Integer.toString(tagId));
            }
            return ret;
        }

        return null;
    }
}
