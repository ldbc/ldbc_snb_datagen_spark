package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.dictionary.BrowserDictionary;
import ldbc.socialnet.dbgen.dictionary.IPAddressDictionary;
import ldbc.socialnet.dbgen.dictionary.LanguageDictionary;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.vocabulary.SN;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;

/**
 * Created by aprat on 4/14/14.
 */
public class CommentResolver implements EntityFieldResolver<Comment> {

    private IPAddressDictionary ipDic;
    private LanguageDictionary languageDic;
    private BrowserDictionary browserDic;
    GregorianCalendar date;
    public CommentResolver(BrowserDictionary browserDic, LanguageDictionary languageDic, IPAddressDictionary ipDic) {
        date = new GregorianCalendar();
        this.browserDic = browserDic;
        this.languageDic = languageDic;
        this.ipDic = ipDic;
    }

    @Override
    public ArrayList<String> queryField(String string, Comment comment) {

        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id") ) {
            ret.add(SN.formId(comment.getMessageId()));
            return ret;
        }

        if( string.equals("image") ) {
            ret.add(new String(""));
            return ret;
        }

        if( string.equals("creationDate") ) {
            date.setTimeInMillis(comment.getCreationDate());
            ret.add(DateGenerator.formatDateDetail(date));
            return ret;
        }

        if( string.equals("ip") ) {
            if (comment.getIpAddress() != null) {
                ret.add(comment.getIpAddress().toString());
            } else {
                ret.add(new String(""));
            }
            return ret;
        }

        if( string.equals("browser") ) {
            if (comment.getBrowserIdx() != -1){
                ret.add(browserDic.getName(comment.getBrowserIdx()));
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
            ret.add(comment.getContent());
            return ret;
        }

        if( string.equals("length") ) {
            ret.add(Integer.toString(comment.getTextSize()));
            return ret;
        }

        if( string.equals("creatorId") ) {
            ret.add(Long.toString(comment.getAuthorId()));
            return ret;
        }

        if( string.equals("replyOfPost") ) {
            if(comment.getReplyOf() == comment.getPostId() ) {
                ret.add(Long.toString(comment.getReplyOf()));
            } else {
                ret.add(new String(""));
            }
            return ret;
        }

        if( string.equals("replyOfComment") ) {
            if(comment.getReplyOf() != comment.getPostId() ) {
                ret.add(Long.toString(comment.getReplyOf()));
            } else {
                ret.add(new String(""));
            }
            return ret;
        }

        if( string.equals("forumId") ) {
            ret.add(Long.toString(comment.getGroupId()));
            return ret;
        }

        if( string.equals("place") ) {
            ret.add(Integer.toString(ipDic.getLocation(comment.getIpAddress())));
            return ret;
        }

        if( string.equals("tag") ) {
            Iterator<Integer> it = comment.getTags().iterator();
            while (it.hasNext()) {
                Integer tagId = it.next();
                ret.add(Integer.toString(tagId));
            }
            return ret;
        }

        return null;
    }
}
