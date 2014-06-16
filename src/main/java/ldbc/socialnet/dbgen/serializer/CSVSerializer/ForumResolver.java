package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.dictionary.TagDictionary;
import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.GroupMemberShip;
import ldbc.socialnet.dbgen.vocabulary.SN;

import java.util.ArrayList;
import java.util.GregorianCalendar;

/**
 * Created by aprat on 4/14/14.
 */
public class ForumResolver implements EntityFieldResolver<Group> {

    GregorianCalendar date;
    TagDictionary tagDic;
    public ForumResolver(TagDictionary tagDic) {
        date = new GregorianCalendar();
        this.tagDic = tagDic;
    }

    @Override
    public ArrayList<String> queryField(String string, Group group) {

        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("id") ) {
            ret.add(SN.formId(group.getGroupId()));
            return ret;
        }

        if( string.equals("name") ) {
            ret.add(group.getGroupName());
            return ret;
        }

        if( string.equals("creationDate") ) {
            date.setTimeInMillis(group.getCreatedDate());
            String dateString = DateGenerator.formatDateDetail(date);
            ret.add(dateString);
            return ret;
        }

        if( string.equals("moderator") ) {
            ret.add(Long.toString(group.getModeratorId()));
            return ret;
        }

        if( string.equals("tag") ) {
            Integer groupTags[] = group.getTags();
            for (int i = 0; i < groupTags.length; i ++) {
                String interest = tagDic.getName(groupTags[i]);
                ret.add(Integer.toString(groupTags[i]));
            }
            return ret;
        }
        return null;
    }
}
