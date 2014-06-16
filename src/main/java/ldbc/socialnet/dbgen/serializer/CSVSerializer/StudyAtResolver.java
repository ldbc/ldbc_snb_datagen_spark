package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.StudyAt;

import java.util.ArrayList;
import java.util.GregorianCalendar;

/**
 * Created by aprat on 4/15/14.
 */
public class StudyAtResolver implements EntityFieldResolver<StudyAt> {

    GregorianCalendar date;
    public StudyAtResolver() {
        date = new GregorianCalendar();
    }

    @Override
    public ArrayList<String> queryField(String string, StudyAt studyAt) {
        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("user") ) {
            ret.add(Long.toString(studyAt.user));
            return ret;
        }

        if( string.equals("company") ) {
            ret.add(Long.toString(studyAt.university));
            return ret;
        }

        if( string.equals("year") ) {
            date.setTimeInMillis(studyAt.year);
            ret.add(DateGenerator.formatYear(date));
            return ret;
        }
        return null;
    }
}
