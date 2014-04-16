package ldbc.socialnet.dbgen.serializer.CSVSerializer;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.WorkAt;

import java.util.ArrayList;
import java.util.GregorianCalendar;

/**
 * Created by aprat on 4/15/14.
 */
public class WorkAtResolver implements EntityFieldResolver<WorkAt> {

    GregorianCalendar date;
    public WorkAtResolver() {
        date = new GregorianCalendar();
    }

    @Override
    public ArrayList<String> queryField(String string, WorkAt workAt) {
        ArrayList<String> ret = new ArrayList<String>();
        if( string.equals("user") ) {
            ret.add(Long.toString(workAt.user));
            return ret;
        }

        if( string.equals("company") ) {
            ret.add(Long.toString(workAt.company));
            return ret;
        }

        if( string.equals("year") ) {
            date.setTimeInMillis(workAt.year);
            ret.add(DateGenerator.formatYear(date));
            return ret;
        }
        return null;
    }
}
