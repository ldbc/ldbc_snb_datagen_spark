package ldbc.snb.datagen.util;

/**
 * Created by aprat on 25/11/16.
 */
public class StringUtils {

    public static String clampString( String str, int length) {
        if(str.length() > length) return str.substring(0,length);
        return str;
    }
}
