package ldbc.snb.datagen.test.csv;

/**
 * Created by aprat on 22/12/15.
 */
public abstract class Parser<T> {
    public abstract T parse(String s);
}
