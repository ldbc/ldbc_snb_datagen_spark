package ldbc.snb.datagen.test.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by aprat on 18/12/15.
 */
public abstract class Check {

    protected String checkName = null;
    protected List<Integer> columns = null;

    public String getCheckName() {
        return checkName;
    }

    public Check(String name, List<Integer> columns) {
        this.checkName = name;
        this.columns = columns;
    }

    public List<Integer> getColumns() {
        return columns;
    }

    public abstract boolean check(List<String> values);
}

