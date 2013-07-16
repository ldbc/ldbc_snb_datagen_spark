package com.openlinksw.bibm.qualification;

import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.util.json.JsonList;
import com.openlinksw.util.json.JsonObject;
import com.openlinksw.util.json.impl.AutoJsonObject;
import com.openlinksw.util.json.impl.IgnoreCase;
import com.openlinksw.util.json.impl.Sequence;
import com.openlinksw.util.json.impl.SimpleJsonList;
import com.openlinksw.util.json.impl.SimpleJsonObject;

@Sequence(value={"results", "resultKeys"})
@IgnoreCase
public class ResultDescriptionLists extends AutoJsonObject {
    private ResultDescription[]  resultDescriptions;
    private Integer[] resultKeys;
    
    @Override
    public JsonList<?> newJsonList(String key) {
        if (key.equalsIgnoreCase("results")) {
            return new ResultDescriptionList();
        } else  if (key.equalsIgnoreCase("resultKeys")) {
            return new SimpleJsonList<String>();
        } else {
            return super.newJsonList(key);
        }
    }

    public void setResults(ResultDescriptionList results) {
        resultDescriptions = new ResultDescription[results.size()];
        for (int k=0; k<resultDescriptions.length; k++) {
            resultDescriptions[k]=results.get(k);
        }
        if (resultKeys!=null) {
            checkResultKeys();
        }
    }

    public ResultDescription[]  getResultDescriptions() {
        return resultDescriptions;
    }

    public Integer[] getResultKeys() {
        return resultKeys;
    }

    public void setResultKeys(JsonList<String> resultKeysJ) {
        resultKeys = new Integer[resultKeysJ.size()];
        for (int k=0; k<resultKeys.length; k++) {
            resultKeys[k]=Integer.parseInt(resultKeysJ.get(k));
        }
        if (resultDescriptions!=null) {
            checkResultKeys();
        }
    }

    private void checkResultKeys() {
        for (int k=0; k<resultKeys.length; k++) {
            Integer key = resultKeys[k];
            if (key<1 || key>resultDescriptions.length) {
                throw new BadSetupException("result key out of bounds [1.."+resultDescriptions.length+"]: "+key);
            }
        }
    }

    /**
     * to satisfy sequence
     * @return
     */
    public ResultDescription[]  getResults() {
        return resultDescriptions;
    }

    /**
     * to satisfy sequence
     * @return
     */
    public Integer[] getResult() {
        return resultKeys;
    }

    static class ResultDescriptionList extends  SimpleJsonList<SimpleJsonObject> {

        @Override
        public JsonObject newJsonObject(boolean ignoreCase) {
            return new SimpleJsonObject();
        }

        @Override
        public void add(SimpleJsonObject descr) {
            ResultDescription rd=ResultDescription.newResultDescription(descr);
            super.add(rd);
        }

        @Override
        public ResultDescription get(int k) {
            return (ResultDescription) super.get(k);
        }
    }
    

}
