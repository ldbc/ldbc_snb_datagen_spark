package ldbc.snb.datagen.hadoop.serializer;

import ldbc.snb.datagen.hadoop.DatagenReducer;

import java.io.IOException;

public abstract class StreamSorterAndSerializerReducer<T, T1, T2, T3> extends DatagenReducer<T, T1, T2, T3> {
    protected boolean compressed = false;
    protected String streamType;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        streamType = hadoopConf.get("streamType");
        try {
            compressed = conf.isCompressed();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
