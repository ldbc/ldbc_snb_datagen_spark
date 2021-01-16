package ldbc.snb.datagen.spark.generation.entities.dynamic;

import java.io.Serializable;

public interface DynamicActivity extends Serializable {
    long getCreationDate();
    long getDeletionDate();


}
