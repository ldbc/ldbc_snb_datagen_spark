package ldbc.snb.datagen.entities.dynamic;

import java.io.Serializable;

public interface DynamicActivity extends Serializable {
    long getCreationDate();
    long getDeletionDate();
    boolean isExplicitlyDeleted();
}
