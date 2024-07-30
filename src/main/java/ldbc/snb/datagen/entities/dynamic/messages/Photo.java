package ldbc.snb.datagen.entities.dynamic.messages;

import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;

import java.util.List;

/**
 * In the schema photo extends post. For conciseness it is treated as a message.
 */
public class Photo extends Message {

    public Photo() {
        super();
    }

    public Photo(long messageId,
                 long creationDate,
                 long deletionDate,
                 PersonSummary author,
                 long forumId,
                 String content,
                 List<Integer> tags,
                 int countryId,
                 IP ipAddress,
                 int browserId,
                 boolean isExplicitlyDeleted
    ) {
        super(messageId, creationDate, deletionDate, author, forumId, content, tags, countryId, ipAddress, browserId,isExplicitlyDeleted);
    }
}
