package ldbc.snb.datagen.entities.dynamic.messages;

import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;

import java.util.List;

public class Post extends Message {

    private int language;

    public Post() {
        super();
    }

    public Post(long postId,
                long creationDate,
                long deletionDate,
                PersonSummary author,
                long forumId,
                String content,
                List<Integer> tags,
                int countryId,
                IP ipAddress,
                int browserId,
                int language,
                boolean isExplicitlyDeleted
    ) {
        super(postId, creationDate, deletionDate, author, forumId, content, tags, countryId, ipAddress, browserId,isExplicitlyDeleted);
        this.language = language;
    }

    public void initialize(long postId, long creationDate, long deletionDate, PersonSummary author, long forumId,
                           String content, List<Integer> tags, int countryId, IP ipAddress, int browserId, int language,
                           boolean isExplicitlyDeleted
    ) {
        super.initialize(postId, creationDate, deletionDate, author, forumId, content, tags, countryId, ipAddress, browserId,isExplicitlyDeleted);
        this.language = language;
    }

    public int getLanguage() {
        return language;
    }

    public void setLanguage(int language) {
        this.language = language;
    }

}
