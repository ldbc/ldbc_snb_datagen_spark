package ldbc.snb.datagen.entities.dynamic.messages;


import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.person.PersonSummary;

import java.util.List;

public class Comment extends Message {

    private long rootPostId;
    private long parentMessageId;

    public Comment() {
        super();
    }

    public Comment(Comment comment) {
        super(comment.getMessageId(), comment.getCreationDate(), comment.getDeletionDate(), comment.getAuthor(), comment.getForumId(), comment.getContent(),
              comment.getTags(), comment.getCountryId(), comment.getIpAddress(), comment.getBrowserId(),comment.isExplicitlyDeleted());
        rootPostId = comment.getRootPostId();
        parentMessageId = comment.getParentMessageId();
    }

    public Comment(long commentId,
                   long creationDate,
                   long deletionDate,
                   PersonSummary author,
                   long forumId,
                   String content,
                   List<Integer> tags,
                   int countryId,
                   IP ipAddress,
                   int browserId,
                   long rootPostId,
                   long parentMessageId,
                   boolean isExplicitlyDeleted
    ) {

        super(commentId, creationDate, deletionDate, author, forumId, content, tags, countryId, ipAddress, browserId,isExplicitlyDeleted);
        this.rootPostId = rootPostId;
        this.parentMessageId = parentMessageId;
    }

    public void initialize(long commentId,
                           long creationDate,
                           long deletionDate,
                           PersonSummary author,
                           long forumId,
                           String content,
                           List<Integer> tags,
                           int countryId,
                           IP ipAddress,
                           int browserId,
                           long rootPostId,
                           long parentMessageId,
                           boolean isExplicitlyDeleted) {
        super.initialize(commentId, creationDate, deletionDate, author, forumId, content, tags, countryId, ipAddress, browserId,isExplicitlyDeleted);
        this.rootPostId = rootPostId;
        this.parentMessageId = parentMessageId;
    }

    public long getRootPostId() {
        return rootPostId;
    }

    public void setRootPostId(long rootPostId) {
        this.rootPostId = rootPostId;
    }

    public long getParentMessageId() {
        return parentMessageId;
    }

    public void setParentMessageId(long parentMessageId) {
        this.parentMessageId = parentMessageId;
    }
}
