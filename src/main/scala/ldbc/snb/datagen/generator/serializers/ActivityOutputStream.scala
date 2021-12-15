package ldbc.snb.datagen.generator.serializers

import ldbc.snb.datagen.entities.dynamic.Forum
import ldbc.snb.datagen.entities.dynamic.messages.{Comment, Photo, Post}
import ldbc.snb.datagen.entities.dynamic.relations.{ForumMembership, Like}
import ldbc.snb.datagen.generator.dictionary.Dictionaries
import ldbc.snb.datagen.generator.generators.{GenActivity, GenWall}
import ldbc.snb.datagen.io.raw.RecordOutputStream
import ldbc.snb.datagen.model.raw
import org.javatuples.{Pair, Triplet}

import java.util.function.Consumer
import java.util.stream.Stream
import scala.collection.JavaConverters._

class ActivityOutputStream(
    forumStream: RecordOutputStream[raw.Forum],
    forumHasTagStream: RecordOutputStream[raw.ForumHasTag],
    forumHasMemberStream: RecordOutputStream[raw.ForumHasMember],
    postStream: RecordOutputStream[raw.Post],
    postHasTagStream: RecordOutputStream[raw.PostHasTag],
    commentStream: RecordOutputStream[raw.Comment],
    commentHasTagStream: RecordOutputStream[raw.CommentHasTag],
    personLikesPostStream: RecordOutputStream[raw.PersonLikesPost],
    personLikesCommentStream: RecordOutputStream[raw.PersonLikesComment]
) extends RecordOutputStream[GenActivity] {
  import ActivityOutputStream._

  override def write(genActivity: GenActivity): Unit = {
    writePostWall(genActivity.genWall)
    for { group <- genActivity.genGroups } writePostWall(group)
    writeAlbumWall(genActivity.genAlbums)
  }

  private def writePostWall(genWall: GenWall[Triplet[Post, Stream[Like], Stream[Pair[Comment, Stream[Like]]]]]): Unit = {
    for { forum <- genWall.inner } {
      writeForum(forum.getValue0)
      for { forumMembership <- forum.getValue1 } writeForumMembership(forumMembership)
      for { post            <- forum.getValue2 } {
        writePost(post.getValue0)
        for { like    <- post.getValue1 } writeLike(like)
        for { comment <- post.getValue2 } {
          writeComment(comment.getValue0)
          for { like <- comment.getValue1 } writeLike(like)
        }
      }
    }
  }

  private def writeAlbumWall(genAlbums: GenWall[Pair[Photo, Stream[Like]]]): Unit = {
    for { forum <- genAlbums.inner } {
      writeForum(forum.getValue0)
      for { forumMembership <- forum.getValue1 } writeForumMembership(forumMembership)
      for { photo           <- forum.getValue2 } {
        writePhoto(photo.getValue0)
        for { like <- photo.getValue1 } writeLike(like)
      }
    }
  }

  private def writeForum(forum: Forum): Unit = {
    val rawForum = raw.Forum(
      forum.getCreationDate,
      forum.getDeletionDate,
      forum.isExplicitlyDeleted,
      forum.getId,
      forum.getTitle,
      forum.getModerator.getAccountId
    )

    forumStream.write(rawForum)

    for (i <- forum.getTags.iterator().asScala) { // creationDate, deletionDate, ForumId, TagId
      val forumHasTag = raw.ForumHasTag(forum.getCreationDate, forum.getDeletionDate, forum.getId, i)
      forumHasTagStream.write(forumHasTag)
    }
  }

  private def writePost(post: Post): Unit = {
    val rawPost = raw.Post(
      post.getCreationDate,
      post.getDeletionDate,
      post.isExplicitlyDeleted,
      post.getMessageId,
      None,
      post.getIpAddress.toString,
      Dictionaries.browsers.getName(post.getBrowserId),
      Some(Dictionaries.languages.getLanguageName(post.getLanguage)),
      Some(post.getContent),
      post.getContent.length,
      post.getAuthor.getAccountId,
      post.getForumId,
      post.getCountryId
    )

    postStream.write(rawPost)

    for { i <- post.getTags.iterator().asScala } {
      postHasTagStream.write(raw.PostHasTag(post.getCreationDate, post.getDeletionDate, post.getMessageId, i))
    }
  }

  private def writeComment(comment: Comment): Unit = {

    val (root, parent) = if (comment.getParentMessageId == comment.getRootPostId) {
      (Some(comment.getParentMessageId), None)
    } else {
      (None, Some(comment.getParentMessageId))
    }

    val rawComment = raw.Comment(
      comment.getCreationDate,
      comment.getDeletionDate,
      comment.isExplicitlyDeleted,
      comment.getMessageId,
      comment.getIpAddress.toString,
      Dictionaries.browsers.getName(comment.getBrowserId),
      comment.getContent,
      comment.getContent.length,
      comment.getAuthor.getAccountId,
      comment.getCountryId,
      root,
      parent
    )

    commentStream.write(rawComment)

    for { i <- comment.getTags.iterator().asScala } {
      commentHasTagStream.write(raw.CommentHasTag(comment.getCreationDate, comment.getDeletionDate, comment.getMessageId, i))
    }
  }

  private def writePhoto(photo: Photo): Unit = {
    val rawPhoto = raw.Post(
      photo.getCreationDate,
      photo.getDeletionDate,
      photo.isExplicitlyDeleted,
      photo.getMessageId,
      Some(photo.getContent),
      photo.getIpAddress.toString,
      Dictionaries.browsers.getName(photo.getBrowserId),
      None,
      None,
      0,
      photo.getAuthor.getAccountId,
      photo.getForumId,
      photo.getCountryId
    )

    postStream.write(rawPhoto)

    for { i <- photo.getTags.iterator().asScala } {
      postHasTagStream.write(raw.PostHasTag(photo.getCreationDate, photo.getDeletionDate, photo.getMessageId, i))
    }
  }

  private def writeForumMembership(member: ForumMembership): Unit = {

    val rawForumMembership = raw.ForumHasMember(
      member.getCreationDate,
      member.getDeletionDate,
      member.isExplicitlyDeleted,
      member.getForumId,
      member.getPerson.getAccountId
    )

    forumHasMemberStream.write(rawForumMembership)
  }

  private def writeLike(like: Like): Unit = {
    like.getType match {
      case Like.LikeType.POST | Like.LikeType.PHOTO => {
        personLikesPostStream.write(
          raw.PersonLikesPost(
            like.getCreationDate,
            like.getDeletionDate,
            like.isExplicitlyDeleted,
            like.getPerson,
            like.getMessageId
          )
        )
      }
      case Like.LikeType.COMMENT => {
        personLikesCommentStream.write(
          raw.PersonLikesComment(
            like.getCreationDate,
            like.getDeletionDate,
            like.isExplicitlyDeleted,
            like.getPerson,
            like.getMessageId
          )
        )
      }
    }
  }

  override def close(): Unit = {
    forumStream.close()
    forumHasTagStream.close()
    forumHasMemberStream.close()
    postStream.close()
    postHasTagStream.close()
    commentStream.close()
    commentHasTagStream.close()
    personLikesPostStream.close()
    personLikesCommentStream.close()
  }
}

object ActivityOutputStream {
  implicit final class ScalaLoopSupportForJavaStream[A](val self: Stream[A]) extends AnyVal {
    def foreach[U](fn: A => U): Unit = {
      self.forEach(new Consumer[A] { override def accept(t: A): Unit = fn(t) })
    }
  }
}
