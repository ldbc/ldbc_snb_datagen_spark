/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.generator.generators.GenActivity;
import ldbc.snb.datagen.generator.generators.GenWall;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.util.FactorTable;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.IOException;
import java.util.stream.Stream;

import static ldbc.snb.datagen.util.functional.Thunk.wrapException;

public class PersonActivityExporter implements AutoCloseable {
    protected DynamicActivitySerializer dynamicActivitySerializer;
    protected FactorTable factorTable;

    public PersonActivityExporter(DynamicActivitySerializer dynamicActivitySerializer, FactorTable factorTable) {
        this.dynamicActivitySerializer = dynamicActivitySerializer;
        this.factorTable = factorTable;
    }

    public void export(final GenActivity genActivity) {
        this.exportPostWall(genActivity.genWall);
        genActivity.genGroups.forEach(this::exportPostWall);
        this.exportAlbumWall(genActivity.genAlbums);
    }

    private void exportPostWall(final GenWall<Triplet<Post, Stream<Like>, Stream<Pair<Comment, Stream<Like>>>>> genWall) {
        genWall.inner.forEach(forum -> {
            wrapException(() -> this.exportForum(forum.getValue0()));
            Stream<ForumMembership> genForumMembership = forum.getValue1();
            genForumMembership.forEach(m -> wrapException(() -> this.exportForumMembership(m)));
            Stream<Triplet<Post, Stream<Like>, Stream<Pair<Comment, Stream<Like>>>>> thread = forum.getValue2();
            thread.forEach(t -> {
                wrapException(() -> this.exportPost(t.getValue0()));
                Stream<Like> genLike = t.getValue1();
                genLike.forEach(l -> wrapException(() -> this.exportLike(l)));
                Stream<Pair<Comment, Stream<Like>>> genComment = t.getValue2();
                genComment.forEach(c -> {
                    wrapException(() -> this.exportComment(c.getValue0()));
                    Stream<Like> genLike1 = c.getValue1();
                    genLike1.forEach(l -> wrapException(() -> this.exportLike(l)));
                });
            });
        });
    }

    private void exportAlbumWall(final GenWall<Pair<Photo, Stream<Like>>> genAlbums) {
        genAlbums.inner.forEach(forum -> {
            wrapException(() -> this.exportForum(forum.getValue0()));
            Stream<ForumMembership> genForumMembership = forum.getValue1();
            genForumMembership.forEach(m -> wrapException(() -> this.exportForumMembership(m)));
            Stream<Pair<Photo, Stream<Like>>> thread = forum.getValue2();
            thread.forEach(t -> {
                wrapException(() -> this.exportPhoto(t.getValue0()));
                Stream<Like> genLike = t.getValue1();
                genLike.forEach(l -> wrapException(() -> this.exportLike(l)));
            });
        });
    }

    private void exportForum(final Forum forum) throws Exception {
        dynamicActivitySerializer.serialize(forum);
    }

    private void exportPost(final Post post) throws IOException {
        dynamicActivitySerializer.serialize(post);
    }

    private void exportComment(final Comment comment) throws IOException {
        dynamicActivitySerializer.serialize(comment);
    }

    private void exportPhoto(final Photo photo) throws IOException {
        dynamicActivitySerializer.serialize(photo);
        factorTable.extractFactors(photo);
    }

    private void exportForumMembership(final ForumMembership member) throws IOException {
        dynamicActivitySerializer.serialize(member);
        factorTable.extractFactors(member);
    }

    private void exportLike(final Like like) throws IOException {
        dynamicActivitySerializer.serialize(like);
        factorTable.extractFactors(like);
    }

    @Override
    public void close() throws IOException {
        dynamicActivitySerializer.close();
    }
}
