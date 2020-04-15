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

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.util.FactorTable;

import java.io.IOException;

public class PersonActivityExporter {
    protected DynamicActivitySerializer dynamicActivitySerializer;
    protected InsertEventSerializer insertEventSerializer;
    protected DeleteEventSerializer deleteEventSerializer;
    protected FactorTable factorTable;

    public PersonActivityExporter(DynamicActivitySerializer dynamicActivitySerializer, InsertEventSerializer insertEventSerializer, DeleteEventSerializer deleteEventSerializer, FactorTable factorTable) {
        this.dynamicActivitySerializer = dynamicActivitySerializer;
        this.factorTable = factorTable;
        this.insertEventSerializer = insertEventSerializer;
        this.deleteEventSerializer = deleteEventSerializer;
    }

    public void export(final Forum forum) throws IOException {

        if ((forum.getCreationDate() < Dictionaries.dates.getBulkLoadThreshold() &&
                forum.getDeletionDate() >= Dictionaries.dates.getBulkLoadThreshold())
                || !DatagenParams.updateStreams) {
            dynamicActivitySerializer.export(forum);
            if (forum.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(forum);
                deleteEventSerializer.changePartition();
            }
        } else if (forum.getCreationDate() >= Dictionaries.dates.getBulkLoadThreshold()) {
            insertEventSerializer.export(forum);
            insertEventSerializer.changePartition();
            if (forum.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(forum);
                deleteEventSerializer.changePartition();
            }
        }

    }

    public void export(final Post post) throws IOException {

        if ((post.getCreationDate() < Dictionaries.dates.getBulkLoadThreshold() &&
                post.getDeletionDate() >= Dictionaries.dates.getBulkLoadThreshold())
                || !DatagenParams.updateStreams) {
            dynamicActivitySerializer.export(post);
            factorTable.extractFactors(post);
            if (post.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(post);
                deleteEventSerializer.changePartition();
            }
        } else if (post.getCreationDate() >= Dictionaries.dates.getBulkLoadThreshold()) {
            insertEventSerializer.export(post);
            insertEventSerializer.changePartition();
            if (post.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(post);
                deleteEventSerializer.changePartition();
            }
        }

    }

    public void export(final Comment comment) throws IOException {
        if ((comment.getCreationDate() < Dictionaries.dates.getBulkLoadThreshold() &&
                comment.getDeletionDate() >= Dictionaries.dates.getBulkLoadThreshold())
                || !DatagenParams.updateStreams) {
            dynamicActivitySerializer.export(comment);
            factorTable.extractFactors(comment);
            if (comment.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(comment);
                deleteEventSerializer.changePartition();
            }
        } else if (comment.getCreationDate() >= Dictionaries.dates.getBulkLoadThreshold()) {
            insertEventSerializer.export(comment);
            insertEventSerializer.changePartition();
            if (comment.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(comment);
                deleteEventSerializer.changePartition();
            }
        }
    }

    public void export(final Photo photo) throws IOException {

        if ((photo.getCreationDate() < Dictionaries.dates.getBulkLoadThreshold() &&
                photo.getDeletionDate() >= Dictionaries.dates.getBulkLoadThreshold())
                || !DatagenParams.updateStreams) {
            dynamicActivitySerializer.export(photo);
            factorTable.extractFactors(photo);
            if (photo.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(photo);
                deleteEventSerializer.changePartition();
            }
        } else if (photo.getCreationDate() >= Dictionaries.dates.getBulkLoadThreshold()) {
            insertEventSerializer.export(photo);
            insertEventSerializer.changePartition();
            if (photo.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(photo);
                deleteEventSerializer.changePartition();
            }
        }

    }

    public void export(final ForumMembership member) throws IOException {

        if ((member.getCreationDate() < Dictionaries.dates.getBulkLoadThreshold() &&
                member.getDeletionDate() >= Dictionaries.dates.getBulkLoadThreshold())
                || !DatagenParams.updateStreams) {
            dynamicActivitySerializer.export(member);
            factorTable.extractFactors(member);
            if (member.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(member);
                deleteEventSerializer.changePartition();
            }
        } else if (member.getCreationDate() >= Dictionaries.dates.getBulkLoadThreshold()) {
            insertEventSerializer.export(member);
            insertEventSerializer.changePartition();
            if (member.getDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(member);
                deleteEventSerializer.changePartition();
            }
        }
    }

    public void export(final Like like) throws IOException {

        if ((like.getLikeCreationDate() < Dictionaries.dates.getBulkLoadThreshold() &&
                like.getLikeDeletionDate() >= Dictionaries.dates.getBulkLoadThreshold())
                || !DatagenParams.updateStreams) {
            dynamicActivitySerializer.export(like);
            factorTable.extractFactors(like);
            if (like.getLikeDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(like);
                deleteEventSerializer.changePartition();
            }
        } else if (like.getLikeCreationDate() >= Dictionaries.dates.getBulkLoadThreshold()) {
            insertEventSerializer.export(like);
            insertEventSerializer.changePartition();
            if (like.getLikeDeletionDate() != Dictionaries.dates.getNetworkCollapse()) {
                deleteEventSerializer.export(like);
                deleteEventSerializer.changePartition();
            }
        }
    }
}
