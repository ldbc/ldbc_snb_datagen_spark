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

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.util.FactorTable;

import java.io.IOException;

/**
 * Created by aprat on 3/01/16.
 */
public class PersonActivityExporter {
    protected PersonActivitySerializer personActivitySerializer_;
    protected UpdateEventSerializer updateSerializer_;
    protected FactorTable factorTable_;

    public PersonActivityExporter(PersonActivitySerializer personActivitySerializer, UpdateEventSerializer updateEventSerializer, FactorTable factorTable) {
        this.personActivitySerializer_ = personActivitySerializer;
        this.updateSerializer_ = updateEventSerializer;
        this.factorTable_ = factorTable;
    }

    public void export(final Forum forum) throws IOException {
        if (forum.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            personActivitySerializer_.export(forum);
        } else {
            updateSerializer_.export(forum);
        }
    }

    public void export(final Post post) throws IOException {
        if (post.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            personActivitySerializer_.export(post);
            factorTable_.extractFactors(post);
        } else {
            updateSerializer_.export(post);
        }
    }

    public void export(final Comment comment) throws IOException {
        if (comment.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            personActivitySerializer_.export(comment);
            factorTable_.extractFactors(comment);
        } else {
            updateSerializer_.export(comment);
        }
    }

    public void export(final Photo photo) throws IOException {
        if (photo.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            personActivitySerializer_.export(photo);
            factorTable_.extractFactors(photo);
        } else {
            updateSerializer_.export(photo);
        }
    }

    public void export(final ForumMembership member) throws IOException {
        if (member.creationDate() < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            personActivitySerializer_.export(member);
            factorTable_.extractFactors(member);
        } else {
            updateSerializer_.export(member);
        }
    }

    public void export(final Like like) throws IOException {
        if (like.date < Dictionaries.dates.getUpdateThreshold() || !DatagenParams.updateStreams) {
            personActivitySerializer_.export(like);
            factorTable_.extractFactors(like);
        } else {
            updateSerializer_.export(like);
        }
    }
}
