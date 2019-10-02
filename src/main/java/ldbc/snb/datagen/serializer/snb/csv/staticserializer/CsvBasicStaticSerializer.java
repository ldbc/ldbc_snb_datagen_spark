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
package ldbc.snb.datagen.serializer.snb.csv.staticserializer;

import com.google.common.collect.ImmutableList;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.hadoop.writer.HdfsCsvWriter;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.CsvSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import ldbc.snb.datagen.vocabulary.DBP;
import ldbc.snb.datagen.vocabulary.DBPOWL;

import java.util.List;

import static ldbc.snb.datagen.serializer.snb.csv.FileName.*;

public class CsvBasicStaticSerializer extends StaticSerializer<HdfsCsvWriter> implements CsvSerializer {

    @Override
    public List<FileName> getFileNames() {
        return ImmutableList.of(TAG, TAG_HAS_TYPE_TAGCLASS, TAGCLASS, TAGCLASS_IS_SUBCLASS_OF_TAGCLASS, PLACE, PLACE_IS_PART_OF_PLACE, ORGANISATION, ORGANISATION_IS_LOCATED_IN_PLACE);
    }

    @Override
    public void writeFileHeaders() {
        writers.get(TAG).writeHeader(ImmutableList.of("id", "name", "url"));
        writers.get(TAG_HAS_TYPE_TAGCLASS).writeHeader(ImmutableList.of("Tag.id", "TagClass.id"));
        writers.get(TAGCLASS).writeHeader(ImmutableList.of("id", "name", "url"));
        writers.get(TAGCLASS_IS_SUBCLASS_OF_TAGCLASS).writeHeader(ImmutableList.of("TagClass.id", "TagClass.id"));
        writers.get(PLACE).writeHeader(ImmutableList.of("id", "name", "url", "type"));
        writers.get(ORGANISATION).writeHeader(ImmutableList.of("id", "type", "name", "url"));
        writers.get(ORGANISATION_IS_LOCATED_IN_PLACE).writeHeader(ImmutableList.of("Organisation.id", "Place.id"));
        writers.get(PLACE_IS_PART_OF_PLACE).writeHeader(ImmutableList.of("Place.id", "Place.id"));
    }

    protected void serialize(final Place place) {
        writers.get(PLACE).writeEntry(ImmutableList.of(
            Integer.toString(place.getId()),
            place.getName(),
            DBP.getUrl(place.getName()),
            place.getType()
        ));

        if (place.getType() == Place.CITY || place.getType() == Place.COUNTRY) {
            writers.get(PLACE_IS_PART_OF_PLACE).writeEntry(ImmutableList.of(
                Integer.toString(place.getId()),
                Integer.toString(Dictionaries.places.belongsTo(place.getId()))
            ));
        }
    }

    protected void serialize(final Organisation organisation) {
        writers.get(ORGANISATION).writeEntry(ImmutableList.of(
            Long.toString(organisation.id),
            organisation.type.toString(),
            organisation.name,
            DBP.getUrl(organisation.name)
        ));

        writers.get(ORGANISATION_IS_LOCATED_IN_PLACE).writeEntry(ImmutableList.of(
            Long.toString(organisation.id),
            Integer.toString(organisation.location)
        ));
    }

    protected void serialize(final TagClass tagClass) {
        writers.get(TAGCLASS).writeEntry(ImmutableList.of(
                Integer.toString(tagClass.id),
                tagClass.name,
                "Thing".equals(tagClass.name) ? "http://www.w3.org/2002/07/owl#Thing" : DBPOWL.getUrl(tagClass.name)
        ));
        if (tagClass.parent != -1) {
            writers.get(TAGCLASS_IS_SUBCLASS_OF_TAGCLASS).writeEntry(ImmutableList.of(
                Integer.toString(tagClass.id),
                Integer.toString(tagClass.parent)
            ));
        }
    }

    protected void serialize(final Tag tag) {
        writers.get(TAG).writeEntry(ImmutableList.of(Integer.toString(tag.id), tag.name, DBP.getUrl(tag.name)));
        writers.get(TAG_HAS_TYPE_TAGCLASS).writeEntry(ImmutableList.of(Integer.toString(tag.id), Integer.toString(tag.tagClass)));
    }

}
