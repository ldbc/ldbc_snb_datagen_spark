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

import ldbc.snb.datagen.spark.generation.entities.statictype.Organisation;
import ldbc.snb.datagen.spark.generation.entities.statictype.TagClass;
import ldbc.snb.datagen.spark.generation.entities.statictype.place.Place;
import ldbc.snb.datagen.spark.generation.entities.statictype.tag.Tag;
import ldbc.snb.datagen.hadoop.writer.HdfsWriter;

abstract public class StaticSerializer<TWriter extends HdfsWriter> extends LdbcSerializer<TWriter> {

    abstract public void serialize(final Place place);

    abstract public void serialize(final Organisation organisation);

    abstract public void serialize(final TagClass tagClass);

    abstract public void serialize(final Tag tag);

    @Override
    protected boolean isDynamic() {
        return false;
    }

}
