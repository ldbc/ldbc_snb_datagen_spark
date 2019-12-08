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
package ldbc.snb.datagen.serializer.empty;

import ldbc.snb.datagen.entities.statictype.Organisation;
import ldbc.snb.datagen.entities.statictype.TagClass;
import ldbc.snb.datagen.entities.statictype.place.Place;
import ldbc.snb.datagen.entities.statictype.tag.Tag;
import ldbc.snb.datagen.serializer.StaticSerializer;
import ldbc.snb.datagen.serializer.snb.csv.FileName;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmptyStaticSerializer extends StaticSerializer {

    @Override
    public List<FileName> getFileNames() {
        return Collections.emptyList();
    }

    @Override
    public void writeFileHeaders() {

    }

    @Override
    protected void serialize(final Place place) {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final Organisation organisation) {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final TagClass tagClass) {
        //Intentionally left empty

    }

    @Override
    protected void serialize(final Tag tag) {
        //Intentionally left empty

    }

    @Override
    public Map initialize(Configuration conf, int reducerId, boolean dynamic, List list) throws IOException {
        return Collections.emptyMap();
    }

}
