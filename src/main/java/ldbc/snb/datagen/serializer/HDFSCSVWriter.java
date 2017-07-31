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


import java.io.IOException;
import java.util.ArrayList;

public class HDFSCSVWriter extends HDFSWriter {

    private String separator = "|";
    private StringBuffer buffer;
    private boolean endLineSeparator = true;


    public HDFSCSVWriter(String outputDir, String prefix, int numPartitions, boolean compressed, String separator, boolean endLineSeparator) throws IOException {
        super(outputDir, prefix, numPartitions, compressed, "csv");
        this.separator = separator;
        this.buffer = new StringBuffer(2048);
        this.endLineSeparator = endLineSeparator;

    }

    public void writeHeader(ArrayList<String> entry) {
        buffer.setLength(0);
        for (int i = 0; i < entry.size(); ++i) {
            buffer.append(entry.get(i));
            if ((endLineSeparator && i == (entry.size() - 1)) || (i < entry.size() - 1))
                buffer.append(separator);
        }
        buffer.append("\n");
        this.writeAllPartitions(buffer.toString());
    }

    public void writeEntry(ArrayList<String> entry) {
        buffer.setLength(0);
        for (int i = 0; i < entry.size(); ++i) {
            buffer.append(entry.get(i));
            if ((endLineSeparator && i == (entry.size() - 1)) || (i < entry.size() - 1))
                buffer.append(separator);
        }
        buffer.append("\n");
        this.write(buffer.toString());
    }
}
