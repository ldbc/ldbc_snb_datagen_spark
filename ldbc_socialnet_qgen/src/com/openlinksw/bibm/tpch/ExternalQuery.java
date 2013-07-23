/*
 *  Big Database Semantic Metric Tools
 *
 * Copyright (C) 2011-2013 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.openlinksw.bibm.tpch;

import java.io.IOException;

import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.util.Exec;

public class ExternalQuery extends QueryTiming {
    int streamId;
    int name; // 1 - RF1, 2 - RF2
    String[] command;
    long start; // millis
    long end; // millis

    public ExternalQuery(int streamId, int name, String... command) {
        this.streamId = streamId;
        this.name = name;
        this.command = command;
    }

    /** 
     * 
     * @param nStream
     * @return rounded execution time in seconds
     */
    public void  exec() throws InterruptedException {
        start = System.currentTimeMillis();
        try {
            Exec.execProcess(command);
        } catch (IOException e) {
           throw new ExceptionException("failure in "+name, e);
        }
        end=System.currentTimeMillis();
    }

    @Override
    public String getTiming() {
        return getTiming(end-start);
     }

    public double getTimeiseconds() {
        return end-start;
     }
}
