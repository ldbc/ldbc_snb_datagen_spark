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
package com.openlinksw.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class ByteArrayStream extends ByteArrayOutputStream {
	
	public ByteArrayStream() {
		super(4*1024);
	}

	public void fillFrom(InputStream inp) throws IOException {
		byte[] buf=new byte[1024];
		for (;;) {
			int len=inp.read(buf);
			if (len==-1) break;
			super.write(buf, 0, len);
		}
	}

	public ByteArrayInputStream getInputStream() {
		return new ByteArrayInputStream(buf, 0, count);
	}

}
