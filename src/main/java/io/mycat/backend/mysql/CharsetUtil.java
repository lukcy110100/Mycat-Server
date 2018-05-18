/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author mycat
 */
public class CharsetUtil {
    public static final Logger logger = LoggerFactory
            .getLogger(CharsetUtil.class);
    private static final Map<Integer,String> INDEX_TO_CHARSET = new HashMap<>();
    private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();
    private static final Map<String, String> CHARSET_TO_JAVA = new HashMap<>();
    static {

        // index_to_charset.properties
        INDEX_TO_CHARSET.put(1,"big5");
        INDEX_TO_CHARSET.put(8,"latin1");
        INDEX_TO_CHARSET.put(9,"latin2");
        INDEX_TO_CHARSET.put(14,"cp1251");
        INDEX_TO_CHARSET.put(28,"gbk");
        INDEX_TO_CHARSET.put(24,"gb2312");
        INDEX_TO_CHARSET.put(33,"utf8");
        INDEX_TO_CHARSET.put(45,"utf8mb4");

        String filePath = Thread.currentThread().getContextClassLoader()
                .getResource("").getPath().replaceAll("%20", " ")
                + "index_to_charset.properties";
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(filePath));
            for (Object index : prop.keySet()){
               INDEX_TO_CHARSET.put(Integer.parseInt((String) index), prop.getProperty((String) index));
            }
        } catch (Exception e) {
            logger.error("error:",e);
        }
        
        // charset --> index
        for(Integer key : INDEX_TO_CHARSET.keySet()){
        	String charset = INDEX_TO_CHARSET.get(key);
        	if(charset != null && CHARSET_TO_INDEX.get(charset) == null){
        		CHARSET_TO_INDEX.put(charset, key);
        	}
        }

        CHARSET_TO_INDEX.put("iso-8859-1", 14);
        CHARSET_TO_INDEX.put("iso_8859_1", 14);
        CHARSET_TO_INDEX.put("utf-8", 33);


        CHARSET_TO_JAVA.put("usa7", "US-ASCII");
        CHARSET_TO_JAVA.put("ascii", "US-ASCII");
        CHARSET_TO_JAVA.put("big5", "Big5");
        CHARSET_TO_JAVA.put("dec8", "ISO8859_1");
        CHARSET_TO_JAVA.put("cp850", "Cp850");
        CHARSET_TO_JAVA.put("hp8", "ISO8859_1");
        CHARSET_TO_JAVA.put("koi8r", "KOI8_R");
        CHARSET_TO_JAVA.put("latin1", "Cp1252");
        CHARSET_TO_JAVA.put("latin1_de", "ISO8859_1");
        CHARSET_TO_JAVA.put("german1", "ISO8859_1");
        CHARSET_TO_JAVA.put("danish", "ISO8859_1");
        CHARSET_TO_JAVA.put("latin2", "ISO8859_2");
        CHARSET_TO_JAVA.put("swe7", "ISO8859_1");
        CHARSET_TO_JAVA.put("ascii", "US-ASCII");
        CHARSET_TO_JAVA.put("ujis", "EUC_JP");
        CHARSET_TO_JAVA.put("sjis", "SJIS");
        CHARSET_TO_JAVA.put("hebrew", "ISO8859_8");
        CHARSET_TO_JAVA.put("tis620", "TIS620");
        CHARSET_TO_JAVA.put("euckr", "EUC_KR");
        CHARSET_TO_JAVA.put("koi8u", "KOI8_U");
        CHARSET_TO_JAVA.put("gb2312", "EUC_CN");
        CHARSET_TO_JAVA.put("greek", "ISO8859_7");
        CHARSET_TO_JAVA.put("cp1250", "Cp1250");
        CHARSET_TO_JAVA.put("gbk", "GBK");
        CHARSET_TO_JAVA.put("latin5", "ISO8859_9");
        CHARSET_TO_JAVA.put("armscii8", "ISO8859_1");
        CHARSET_TO_JAVA.put("utf8", "UTF-8");
        CHARSET_TO_JAVA.put("ucs2", "UnicodeBig");
        CHARSET_TO_JAVA.put("cp866", "Cp866");
        CHARSET_TO_JAVA.put("keybcs2", "Cp895");
        CHARSET_TO_JAVA.put("macce", "MacCentralEurope");
        CHARSET_TO_JAVA.put("macroman", "MacRoman");
        CHARSET_TO_JAVA.put("cp852", "Cp852");
        CHARSET_TO_JAVA.put("latin7", "ISO8859_13");
        CHARSET_TO_JAVA.put("utf8mb4", "UTF-8");
        CHARSET_TO_JAVA.put("cp1251", "Cp1251");
        CHARSET_TO_JAVA.put("utf16", "UTF-16");
        CHARSET_TO_JAVA.put("utf16le", "UTF-16LE");
        CHARSET_TO_JAVA.put("cp1256", "Cp1256");
        CHARSET_TO_JAVA.put("cp1257", "Cp1257");
        CHARSET_TO_JAVA.put("utf32", "UTF-32");
        CHARSET_TO_JAVA.put("binary", "ISO8859_1");
        CHARSET_TO_JAVA.put("geostd8", "US-ASCII");
        CHARSET_TO_JAVA.put("cp932", "MS932");
        CHARSET_TO_JAVA.put("eucjpms", "EUC_JP_Solaris");
        CHARSET_TO_JAVA.put("gb18030", "GB18030");
    }

    public static final String getCharset(int index) {
        return INDEX_TO_CHARSET.get(index);
    }

    public static final int getIndex(String charset) {
        if (charset == null || charset.length() == 0) {
            return 0;
        } else {
            Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
            return (i == null) ? 0 : i;
        }
    }

    public static String getJavaCharset(String charset)
    {
        if (charset == null || charset.length() == 0)
            return StandardCharsets.UTF_8.toString();
        String javaCharset = CHARSET_TO_JAVA.get(charset);
        if (javaCharset == null)
            return StandardCharsets.UTF_8.toString();
        return javaCharset;
    }

}
