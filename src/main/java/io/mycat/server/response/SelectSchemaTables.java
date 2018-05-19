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
package io.mycat.server.response;

import com.google.common.collect.Lists;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author mycat
 */
public final class SelectSchemaTables
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectSchemaTables.class);


    public static void execute(ServerConnection c, String sql) {


    List<String>  splitVar= Lists.newArrayList( "table_schema","table_name","engine","row_format","table_collation","table_comment","auto_increment") ;

        int FIELD_COUNT = splitVar.size();
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        FieldPacket[] fields = new FieldPacket[FIELD_COUNT];

        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        for (int i1 = 0, splitVarSize = splitVar.size(); i1 < splitVarSize; i1++)
        {
            String s = splitVar.get(i1);
            fields[i] = PacketUtil.getField(s, Fields.FIELD_TYPE_VAR_STRING);
            fields[i++].packetId = ++packetId;
        }


        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c,true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }


        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
        // write eof
        buffer = eof.write(buffer, c,true);

        // write rows
        //byte packetId = eof.packetId;

     //   RowDataPacket row = new RowDataPacket(FIELD_COUNT);
//        for (int i1 = 0, splitVarSize = splitVar.size(); i1 < splitVarSize; i1++)
//        {
//            String s = splitVar.get(i1);
//            String value=  variables.get(s) ==null?"":variables.get(s) ;
//            row.add(value.getBytes());
//
//        }
//        List<String> values = Lists.newArrayList("company","dts_increment_trx","COMPANY","DTS_INCREMENT_TRX","customers");
        List<String> values = Lists.newArrayList("company","customers");
        for (String name : values) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode("TESTDB", c.getCharset()));
            row.add(StringUtil.encode(name, c.getCharset()));
            row.add(StringUtil.encode("InnoDB", c.getCharset()));
            row.add(StringUtil.encode("Dynamic", c.getCharset()));
            row.add(StringUtil.encode("utf8mb4_general_ci", c.getCharset()));
            row.add(StringUtil.encode("", c.getCharset()));
//            row.add(StringUtil.encode("", c.getCharset()));
            row.add(null);
            row.packetId = ++packetId;
            buffer = row.write(buffer, c,true);
        }
//        for (String name : values) {
//            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
//            row.add(StringUtil.encode("BASE", c.getCharset()));
//            row.add(StringUtil.encode(name, c.getCharset()));
//            row.add(StringUtil.encode("InnoDB", c.getCharset()));
//            row.add(StringUtil.encode("Dynamic", c.getCharset()));
//            row.add(StringUtil.encode("utf8_general_ci", c.getCharset()));
//            row.add(StringUtil.encode("", c.getCharset()));
//            row.add(StringUtil.encode("", c.getCharset()));
//            row.packetId = ++packetId;
//            buffer = row.write(buffer, c,true);
//        }
//        for (String name : values) {
//            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
//            row.add(StringUtil.encode("base", c.getCharset()));
//            row.add(StringUtil.encode(name, c.getCharset()));
//            row.packetId = ++packetId;
//            buffer = row.write(buffer, c,true);
//        }
//        for (String name : values) {
//            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
//            row.add(StringUtil.encode("BASE", c.getCharset()));
//            row.add(StringUtil.encode(name, c.getCharset()));
//            row.packetId = ++packetId;
//            buffer = row.write(buffer, c,true);
//        }

//        row.packetId = ++packetId;
//        buffer = row.write(buffer, c,true);



        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static List<String> convert(List<String> in)
    {
        List<String> out=new ArrayList<>();
        for (String s : in)
        {
          int asIndex=s.toUpperCase().indexOf(" AS ");
            if(asIndex!=-1)
            {
                out.add(s.substring(asIndex+4)) ;
            }
        }
         if(out.isEmpty())
         {
             return in;
         }  else
         {
             return out;
         }


    }




    private static final Map<String, String> variables = new HashMap<String, String>();
    static {
        variables.put("@@character_set_client", "utf8");
        variables.put("@@character_set_connection", "utf8");
        variables.put("@@character_set_results", "utf8");
        variables.put("@@character_set_server", "utf8");
        variables.put("@@init_connect", "");
        variables.put("@@interactive_timeout", "172800");
        variables.put("@@license", "GPL");
        variables.put("@@lower_case_table_names", "1");
        variables.put("@@max_allowed_packet", "16777216");
        variables.put("@@net_buffer_length", "16384");
        variables.put("@@net_write_timeout", "60");
        variables.put("@@query_cache_size", "0");
        variables.put("@@query_cache_type", "OFF");
        variables.put("@@sql_mode", "STRICT_TRANS_TABLES");
        variables.put("@@system_time_zone", "CST");
        variables.put("@@time_zone", "SYSTEM");
        variables.put("@@tx_isolation", "REPEATABLE-READ");
        variables.put("@@wait_timeout", "172800");
        variables.put("@@session.auto_increment_increment", "1");

        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("license", "GPL");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "16384");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("tx_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");
        variables.put("auto_increment_increment", "1");
    }
    

}