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

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.google.common.collect.Lists;
import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
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
import java.util.List;
import java.util.Map;

/**
 * @author mycat
 */
public final class SelectSchemata
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectSchemata.class);


    public static void execute(ServerConnection c, String sql) {


        // 从配置文件获取逻辑库和用户名进行拼接
        List<String> allColumn = Lists.newArrayList("CATALOG_NAME","SCHEMA_NAME","DEFAULT_CHARACTER_SET_NAME","DEFAULT_COLLATION_NAME","SQL_PATH");
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        List<String> splitVar = SelectDTXBase.getHeadder(visitor.getColumns(), allColumn);

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
        // 从配置文件获取逻辑库和用户名进行拼接
        MycatConfig conf = MycatServer.getInstance().getConfig();
        Map<String, SchemaConfig> schemas = conf.getSchemas();
        // 构造结果集，包含所有列，同時根據條件過濾
        Map<String, Integer> columnIndex = SelectDTXBase.toMapIndex(allColumn);
        List<String[]> dataRows = new ArrayList<>();
        schemas.forEach((k, v) -> {
            String[] datRow = new String[]{"def", k, "utf8", "utf8_general_ci", null};
            if (SelectDTXBase.filterRow(datRow, visitor.getConditions(), columnIndex)) {
                dataRows.add(datRow);
            }
        });

        // 返回结果集，過濾列
        for (String[] dataRow : dataRows) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            for (String head : splitVar) {
                row.add(StringUtil.encode(dataRow[columnIndex.get(head)], c.getCharset()));
            }
            row.packetId = ++packetId;
            buffer = row.write(buffer, c, true);
        }


        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

}