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
import com.alibaba.druid.stat.TableStat;
import com.google.common.collect.Lists;
import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author mycat
 */
public final class SelectDTXBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SelectSchemaPrivilege.class);

    public static boolean filterRow(String[] dataRow, List<TableStat.Condition> conditions, Map<String, Integer> columnIndex) {
        for (TableStat.Condition condition : conditions) {
            boolean flag;
            List<Object> values = condition.getValues();
            Integer index = columnIndex.get(condition.getColumn().getName().toUpperCase());
            String data = dataRow[columnIndex.get(condition.getColumn().getName().toUpperCase())];
            if(index == null){
                LOGGER.error("Condition column name error, please check, Condition column name:{}", condition.getColumn().getName());
                return false;
            }
            switch (condition.getOperator().toUpperCase()) {
                case "=":
                case "IN":
                    flag = values.contains(data);
                    break;
                case "LIKE":
                    flag = values.size() > 0 && data.matches(values.get(0).toString().replaceAll("%", "(.*)").replaceAll("_", "(.?)"));
                    break;
                default:
                    LOGGER.error("Not support Operator:{}", condition.getOperator());
                    return false;
            }
            // 目前只支持and
            if(!flag){
                return false;
            }
        }
        return true;
    }

    public static Map<String, Integer> toMapIndex(List<String> allColumn){
        Map<String, Integer> map = new HashMap<>();
        for(int i = 0; i < allColumn.size(); i++){
            map.put(allColumn.get(i), i);
        }
        return map;
    }

    public static List<String> getHeadder(Collection<TableStat.Column> columns, List<String> allColumn) {
        List<String> headerList = new ArrayList<>();
        for (TableStat.Column column : columns) {
            if(column.isSelect()){
                if("*".equals(column.getName())){
                    return allColumn;
                } else if (allColumn.contains(column.getName().toUpperCase())){
                    headerList.add(column.getName().toUpperCase());
                } else {
                    LOGGER.error("Column name error, please check, column:{}", column.getName());
                }
            }
        }
        return headerList;
    }
}