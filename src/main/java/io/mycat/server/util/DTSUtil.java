package io.mycat.server.util;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.MycatServer;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.util.RouterUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DTSUtil {


    /**
     * 修改sql，除了表名，还有limit 1的改为limit 0
     * 后续需要根据用户判断，特定用户配置的数据才可
     * @param user
     * @param schema
     * @param sql
     * @return
     */
    public static String changeSQLForDTS(String user,String schema, String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement statement = parser.parseStatement();

        if (statement instanceof SQLSelectStatement) {
            SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statement;
            SQLSelect select = sqlSelectStatement.getSelect();
            if (select != null && select.getQuery() instanceof MySqlSelectQueryBlock) {
                MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) select.getQuery();
                if (queryBlock.getFrom() instanceof SQLExprTableSource) {
                    SQLExprTableSource tableSource = (SQLExprTableSource) queryBlock.getFrom();
                    if (tableSource.getExpr() instanceof SQLPropertyExpr) {
                        SQLPropertyExpr expr = (SQLPropertyExpr) tableSource.getExpr();
                        String key = expr.getName();
                        //
                        boolean hasF = false;
                        if (key != null && key.contains("`")) {
                            hasF = true;
                            key = key.replaceAll("`", "");
                        }

                        boolean needReSQL =false;
                   MySqlSelectQueryBlock.Limit limit =     queryBlock.getLimit();
                        if(queryBlock.getSelectList().size()==1&&limit!=null && limit.getOffset()==null && limit.getRowCount() instanceof SQLIntegerExpr)
                        {
                            Number number= ((SQLIntegerExpr) limit.getRowCount()).getNumber();
                           String star= queryBlock.getSelectList().get(0).getExpr().toString();
                            if("*".equals(star)&&number.intValue() == 1 && !sql.contains("information_schema"))
                            {
                                limit.setRowCount(new SQLIntegerExpr(0));
                                needReSQL =true;
                            }
                        }

                        String table = getRealTableName(user,schema, key);
                        if (!key.equalsIgnoreCase(table)) {
                            if (hasF) {
                                table = "`" + table + "`";
                            }
                            expr.setName(table);
                            needReSQL =true;

                        }


                        if(needReSQL)
                        {
                            return statement.toString();
                        }
                    }
                }

            }

        } else if (statement instanceof MySqlReplaceStatement) {
            String routSQL = "insert " + sql.substring(sql.toLowerCase().indexOf("replace") + 7);
            return "/** mycat:sql='" + routSQL + "' */" + RouterUtil.removeSchema(sql, schema);
        }
//        else if (statement instanceof MySqlShowKeysStatement) {
//            MySqlShowKeysStatement sqlShowKey = (MySqlShowKeysStatement) statement;
//            if (sqlShowKey.getTable() instanceof SQLIdentifierExpr) {
//                SQLIdentifierExpr sqlIdExpr = (SQLIdentifierExpr) sqlShowKey.getTable();
//                String table = getRealTableName(user, schema, sqlIdExpr.getSimpleName());
//                sqlIdExpr.setName(table);
//                sqlShowKey.setTable(sqlIdExpr);
//                return statement.toString();
//            }
//        }
        else if (statement instanceof MySqlInsertStatement) {
            MySqlInsertStatement sqlInsertStatement = (MySqlInsertStatement) statement;
            if (!sqlInsertStatement.isIgnore()) {
                sqlInsertStatement.setIgnore(true);
                sql = sqlInsertStatement.toString();
            }

        }


        SchemaUtil.SchemaInfo info = SchemaUtil.parseTable(statement, new MycatSchemaStatVisitor());
        if (info != null) {
            String key = info.table;
            String sqlScema = info.schema;
            String table = getRealTableName(user, schema, key);
            if (!key.equalsIgnoreCase(table)) {

                if (sqlScema != null && sqlScema.contains(key)) {
                    int index = sql.replaceFirst(key, table).indexOf(key);
                    return sql.substring(0, index + 2) + table + sql.substring(index + key.length());
                }
                return sql.replaceFirst(key, table);
            }
        }


        return sql;
    }


    /**
     * 获取实际表名,但是返回给客户端的表面请使用原名
     *  判断规则，根据用户启用判断。
     *  1.schema中已有不过滤
     *  2.去除表的下划线的后缀
     *  3.dts_increment_trx会加上用户名的后缀
     * @param user
     * @param schema
     * @param table
     * @return
     */
    public static String getRealTableName(String user, String schema, String table) {
        if ("dts_increment_trx".equalsIgnoreCase(table)) {

            return table+"_"+user;
        }
        MycatConfig config = MycatServer.getInstance().getConfig();
        if (config != null && config.getSchemas() != null) {
            SchemaConfig schemaConfig = config.getSchemas().get(schema);
            if (schemaConfig != null) {
                if (schemaConfig.getTables().containsKey(table)) {
                    return table;
                }
            }
        }

        if (table.contains("_")) {
            int index = table.lastIndexOf("_");
            return table.substring(0, index);
        }

        return table;
    }

    public static void main(String[] args) {
        String sql = "select * from `informatio1n_schema`.columns_2_3 where table_schema = 'columns' and table_name = 'customers' limit 1 ";
        String rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);
        sql = "select COLUMN_NAME, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE from `base`.base where table_schema = 'columns' and table_name = 'customers' limit 1 ";
         rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);

        sql = "truncate table  `base_1_2`.base_1";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);
        sql = "truncate table  `base_1_2`.`base_1`";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);
        sql = "truncate table  `base`.base  ";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);
        sql = "show index from `base`.`customers_123`";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);
        sql = "delete from `base`.`customers_123`";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);

        sql = "SHOW CREATE TABLE `base`.`customers_1`";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);

        sql = "show index from `base`.`customers_1`";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);
        sql = "SHOW KEYS FROM customers_1";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);

        sql = " desc `base`.`customers_1`;";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);
        sql = "SHOW KEYS FROM `customers_12` FROM `db_6476_0000`";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);

        sql=" Replace into aaa";

        System.out.println("insert "+sql.substring(sql.toLowerCase().indexOf("replace")+7));

        sql =" insert into `base`.`customers_1` (`ADDRESS`,`NAME`,`ID`,`SALARY`,`AGE`) values(_utf8'917967122-439996636-619354915-313533621-953661020-077331597-009237497-559704742-906554219-982239722-\n" +
                "984796235-093609665-889699563-994489678-180301138-525652697-963755889-145864970-082034748-841301782-\n" +
                "430166514-980926150-063571981-163573020-430589437-342334045-320717004-477347844-870466217-621466421-\n" +
                "732137654-979380372-821656024-198600034-992838979-284877650-695469158-674375169-878773500-147702545-\n" +
                "305417651-508191771-047167123-574248529-071634950-691641588-499284811-435718511-416460167-413397669-\n" +
                "203140970-103596576-197725465-505300324-695009276-041569506-722705741-635116886-888790704-272005539-\n" +
                "364123493-234216130-841400599-038809230-954386056-059985856-210233622-907183660-694466254-884719915-\n" +
                "432654723-042932833-470969798-274159732-783094818-747617786-079498022-145919835-736565316-245518956-\n" +
                "976956589-128194366-851829638-344841709-172097663-560939894-316928399-872504702-337545603-401403211-\n" +
                "103031039-971226780-318116372-366631183-907558937-158491024-302185027-367779775-953700367-5',_utf8'665704673-931460038-467465372-003711583-067120351-667784392-745553709-703222415-289277414-489906405-\n" +
                "276016730-972357503-523681354-226611361-631128336-823108436-926410748-200045535-228459424-199035348-\n" +
                "716315209-997360003-660800213-488567755-739119701-822697458-475836792-735194383-674787161-566478283-\n" +
                "358258341-480729505-715004006-818254148-697176793-548609126-655288421-648219734-738000741-600157357-\n" +
                "493553237-482571639-910556274-160163269-710753445-258015649-731836871-706832097-908801521-337146771-\n" +
                "886248300-407349584-509960138-558824407-898232448-125104950-805564857-551496740-450724578-492312851-\n" +
                "530357728-709163518-486626213-484953632-159322755-464849869-897001579-930178372-714966300-345643174-\n" +
                "656936316-322418903-726967320-830076186-086645589-336952085-467560375-554287022-973889287-955851097-\n" +
                "206571186-583144029-108575883-179369094-508007849-260963872-177077963-320970987-845466122-353435572-\n" +
                "260197347-585925056-884420960-932281092-881835628-136202429-567264538-792863956-090072754-3',11217311,0.85,82);";
        rtn = changeSQLForDTS("s","base", sql);
        System.out.println(rtn);

    }


}
