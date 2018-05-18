package io.mycat.server.handler;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.MycatServer;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.util.RouterUtil;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MysqlInformationSchemaColumnsHandler {

    private static final Logger logger = LoggerFactory.getLogger(MysqlInformationSchemaColumnsHandler.class);
    private static final String TABLE_SCHEMA_FIELD = "table_schema";
    private static final String TABLE_SCHEMA_FAKED_VALUE = "__faked_table_schema__";

    //是否改写了sql
    private boolean isChangeSelectInfomationSchemSql = false;

    //sql中是否指定table_schema条件
    private boolean isTableSchemaCondition = false;

    private String stmt;  //sql
    private String user;
    private RouteResultset rrs;
    private Set<String> tableSchemaValues = new HashSet<>();

    public MysqlInformationSchemaColumnsHandler(String user, String stmt, RouteResultset rrs) {
        this.user = user;
        this.stmt = stmt;
        this.rrs = rrs;
    }

    private void changeTableSchemaConditionValue(SQLCharExpr value) {
        MycatConfig config = MycatServer.getInstance().getConfig();
        Map<String, SchemaConfig> schemas = config.getSchemas();
        String schemaValue = value.getText();

        rrs.setTableSchemaConditionValue(schemaValue);

        //不支持table_schema指定多个条件值
        tableSchemaValues.add(schemaValue.toLowerCase(Locale.ENGLISH));
        if (tableSchemaValues.size() > 1) {
            String err = "table_schema condition illegal.";
            logger.error(err);
            throw new RuntimeException(err);
        }

        SchemaConfig schema = schemas.get(schemaValue);
        if (schema != null) {
            //找到默认节点对应的物理分片库名
            String defautNode = schema.getDataNode();
            //如果默认节点和rrs已有节点不一样，则修改
            if (rrs.getNodes() == null
                    || (rrs.getNodes().length > 0 && !rrs.getNodes()[0].getName().equals(defautNode))){
                rrs = RouterUtil.routeToSingleNode(rrs, defautNode, stmt);
            }

            String database = config.getDataNodes().get(defautNode).getDatabase();

            //因为是发到默认分片执行，所有table_schema要取默认分片的物理库名，如果不一样，要修改
            if (database != null && !database.equalsIgnoreCase(schemaValue)) {
                if (config.checkSchemaSelectPrivilege(schemaValue, user)) {
                    value.setText(database);
                } else {
                    //没权限的逻辑库，设置为一个特殊值，返回空结果集
                    value.setText(TABLE_SCHEMA_FAKED_VALUE);
                }
                isChangeSelectInfomationSchemSql = true;

            } else {
                if (!config.checkSchemaSelectPrivilege(schemaValue, user)) {
                    //没权限的逻辑库，设置为一个特殊值，返回空结果集
                    value.setText(TABLE_SCHEMA_FAKED_VALUE);
                    isChangeSelectInfomationSchemSql = true;
                }
            }
        } else {
            //查不到的逻辑库，设置为一个特殊值，返回空结果集
            value.setText(TABLE_SCHEMA_FAKED_VALUE);
            isChangeSelectInfomationSchemSql = true;
        }
    }

    private void handleTableSchemaCondition(SQLInListExpr schemaInExpr) {
        SQLExpr schemaInExprLeft = schemaInExpr.getExpr();
        if (schemaInExprLeft instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr schemaIdentifyExpr = (SQLIdentifierExpr)schemaInExprLeft;

            if (schemaIdentifyExpr.getName().equalsIgnoreCase(TABLE_SCHEMA_FIELD)) {
                List<SQLExpr> valueList = schemaInExpr.getTargetList();
                if (!CollectionUtils.isEmpty(valueList)) {
                    for (SQLExpr expr : valueList) {
                        //不支持 not in
                        if (schemaInExpr.isNot()) {
                            String err = "table_schema condition illegal.";
                            logger.error(err);
                            throw new RuntimeException(err);
                        }
                        if (expr instanceof SQLCharExpr) {
                            changeTableSchemaConditionValue((SQLCharExpr)expr);
                        }
                    }
                }
                isTableSchemaCondition = true;
            }
        }
    }

    private void handleTableSchemaCondition(SQLIdentifierExpr schemaIdentifyExpr, SQLCharExpr value, SQLBinaryOperator operator) {
        if (schemaIdentifyExpr.getName().equalsIgnoreCase(TABLE_SCHEMA_FIELD)) {
            if (!operator.getName().equalsIgnoreCase("=")) {
                String err = "table_schema condition illegal.";
                logger.error(err);
                throw new RuntimeException(err);
            }

            changeTableSchemaConditionValue(value);
            isTableSchemaCondition = true;
        }
    }

    private void handleWhereCondition(SQLExpr whereExpr) {
        if (whereExpr instanceof SQLBinaryOpExpr) {
            SQLExpr left = ((SQLBinaryOpExpr) whereExpr).getLeft();
            SQLExpr right = ((SQLBinaryOpExpr) whereExpr).getRight();
            SQLBinaryOperator opertator = ((SQLBinaryOpExpr) whereExpr).getOperator();
            if ((left instanceof SQLIdentifierExpr) && (right instanceof SQLCharExpr)) {
                handleTableSchemaCondition((SQLIdentifierExpr)left, (SQLCharExpr)right, opertator);
            } else if ((right instanceof SQLIdentifierExpr) && (left instanceof SQLCharExpr)) {
                handleTableSchemaCondition((SQLIdentifierExpr) right, (SQLCharExpr)left, opertator);
            }

            //支持 table_schema =、in比较
            if (left instanceof SQLBinaryOpExpr || left instanceof SQLInListExpr) {
                handleWhereCondition(((SQLBinaryOpExpr) whereExpr).getLeft());
            }

            if (right instanceof SQLBinaryOpExpr || right instanceof SQLInListExpr) {
                handleWhereCondition(((SQLBinaryOpExpr) whereExpr).getRight());
            }

            //如果是子查询，递归处理
            if (right instanceof SQLInSubQueryExpr) {
                SQLSelectQuery query = ((SQLInSubQueryExpr) right).getSubQuery().getQuery();
                handleSelectQuery((MySqlSelectQueryBlock)query);
            }
        } else if (whereExpr instanceof SQLInListExpr) {
            handleTableSchemaCondition((SQLInListExpr)whereExpr);
        }
    }

    private void handleSelectQuery(MySqlSelectQueryBlock mysqlSelectQuery) {
        SQLExpr whereExpr = mysqlSelectQuery.getWhere();
        handleWhereCondition(whereExpr);
    }

    public void handle() {

        if (!stmt.toLowerCase(Locale.ENGLISH).contains(TABLE_SCHEMA_FIELD)) {
            String err = "table_schema condition not specified.";
            logger.error(err);
            throw new RuntimeException(err);
        }

        SQLStatementParser parser = new MySqlStatementParser(stmt);
        SQLStatement statement;

        try {
            statement = parser.parseStatement();
        } catch (Exception e) {
            logger.error("MysqlInformationSchemaColumnsHandler parse sql error:{}", e);
            throw new RuntimeException("parse sql error");
        }

        if (statement instanceof SQLSelectStatement) {
            SQLSelectStatement selectStmt = (SQLSelectStatement)statement;
            SQLSelectQuery sqlSelectQuery = selectStmt.getSelect().getQuery();
            if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
                MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock) selectStmt.getSelect().getQuery();

                //改写SQL
                handleSelectQuery(mysqlSelectQuery);

                //如果没有指定table_schema条件，就用前端连接的schema做条件
                if (!isTableSchemaCondition) {
                    String err = "table_schema condition not specified.";
                    throw new RuntimeException(err);
                }

                if (isChangeSelectInfomationSchemSql) {
                    String sqlSelectInformation = statement.toString();
                    rrs.changeNodeSql(sqlSelectInformation);
                }
            }
        }
    }
}
