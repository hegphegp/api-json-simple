package zuo.biao.apijson.parser.core;

import java.util.ArrayList;
import java.util.List;

public class SQLExplorer {
    private static final String AND = ") AND (";
    private static final String OR = ") OR (";
    private SQLStatement sql = new SQLStatement();
    private SQLProvider sqlProvider;

    public SQLExplorer(SQLProvider sqlProvider) {
        this.sqlProvider = sqlProvider;
    }

    public String getSQL() {
        feedSQLStatement();
//        //异常处理
//        if (!sqlProvider.getMessage().getErrors().isEmpty()) {
//            throw new SQLProviderException(sqlProvider.getMessage().getErrors());
//        }

        return sql.assemblySQL();
    }

    /*** 将提供器提供的数据刷新到SQLStatement中 */
    private void feedSQLStatement() {
        sql = new SQLStatement();
        if (sqlProvider.getStatementType() == null) {
            return;
        }

        sql.statementType = sqlProvider.getStatementType();                       /** StatementType表示要生成的是SELECT,INSERT,UPDATE,DLEETE中的哪种 */
        sql.updateFields.addAll(ofNullable(sqlProvider.getUpdateFields()));       /** StatementType为UPDATE时 UPDATE要更新的字段 */
        sql.selectFields.addAll(ofNullable(sqlProvider.getSelectFields()));       /** StatementType为SELECT时 SELECT要查询的字段 */
        sql.tables.addAll(ofNullable(sqlProvider.getTables()));                   /** StatementType所有类型都将使用这个方法 SQL所涉及的表 */
        sql.join.addAll(ofNullable(sqlProvider.getJoin()));
        sql.innerJoin.addAll(ofNullable(sqlProvider.getInnerJoin()));
        sql.outerJoin.addAll(ofNullable(sqlProvider.getOuterJoin()));
        sql.leftOuterJoin.addAll(ofNullable(sqlProvider.getLeftOuterJoin()));
        sql.rightOuterJoin.addAll(ofNullable(sqlProvider.getRightOuterJoin()));
        sql.where.addAll(ofNullable(sqlProvider.getWhere()));
        sql.having.addAll(ofNullable(sqlProvider.getHaving()));
        sql.groupBy.addAll(ofNullable(sqlProvider.getGroupBy()));
        sql.orderBy.addAll(ofNullable(sqlProvider.getOrderBy()));
        sql.lastList.addAll(ofNullable(sqlProvider.getLastList()));
        sql.columns.addAll(ofNullable(sqlProvider.getColumns()));
        sql.values.addAll(ofNullable(sqlProvider.getValues()));
    }

    private List<String> ofNullable(List<String> list) {
        if (list == null) {
            return new ArrayList();
        }
        return list;
    }

    private static class SQLStatement {

        private StatementType statementType;
        private List<String> updateFields = new ArrayList();
        private List<String> selectFields = new ArrayList();
        private List<String> tables = new ArrayList();
        private List<String> join = new ArrayList();
        private List<String> innerJoin = new ArrayList();
        private List<String> outerJoin = new ArrayList();
        private List<String> leftOuterJoin = new ArrayList();
        private List<String> rightOuterJoin = new ArrayList();
        private List<String> where = new ArrayList();
        private List<String> having = new ArrayList();
        private List<String> groupBy = new ArrayList();
        private List<String> orderBy = new ArrayList();
        private List<String> lastList = new ArrayList();
        private List<String> columns = new ArrayList();
        private List<String> values = new ArrayList();
        private boolean distinct;
        private StringBuffer builder = new StringBuffer();

        public SQLStatement() {
            // Prevent Synthetic Access
        }

        private void sqlClause(String keyword, List<String> parts, String open, String close, String conjunction) {
            if (!parts.isEmpty()) {
                builder.append("\n");
                builder.append(keyword);
                builder.append(" ");
                builder.append(open);
                String last = "________";
                for (int i = 0, n = parts.size(); i < n; i++) {
                    String part = parts.get(i);
                    if (i > 0 && !part.equals(AND) && !part.equals(OR) && !last.equals(AND) && !last.equals(OR)) {
                        builder.append(conjunction);
                    }
                    builder.append(part);
                    last = part;
                }
                builder.append(close);
            }
        }

        private String selectSQL() {
            if (distinct) {
                sqlClause("SELECT DISTINCT", selectFields, "", "", ", ");
            } else {
                sqlClause("SELECT", selectFields, "", "", ", ");
            }

            sqlClause("FROM", tables, "", "", ", ");
            joins();
            sqlClause("WHERE", where, "(", ")", " AND ");
            sqlClause("GROUP BY", groupBy, "", "", ", ");
            sqlClause("HAVING", having, "(", ")", " AND ");
            sqlClause("ORDER BY", orderBy, "", "", ", ");
            return builder.toString();
        }

        private void joins() {
            sqlClause("JOIN", join, "", "", "\nJOIN ");
            sqlClause("INNER JOIN", innerJoin, "", "", "\nINNER JOIN ");
            sqlClause("OUTER JOIN", outerJoin, "", "", "\nOUTER JOIN ");
            sqlClause("LEFT OUTER JOIN", leftOuterJoin, "", "", "\nLEFT OUTER JOIN ");
            sqlClause("RIGHT OUTER JOIN", rightOuterJoin, "", "", "\nRIGHT OUTER JOIN ");
        }

        private String insertSQL() {
            sqlClause("INSERT INTO", tables, "", "", "");
            sqlClause("", columns, "(", ")", ", ");
            sqlClause("VALUES", values, "(", ")", ", ");
            return builder.toString();
        }

        private String deleteSQL() {
            sqlClause("DELETE FROM", tables, "", "", "");
            sqlClause("WHERE", where, "(", ")", " AND ");
            return builder.toString();
        }

        private String updateSQL() {
            sqlClause("UPDATE", tables, "", "", "");
            joins();
            sqlClause("SET", updateFields, "", "", ", ");
            sqlClause("WHERE", where, "(", ")", " AND ");
            return builder.toString();
        }

        public String assemblySQL() {
            if (statementType == null) {
                return null;
            }

            switch (statementType) {
                case DELETE:
                    return deleteSQL();
                case INSERT:
                    return insertSQL();
                case SELECT:
                    return selectSQL();
                case UPDATE:
                    return updateSQL();
            }

            return null;
        }
    }
}
