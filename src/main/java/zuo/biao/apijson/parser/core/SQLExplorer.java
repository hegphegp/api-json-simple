package zuo.biao.apijson.parser.core;

import java.io.IOException;
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

    public String getSQL() throws SQLProviderException {
        feedSQLStatement();
        //异常处理
        if (!sqlProvider.getMessage().getErrors().isEmpty()) {
            throw new SQLProviderException(sqlProvider.getMessage().getErrors());
        }
        StringBuffer sb = new StringBuffer();
        sql.sql(sb);
        return sb.toString();
    }

    /***
     *  将提供器提供的数据刷新到SQLStatement中
     */
    private void feedSQLStatement() {
        sql = new SQLStatement();
        if (sqlProvider.getStatementType() == null) {
            return;
        }

        sql.statementType = sqlProvider.getStatementType();                    /** StatementType表示要生成的是SELECT,INSERT,UPDATE,DLEETE中的哪种 */
        sql.sets.addAll(ofNullable(sqlProvider.getSets()));                    /** StatementType为UPDATE时 UPDATE要更新的字段 */
        List<String> list = sqlProvider.getSelect();
        sql.select.addAll(ofNullable(sqlProvider.getSelect()));                /** StatementType为SELECT时 SELECT要查询的字段 */
        sql.tables.addAll(ofNullable(sqlProvider.getTables()));                /** StatementType所有类型都将使用这个方法 SQL所涉及的表 */
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

    public SQLProvider getSqlProvider() {
        return sqlProvider;
    }

    public void setSqlProvider(SQLProvider sqlProvider) {
        this.sqlProvider = sqlProvider;
    }

    private List<String> ofNullable(List<String> list) {
        if (list == null) {
            return new ArrayList();
        }
        return list;
    }

    private static class SafeAppendable {
        private final Appendable a;
        private boolean empty = true;

        public SafeAppendable(Appendable a) {
            super();
            this.a = a;
        }

        public SafeAppendable append(CharSequence s) {
            try {
                if (empty && s.length() > 0) {
                    empty = false;
                }
                a.append(s);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public boolean isEmpty() {
            return empty;
        }

    }

    private static class SQLStatement {

        private StatementType statementType;
        private List<String> sets = new ArrayList();
        private List<String> select = new ArrayList();
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

        public SQLStatement() {
            // Prevent Synthetic Access
        }

        private void sqlClause(SafeAppendable builder, String keyword, List<String> parts, String open, String close, String conjunction) {
            if (!parts.isEmpty()) {
                if (!builder.isEmpty()) {
                    builder.append("\n");
                }
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

        private String selectSQL(SafeAppendable builder) {
            if (distinct) {
                sqlClause(builder, "SELECT DISTINCT", select, "", "", ", ");
            } else {
                sqlClause(builder, "SELECT", select, "", "", ", ");
            }

            sqlClause(builder, "FROM", tables, "", "", ", ");
            joins(builder);
            sqlClause(builder, "WHERE", where, "(", ")", " AND ");
            sqlClause(builder, "GROUP BY", groupBy, "", "", ", ");
            sqlClause(builder, "HAVING", having, "(", ")", " AND ");
            sqlClause(builder, "ORDER BY", orderBy, "", "", ", ");
            return builder.toString();
        }

        private void joins(SafeAppendable builder) {
            sqlClause(builder, "JOIN", join, "", "", "\nJOIN ");
            sqlClause(builder, "INNER JOIN", innerJoin, "", "", "\nINNER JOIN ");
            sqlClause(builder, "OUTER JOIN", outerJoin, "", "", "\nOUTER JOIN ");
            sqlClause(builder, "LEFT OUTER JOIN", leftOuterJoin, "", "", "\nLEFT OUTER JOIN ");
            sqlClause(builder, "RIGHT OUTER JOIN", rightOuterJoin, "", "", "\nRIGHT OUTER JOIN ");
        }

        private String insertSQL(SafeAppendable builder) {
            sqlClause(builder, "INSERT INTO", tables, "", "", "");
            sqlClause(builder, "", columns, "(", ")", ", ");
            sqlClause(builder, "VALUES", values, "(", ")", ", ");
            return builder.toString();
        }

        private String deleteSQL(SafeAppendable builder) {
            sqlClause(builder, "DELETE FROM", tables, "", "", "");
            sqlClause(builder, "WHERE", where, "(", ")", " AND ");
            return builder.toString();
        }

        private String updateSQL(SafeAppendable builder) {
            sqlClause(builder, "UPDATE", tables, "", "", "");
            joins(builder);
            sqlClause(builder, "SET", sets, "", "", ", ");
            sqlClause(builder, "WHERE", where, "(", ")", " AND ");
            return builder.toString();
        }

        public String sql(Appendable a) {
            SafeAppendable builder = new SafeAppendable(a);
            if (statementType == null) {
                return null;
            }

            String answer;

            switch (statementType) {
                case DELETE:
                    answer = deleteSQL(builder);
                    break;

                case INSERT:
                    answer = insertSQL(builder);
                    break;

                case SELECT:
                    answer = selectSQL(builder);
                    break;

                case UPDATE:
                    answer = updateSQL(builder);
                    break;

                default:
                    answer = null;
            }

            return answer;
        }
    }
}
