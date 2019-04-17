package zuo.biao.apijson.parser.core;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class APIJSONProvider extends SQLProvider {

    private final String ALIAS_SPLIT = ":";
    private static final String AND = " AND ";
    private static final String OR = " OR ";
    private boolean isWrap = false;

    public Set<String> tableWhiteSet = new HashSet();
    public Set<String> tableBlackSet = new HashSet();
    public Map<String,Set<String>> columnWhiteMap = new HashMap();
    public Map<String,Set<String>> columnBlackMap = new HashMap();

    private Set<String> tableNames;
    private JSONObject request;
    private JSONObject join;

    /** 传入的参数应该是一个通过验证的APIJSON请求 */
    public APIJSONProvider(JSONObject obj) {
        if (obj == null) {
            throw new RuntimeException("APIJSONProvider传入的请求不能为空");
        }
        JSONObject tabs = obj.getJSONObject("[]");
        this.request = tabs!=null? tabs:obj;
        this.join = tabs!=null? obj.getJSONObject("join"):new JSONObject();
        tableNames = this.request.keySet();
        for (String tableName : tableNames) {
            if (!tableName.matches("(\\w+(:\\w+)?)")) {
                throw new RuntimeException("表" + tableName + "格式不符合");
            }
        }
    }

    private String[] checkJoin = {"@innerJoin", "@leftOuterJoin", "@rightOuterJoin", "@join", "@outerJoin"};
    /**
     * 解析请求中的表名
     * 表名必须符合：(\w+(:\w+)?)
     * 即：
     * 表名
     * 表名:表别名
     * 两种形式
     */
    @Override
    public List<String> getTables() {
        List<String> list = new ArrayList<>();
        for (String tableName : tableNames) {
            //是否有自定义别名
            String tableRealName  = tableName.contains(ALIAS_SPLIT)? tableName.split(ALIAS_SPLIT)[0]:tableName;
            String tableAliasName = tableName.contains(ALIAS_SPLIT)? tableName.split(ALIAS_SPLIT)[1]:tableName;
            validateTable(tableRealName);
            if (getStatementType() == StatementType.SELECT) {
                list.add(tableRealName + " " + tableAliasName);
            } else {
                list.add(tableRealName);
            }
        }
        for (String str:list.stream().collect(Collectors.toSet())) {
            String[] arr = str.split(" ");
            for (String joinKey:checkJoin) {
                JSONArray array = join.getJSONArray(joinKey);
                array = array!=null? array:new JSONArray();
                for (Object obj:array) {
                    String joinTableName = obj instanceof String ? (String) obj:"";
                    if (joinTableName.startsWith(arr[0]) || joinTableName.startsWith(arr[1])) {
                        list.remove(str);
                    }
                }
            }
        }
        return list;
    }

    /*
     * ==================================
     *              查询逻辑
     * ==================================
     */
    /**
     * 解析请求中的字段
     * 路径：/表名/@column
     * 同时为字段设置好引用的表别名，如: p.id
     * 表名必须符合：(\w+(:\w+)?)
     * 字段值必须符合：
     * 不支持函数的正则：(\w+(:\w+)?)+(\s?,\s?(\w+(:\w+)?)+)*
     * 支持函数的正则：((\w+\(\w+\):\w+|\w+)(:\w+)?)+(\s?,\s?((\w+\(\w+\):\w+|\w+)(:\w+)?)+)*
     * 例：a,b,c或a:a1,b:b1,c
     * 约束：必须要有表名
     * 目前支持函数
     */
    @Override
    public List<String> getSelectFields() {
        List<String> list = new ArrayList();
        if (getStatementType() == StatementType.SELECT) {
            Set<String> tableNames = request.keySet();
            for (String tableName:tableNames) {
                //是否有自定义别名
                String tableRealName  = tableName.contains(ALIAS_SPLIT)? tableName.split(ALIAS_SPLIT)[0]:tableName;
                String tableAliasName = tableName.contains(ALIAS_SPLIT)? tableName.split(ALIAS_SPLIT)[1]:tableName;
                // 获取请求@column的值
                JSONObject propertis = request.getJSONObject(tableName);
                String columnsValue = propertis.getString("@column");

                if (columnsValue == null) {
                    validateColumn(tableRealName, "*");
                    //没有填写@column字段，默认为全部
                    list.add(tableAliasName + ".*");
                    continue;
                }
                if (!columnsValue.matches("((\\w+\\(\\w+\\):\\w+|\\w+)(:\\w+)?)+(\\s?,\\s?((\\w+\\(\\w+\\):\\w+|\\w+)(:\\w+)?)+)*")) {
                    throw new RuntimeException("字段@column：" + columnsValue + "格式不符合，正确请求如：a,b,c:d或者a,max(d):d,c:e");
                }
                //填写了，则返回tableAliasName.colName或tableAliasName.columnName as columnAliasName
                String[] columnNames = columnsValue.replaceAll("\\s", "").split(",");
                for (String columnName:columnNames) {
                    if (columnName.contains(ALIAS_SPLIT)) {
                        //填写了字段别名，使用：tableAliasName.columnName as columnAliasName这种类型
                        //这里columnRealName有两种情况，如：id或max(id)
                        String functionOrColumn = columnName.split(ALIAS_SPLIT)[0];
                        if (functionOrColumn.contains("(")) {
                            //有函数的字段
                            //去掉)
                            functionOrColumn = functionOrColumn.replaceAll("\\)", "");
                            String[] functionStrs = functionOrColumn.split("\\(");
                            String funcitonName = functionStrs[0];
                            //要对函数进行控制这这里进行
                            //此处省略函数合法性检查的代码...
                            String columnRealName = functionStrs[1];
                            String columnAliasName = columnName.split(ALIAS_SPLIT)[1];
                            validateColumn(tableRealName, columnRealName);
                            if (propertis.getString("id@") != null)
                                columnAliasName = wrapColumn(tableAliasName, columnAliasName);
                            list.add(funcitonName + "(" + tableAliasName + "." + columnRealName + ")" + " as " + columnAliasName);
                        } else {
                            //没函数的字段
                            String columnRealName = functionOrColumn;
                            String columnAliasName = columnName.split(ALIAS_SPLIT)[1];
                            validateColumn(tableRealName, columnRealName);
                            if (propertis.getString("id@") != null) {
                                columnAliasName = wrapColumn(tableAliasName, columnAliasName);
                            }
                            list.add(tableAliasName + "." + columnRealName + " as " + columnAliasName);
                        }
                    } else {
                        //使用tableAliasName.columnName类型
                        validateColumn(tableRealName, columnName);
                        if (propertis.getString("id@") != null) {
                            String columnAliasName = wrapColumn(tableAliasName, columnName);
                            list.add(tableAliasName + "." + columnName + " as " + columnAliasName);
                        } else {
                            list.add(tableAliasName + "." + columnName);
                        }
                    }
                }
            }
        }
        return list;
    }

    private String wrapColumn(String tableAliasName, String columnAliasName) {
        return isWrap ? "\"" + tableAliasName + "." + columnAliasName + "\"" : "\"" + columnAliasName + "\"";
    }

    /**
     * 解析请求中的过滤条件
     * 支持以下模式：
     * 单值："id" : "12" 即id=12
     * 多值："id&{}" : ">12,<30" 即 id > 12 AND id < 30
     * 多值："id|{}" : ">12,<30" 即 id > 12 OR id < 30
     * 多值："id{}" : [1,2,3] 即 id IN (1,2,3)
     * 多值："id!{}" : [1,2,3] 即 id NOT IN (1,2,3)
     * 模糊："content~":"keyword" 即字段content包含字符串keyword
     * 模糊："content$":"%keyword" 同样是模糊查询，%放出来自己操控
     * 正则："content?":"^[0-9]+$" 后面填写正则即可
     * 外键："id@":"/外键表/外键字段"
     */
    @Override
    public List<String> getWhere() {
        List<String> list = new ArrayList<>();
        Set<String> tableNames = request.keySet();
        for (String tableName : tableNames) {
            //是否有自定义别名
            String tableAliasName = tableName.contains(ALIAS_SPLIT)? tableName.split(ALIAS_SPLIT)[1]:tableName;
            // 遍历过滤条件
            JSONObject propertis = request.getJSONObject(tableName);
            conditionLoop:
            for (String condition:propertis.keySet()) {
                //关键字则跳过
                if (condition.startsWith("@")) {
                    continue conditionLoop;
                }
                if (condition.matches("\\w+")) {
                    //纯字段名
                    if (propertis.get(condition) instanceof Integer ||
                            propertis.get(condition) instanceof Float ||
                            propertis.get(condition) instanceof Double ||
                            propertis.get(condition) instanceof BigDecimal) {
                        list.add(tableAliasName + "." + condition + " = " + propertis.get(condition));
                    } else if (propertis.get(condition) instanceof String) {
                        list.add(tableAliasName + "." + condition + " = '" + ((String) propertis.get(condition)).replaceAll("'", "''") + "'");
                    }
                } else if (condition.matches("(\\w+(!|&|\\|)?\\{\\})+")) {
                    //表示这是一个多条件类型
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        //是否匹配这种类型：<20.3,>3.3,=3.3
                        if (!exp.matches("(\\s?(>|<|>=|<=|=|<>)+\\s?((\\-|\\+)?\\d+(\\.\\d+)?)+)+(\\s?,\\s?((>|<|>=|<=|=|<>)+\\s?((\\-|\\+)?\\d+(\\.\\d+)?)))*")) {
                            throw new RuntimeException(condition + "的格式不正确，正确使用方式如: >10,<20");
                        }
                        if (condition.endsWith("|{}")) {
                            String[] terms = exp.replaceAll("\\s", "").split(",");
                            String columnName = condition.replaceAll("\\|\\{\\}", "");
                            assemblySQLOrAnd(list, OR, tableAliasName, columnName, terms);
                        } else {
                            getLastList().add(AND);
                            String[] terms = exp.replaceAll("\\s", "").split(",");
                            String columnName = condition.replaceAll("\\{\\}", "").replaceAll("&", "").replaceAll("!", "");
                            assemblySQLOrAnd(list, AND, tableAliasName, columnName, terms);
                        }
                    } else if (propertis.get(condition) instanceof JSONArray) {
                        JSONArray array = (JSONArray) propertis.get(condition);
                        //分离出字段名
                        String columnName = condition.replaceAll("\\{\\}", "").replaceAll("&", "").replaceAll("!", "");
                        if (condition.endsWith("!{}")) {
                            assemblyArrConditions(list, array, tableAliasName, columnName, false);
                        } else {
                            assemblyArrConditions(list, array, tableAliasName, columnName, true);
                        }
                    }
                } else if (condition.matches("\\w+~")) {
                    //字符串查询，包含
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        String columnName = condition.replaceAll("~", "");
                        list.add(tableAliasName + "." + columnName + " LIKE '%" + exp.replaceAll("'", "''") + "%'");
                    } else {
                        throw new RuntimeException(condition + "的值必须要是字符串");
                    }
                } else if (condition.matches("\\w+\\$")) {
                    //字符串查询，LIKE
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        String columnName = condition.replaceAll("\\$", "");
                        list.add(tableAliasName + "." + columnName + " LIKE '" + exp.replaceAll("'", "''") + "'");
                    } else {
                        throw new RuntimeException(condition + "的值必须要是字符串");
                    }
                } else if (condition.matches("\\w+\\?")) {
                    //字符串查询，正则
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        String columnName = condition.replaceAll("\\?", "");
                        list.add(" regexp_like(" + tableAliasName + "." + columnName + ",'" + exp.replaceAll("'", "''") + "')");
                    } else {
                        throw new RuntimeException(condition + "的值必须要是字符串");
                    }
                } else if (condition.matches("\\w+@")) {
                    //内连接
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        String columnName = condition.replaceAll("@", "");
                        if (exp.matches("/\\w+/\\w+")) {
                            String[] args = exp.split("/");
                            String refTable = args[1];
                            String refColumn = args[2];
                            list.add(refTable + "." + refColumn + " = " + tableAliasName + "." + columnName);
                        } else {
                            throw new RuntimeException(condition + "必须符合：\"/表名或别名/字段名\"的形式");
                        }
                    } else {
                        throw new RuntimeException(condition + "的值必须要是字符串");
                    }
                }
            }
        }
        return list;
    }

    public void assemblyArrConditions(List<String> list, JSONArray array, String tableAliasName, String columnName, boolean isIn) {
        String limit = "";
        if (!array.isEmpty()) {
            for (int i = 0; i < array.size(); i++) {
                Object obj = array.get(i);
                if (i != 0) {
                    limit += ", ";
                }
                if (obj instanceof Integer || obj instanceof Float || obj instanceof Double || obj instanceof BigDecimal) {
                    limit += obj;
                } else if (obj instanceof String) {
                    limit += "'" + ((String) obj).replaceAll("'", "''") + "'";
                }
            }
            list.add(tableAliasName + "." + columnName + (isIn==false?" NOT ":" ")+"IN [" + limit + "]");
        }
    }

    public void assemblySQLOrAnd(List<String> list, String Operation, String tableAliasName, String columnName, String[] terms) {
        String limit = "";
        for (int i = 0; i < terms.length; i++) {
            String term = terms[i];
            if (term.startsWith(">=")) {
                limit += tableAliasName + "." + columnName + " >= " + term.replaceAll(">=", "");
            } else if (term.startsWith("<=")) {
                limit += tableAliasName + "." + columnName + " <= " + term.replaceAll("<=", "");
            } else if (term.startsWith(">")) {
                limit += tableAliasName + "." + columnName + " > " + term.replaceAll(">", "");
            } else if (term.startsWith("<")) {
                limit += tableAliasName + "." + columnName + " < " + term.replaceAll("<", "");
            } else if (term.startsWith("<>") || term.startsWith("!=")) {
                limit += tableAliasName + "." + columnName + " <> " + term.replaceAll("<>", "").replaceAll("!=", "");
            } else if (term.startsWith("=")) {
                limit += tableAliasName + "." + columnName + " = " + term.replaceAll("=", "");
            }
            if (terms.length > 1 && (i != (terms.length - 1)))
                limit += Operation;
        }
        list.add(limit);
    }

    /**
     * 内连接
     * 请求："@innerJoin" : ["table1.column1 = table2.column2","table1.column1 = table2.column2"]
     * 编译之后：INNER JOIN table1 ON table1.column1=table2.column2
     */
    @Override
    public List<String> getInnerJoin() {
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.SELECT) {
            if (join != null && join.get("@innerJoin") != null) {
                JSONArray stms = join.getJSONArray("@innerJoin");
                for (int i = 0; i < stms.size(); i++) {
                    Object obj = stms.get(i);
                    if (obj instanceof String) {
                        String joinStr = (String) obj;
                        if (joinStr.matches("\\w+\\.\\w+\\s?=\\s?\\w+\\.\\w+")) {
                            //table1.column1 = table2.column2
                            String[] tcs = joinStr.replaceAll("\\s", "").split("=");
//                            String leftTable = tcs[0].split("\\.")[0];
//                            String rightTable = tcs[1].split("\\.")[0];
                            validateTable(tcs[0].split("\\.")[0]);
                            list.add(tcs[0].split("\\.")[0] + " ON " + joinStr);
                        } else {
                            throw new RuntimeException("@innerJoin的格式必须是：table1.column1 = table2.column2,相当于INNER JOIN table1 ON table1.column1=table2.column2");
                        }
                    } else {
                        throw new RuntimeException("@innerJoin的类型必须是String类型，填写的值如：table1.column1 = table2.column2,相当于INNER JOIN table1 ON table1.column1=table2.column2。注意：表有别名的应该写表别名");
                    }
                }
            }
        }
        return list;
    }

    /** 左外连接 */
    @Override
    public List<String> getLeftOuterJoin() {
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.SELECT) {
            JSONArray stms = join.get("@leftOuterJoin")!= null? join.getJSONArray("@leftOuterJoin"):new JSONArray();
            for (int i = 0; i < stms.size(); i++) {
                if (stms.get(i) instanceof String) {
                    String joinStr = (String) stms.get(i);
                    if (joinStr.matches("\\w+\\.\\w+\\s?=\\s?\\w+\\.\\w+")) {
                        //table1.column1 = table2.column2
                        String[] tcs = joinStr.replaceAll("\\s", "").split("=");
//                            String leftTable = tcs[0].split("\\.")[0];
//                            String rightTable = tcs[1].split("\\.")[0];
                        validateTable(tcs[0].split("\\.")[0]);
                        list.add(tcs[0].split("\\.")[0] + " ON " + joinStr);
                    } else {
                        throw new RuntimeException("@leftOuterJoin的格式必须是：table1.column1 = table2.column2,相当于LEFT OUTER JOIN table1 ON table1.column1=table2.column2");
                    }
                } else {
                    throw new RuntimeException("@leftOuterJoin的类型必须是String类型，填写的值如：table1.column1 = table2.column2,相当于LEFT OUTER JOIN table1 ON table1.column1=table2.column2。注意：表有别名的应该写表别名");
                }
            }
        }
        return list;
    }

    /** 右外链接 */
    @Override
    public List<String> getRightOuterJoin() {
        // TODO Auto-generated method stub
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.SELECT) {
            JSONArray stms = join.get("@rightOuterJoinv")!= null? join.getJSONArray("@rightOuterJoin"):new JSONArray();
            for (int i = 0; i < stms.size(); i++) {
                Object obj = stms.get(i);
                if (obj instanceof String) {
                    String joinStr = (String) obj;
                    if (joinStr.matches("\\w+\\.\\w+\\s?=\\s?\\w+\\.\\w+")) {
                        //table1.column1 = table2.column2
                        String[] tcs = joinStr.replaceAll("\\s", "").split("=");
//                        String leftTable = tcs[0].split("\\.")[0];
//                        String rightTable = tcs[1].split("\\.")[0];
                        validateTable(tcs[0].split("\\.")[0]);
                        list.add(tcs[0].split("\\.")[0] + " ON " + joinStr);
                    } else {
                        throw new RuntimeException("@rightOuterJoin的格式必须是：table1.column1 = table2.column2,相当于RIGHT OUTER JOIN table1 ON table1.column1=table2.column2");
                    }
                } else {
                    throw new RuntimeException("@rightOuterJoin的类型必须是String类型，填写的值如：table1.column1 = table2.column2,相当于RIGHT OUTER JOIN table1 ON table1.column1=table2.column2。注意：表有别名的应该写表别名");
                }
            }
        }
        return list;
    }

    /** join连接 */
    @Override
    public List<String> getJoin() {
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.SELECT) {
            if (join != null && join.get("@join") != null) {
                JSONArray stms = join.getJSONArray("@join");
                for (int i = 0; i < stms.size(); i++) {
                    Object obj = stms.get(i);
                    if (obj instanceof String) {
                        String joinStr = (String) obj;
                        if (joinStr.matches("\\w+\\.\\w+\\s?=\\s?\\w+\\.\\w+")) {
                            //table1.column1 = table2.column2
                            String[] tcs = joinStr.replaceAll("\\s", "").split("=");
//                            String leftTable = tcs[0].split("\\.")[0];
//                            String rightTable = tcs[1].split("\\.")[0];
                            validateTable(tcs[0].split("\\.")[0]);
                            list.add(tcs[0].split("\\.")[0] + " ON " + joinStr);
                        } else {
                            throw new RuntimeException("@join的格式必须是：table1.column1 = table2.column2,相当于JOIN table1 ON table1.column1=table2.column2");
                        }
                    } else {
                        throw new RuntimeException("@join的类型必须是String类型，填写的值如：table1.column1 = table2.column2,相当于JOIN table1 ON table1.column1=table2.column2。注意：表有别名的应该写表别名");
                    }
                }
            }
        }
        return list;
    }

    /** 外连接 */
    @Override
    public List<String> getOuterJoin() {
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.SELECT) {
            if (join != null && join.get("@outerJoin") != null) {
                JSONArray stms = join.getJSONArray("@outerJoin");
                for (int i = 0; i < stms.size(); i++) {
                    Object obj = stms.get(i);
                    if (obj instanceof String) {
                        String joinStr = (String) obj;
                        if (joinStr.matches("\\w+\\.\\w+\\s?=\\s?\\w+\\.\\w+")) {
                            //table1.column1 = table2.column2
                            String[] tcs = joinStr.replaceAll("\\s", "").split("=");
//                            String leftTable = tcs[0].split("\\.")[0];
//                            String rightTable = tcs[1].split("\\.")[0];
                            validateTable(tcs[0].split("\\.")[0]);
                            list.add(tcs[0].split("\\.")[0] + " ON " + joinStr);
                        } else {
                            throw new RuntimeException("@outerJoin的格式必须是：table1.column1 = table2.column2,相当于OUTER JOIN table1 ON table1.column1=table2.column2");
                        }
                    } else {
                        throw new RuntimeException("@outerJoin的类型必须是String类型，填写的值如：table1.column1 = table2.column2,相当于OUTER JOIN table1 ON table1.column1=table2.column2。注意：表有别名的应该写表别名");
                    }
                }
            }
        }
        return list;
    }

    /** 解析分组 "@group":"store_id" */
    @Override
    public List<String> getGroupBy() {
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.SELECT) {
            Set<String> tableNames = request.keySet();
            for (String tableName : tableNames) {
                //格式检查
                if (!tableName.matches("(\\w+(:\\w+)?)")) {
                    throw new RuntimeException("表" + tableName + "格式不符合");
                }
                //是否有自定义别名
                String tableAliasName = tableName.contains(ALIAS_SPLIT)? tableName.split(ALIAS_SPLIT)[1]:tableName;
                // 获取请求@group的值
                JSONObject propertis = request.getJSONObject(tableName);
                String columnsValue = propertis.getString("@group");
                if (columnsValue != null) {
                    if (!columnsValue.matches("\\w+(\\s?,\\s?\\w+)*")) {
                        throw new RuntimeException("字段@group：" + columnsValue + "格式不符合，正确请求如：a,b,c");
                    }
                    String[] columnNames = columnsValue.replaceAll("\\s", "").split(",");
                    for (String colmunName : columnNames) {
                        list.add(tableAliasName + "." + colmunName);
                    }
                }

            }
        }
        return list;
    }

    /** 解析排序逻辑 column1+,column2-,+表示升序，-表示降序 */
    @Override
    public List<String> getOrderBy() {
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.SELECT) {
            Set<String> tableNames = request.keySet();
            for (String tableName : tableNames) {
                //格式检查
                if (!tableName.matches("(\\w+(:\\w+)?)")) {
                    throw new RuntimeException("表" + tableName + "格式不符合");
                }
                //是否有自定义别名
                String tableAliasName = tableName.contains(ALIAS_SPLIT)? tableName.split(ALIAS_SPLIT)[1]:tableName;
                // 获取请求@column的值
                JSONObject propertis = request.getJSONObject(tableName);
                String columnsValue = propertis.getString("@orders");
                if (columnsValue != null) {
                    if (!columnsValue.matches("(\\w+(\\+|\\-)?)+(\\s?,\\s?(\\w+(\\+|\\-)?)+)*")) {
                        throw new RuntimeException("字段@orders：" + columnsValue + "格式不符合，正确格式如：column1+,column2-,+表示升序，-表示降序。");
                    }
                    //没有填写@orders字段，默认为全部
                    String[] columnNames = columnsValue.replaceAll("\\s", "").split(",");
                    for (String columnName : columnNames) {
                        if (columnName.endsWith("+")) {
                            list.add(tableAliasName + "." + columnName.replaceAll("\\+", "") + " ASC");
                        } else if (columnName.endsWith("-")) {
                            list.add(tableAliasName + "." + columnName.replaceAll("\\-", "") + " DESC");
                        }
                    }
                }
            }
        }
        return list;
    }

    /*
     * ==================================
     *              新增逻辑
     * ==================================
     */
    @Override
    public List<String> getColumns() {
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.INSERT) {
            Set<String> tableNames = request.keySet();
            if (tableNames.size() != 1) {
                throw new RuntimeException("新增时，表只能有一个");
            }
            for (String tableName:tableNames) {
                //是否有自定义别名
                if (tableName.contains(ALIAS_SPLIT)) {
                    //填写了表别名
                    throw new RuntimeException("新增时，表不需要有别名");
                }
                // 遍历过滤条件
                JSONObject propertis = request.getJSONObject(tableName);
                for (String condition:propertis.keySet()) {
                    //关键字则跳过
                    if (condition.endsWith("@"))
                        continue;
                    if (condition.matches("\\w+")) {
                        //纯字段名
                        validateColumn(tableName, condition);
                        list.add(condition);
                    } else {
                        throw new RuntimeException("新增时，" + condition + "必须是字段名");
                    }
                }
            }
        }
        return list;
    }

    @Override
    public List<String> getValues() {
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.INSERT) {
            Set<String> tableNames = request.keySet();
            if (tableNames.size() != 1) {
                throw new RuntimeException("新增时，表只能有一个");
            }
            for (String tableName:tableNames) {
                //是否有自定义别名
                if (tableName.contains(ALIAS_SPLIT)) {
                    //填写了表别名
                    throw new RuntimeException("新增时，表不需要有别名");
                }
                // 遍历过滤条件
                JSONObject propertis = request.getJSONObject(tableName);
                for (String condition : propertis.keySet()) {
                    //关键字则跳过
                    if (condition.endsWith("@")) {
                        continue;
                    }
                    if (condition.matches("\\w+")) {
                        //纯字段名
                        if (propertis.get(condition) instanceof Integer || propertis.get(condition) instanceof Float ||
                            propertis.get(condition) instanceof Double || propertis.get(condition) instanceof BigDecimal) {
                            list.add(propertis.get(condition).toString());
                        } else if (propertis.get(condition) instanceof String) {
                            list.add("'" + ((String) propertis.get(condition)).replaceAll("'", "''") + "'");
                        }
                    } else {
                        throw new RuntimeException("新增时，" + condition + "必须是字段名");
                    }
                }
            }
        }
        return list;
    }

    /*
     * ==================================
     *              修改逻辑
     * ==================================
     */
    /**
     * 更新字段
     * "@description": "20190101,元旦快乐"
     * 有@标记的才会更新，否则会被认为是WHERE的条件
     */
    @Override
    public List<String> getUpdateFields() {
        List<String> list = new ArrayList<>();
        if (getStatementType() == StatementType.UPDATE) {
            Set<String> tableNames = request.keySet();
            if (tableNames.size() != 1) {
                throw new RuntimeException("更新时，表只能有一个");
            }
            for (String tableName:tableNames) {
                //是否有自定义别名
                if (tableName.contains(ALIAS_SPLIT)) {
                    //填写了表别名
                    throw new RuntimeException("更新时，表不需要有别名");
                }
                // 遍历过滤条件
                JSONObject propertis = request.getJSONObject(tableName);
                for (String condition : propertis.keySet()) {
                    //关键字则跳过
                    if (condition.matches("@\\w+")) {
                        //要更新的字段
                        String columnName = condition.replaceAll("@", "");
                        if (propertis.get(condition) instanceof Integer || propertis.get(condition) instanceof Float ||
                            propertis.get(condition) instanceof Double || propertis.get(condition) instanceof BigDecimal) {
                            validateColumn(tableName, columnName);
                            list.add(tableName + "." + columnName + "=" + propertis.get(condition).toString());
                        } else if (propertis.get(condition) instanceof String) {
                            validateColumn(tableName, columnName);
                            list.add(tableName + "." + columnName + "=" + "'" + ((String) propertis.get(condition)).replaceAll("'", "''") + "'");
                        }
                    }
                }
            }
        }
        return list;
    }

    /*
     * ==================================
     *              权限逻辑
     * ==================================
     * 不管权限认证系统有多复杂，最后到生成SQL这步
     * 都是进行黑白名单的检查
     * 不论黑白，只要名单为空，表示所有数据都可以
     * 表名单：
     *   格式：表名
     *   大小写不敏感
     * 字段名单：
     *   格式：表名.字段
     *   大小写不敏感
     * 所有字段：
     *   格式： 表名.*
     * 如果想要新增或者修改的更复杂的逻辑，
     * 请在外层处理完成之后以制定格式提交该SQL的黑白名单即可
     */

    /** 表的黑白名单检查 */
    private void validateTable(String tableName) {
        if (tableBlackSet.contains(tableName)) {
//            throw new RuntimeException("请求的表:" + tableName + "在黑名单中");
            return;
        }
        if (tableWhiteSet.contains(tableName)==false) {
//            throw new RuntimeException("请求的表:" + tableName + "不在白名单中");
        }
    }

    /** 字段黑白名单检查 */
    private void validateColumn(String tableName, String columnName) {
        Set<String> columnBlackSet = columnBlackMap.get(tableName);
        if (columnBlackSet!=null && columnBlackSet.contains(columnName)) {
//            throw new RuntimeException("请求的字段:" + columnName + "在"+tableName+"黑名单中");
            return;
        }

        Set<String> columnWhiteSet = columnWhiteMap.get(tableName);
        if (columnWhiteSet==null || columnWhiteSet.contains(columnName)==false) {
//            throw new RuntimeException("请求的字段:" + columnName + "不在"+tableName+"表白名单中");
        }
    }
}