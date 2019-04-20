package zuo.biao.apijson.parser.core;

import java.math.BigDecimal;
import java.util.*;

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
    public Map<String,String> aliasNameMap = new LinkedHashMap<>();
    private Set<String> tableNames;
    private JSONObject request;
    private JSONObject join;

    private APIJSONProvider() { }

    /** 传入的参数应该是一个通过验证的APIJSON请求 */
    public APIJSONProvider(JSONObject obj, StatementType statementType) {
        if (obj == null) {
            throw new RuntimeException("APIJSONProvider传入的请求不能为空");
        }
        super.setStatementType(statementType);
        JSONObject tabs = obj.getJSONObject("[]");
        this.request = tabs!=null? tabs:obj;
        this.join = tabs!=null? obj.getJSONObject("join"):new JSONObject();
        tableNames = this.request.keySet();
        for (String tableName:tableNames) {
            if (StatementType.INSERT==statementType && tableName.contains(ALIAS_SPLIT)) {  // 是否有自定义别名
                throw new RuntimeException("新增时，表不需要有别名");                          // 填写了表别名
            }
            if (!tableName.matches("(\\w+(:\\w+)?)")) {
                throw new RuntimeException("表" + tableName + "格式不符合");
            } else {
                Table table = new Table(tableName);
                aliasNameMap.put(table.aliasName, table.realName);
            }
        }
        if (StatementType.INSERT==statementType || StatementType.UPDATE==statementType) {
            if (tableNames.size() != 1) {
                throw new RuntimeException("新增或者更新时，表只能有一个");
            }
        }
    }

    private static final Map<String, String> tableJoinConvertMap = new HashMap<String, String>(){{
        put("@innerJoin","INNER JOIN");
        put("@leftOuterJoin","LEFT OUTER JOIN");
        put("@rightOuterJoin","RIGHT OUTER JOIN");
        put("@join","JOIN");
        put("@outerJoin","OUTER JOIN");
    }};
    private static final Set<String> tableJoinTypes = tableJoinConvertMap.keySet();

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
        List<String> tableNameList = new ArrayList();
        for (String tableName:tableNames) {
            Table table = new Table(tableName);
            validateTable(table.realName);
            if (isSelectOperation()) {
                tableNameList.add(table.realName+" "+table.aliasName);
                for (String joinKey: tableJoinTypes) {
                    JSONArray array = join.getJSONArray(joinKey)!=null? join.getJSONArray(joinKey):new JSONArray();
                    for (Object obj:array) {
                        String joinTableName = obj instanceof String ? (String) obj:"";
                        if (joinTableName.startsWith(table.realName) || joinTableName.startsWith(table.aliasName)) {
                            tableNameList.remove(table.realName+" "+table.aliasName);
                            continue;
                        }
                    }
                }
            } else {
                tableNameList.add(table.realName);
            }
        }
        return tableNameList;
    }

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
        if (isSelectOperation()==false) { return list; }
        for (String tableName:tableNames) {
            Table table = new Table(tableName);

            JSONObject propertis = request.getJSONObject(tableName);
            String columnsValue = propertis.getString("@column");

            if (columnsValue == null) {
                validateColumn(table.realName, "*");
                list.add(table.aliasName + ".*"); //没有填写@column字段，默认为全部
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
                        validateColumn(table.realName, columnRealName);
                        if (propertis.getString("id@") != null) {
                            columnAliasName = wrapColumn(table.aliasName, columnAliasName);
                        }
                        list.add(funcitonName + "(" + table.aliasName + "." + columnRealName + ")" + " as " + columnAliasName);
                    } else { //没函数的字段
                        String columnRealName = functionOrColumn;
                        String columnAliasName = columnName.split(ALIAS_SPLIT)[1];
                        validateColumn(table.realName, columnRealName);
                        columnAliasName = propertis.getString("id@") != null? wrapColumn(table.aliasName, columnAliasName): columnAliasName;
                        list.add(table.aliasName + "." + columnRealName + " as " + columnAliasName);
                    }
                } else {
                    //使用tableAliasName.columnName类型
                    validateColumn(table.realName, columnName);
                    if (propertis.getString("id@") != null) {
                        String columnAliasName = wrapColumn(table.aliasName, columnName);
                        list.add(table.aliasName + "." + columnName + " as " + columnAliasName);
                    } else {
                        list.add(table.aliasName + "." + columnName);
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
        for (String tableName:tableNames) {
            Table table = new Table(tableName);
            JSONObject propertis = request.getJSONObject(tableName); // 遍历过滤条件
            for (String condition:propertis.keySet()) {
                if (condition.startsWith("@")) { continue; } // 关键字则跳过
                if (condition.matches("\\w+")) {       // 纯字段名
                    Object queryValue = propertis.get(condition);
                    if (queryValue instanceof Integer || queryValue instanceof Float || queryValue instanceof Double || queryValue instanceof BigDecimal) {
                        list.add(table.aliasName + "." + condition + " = " + propertis.get(condition));
                    } else if (propertis.get(condition) instanceof String) {
                        list.add(table.aliasName + "." + condition + " = '" + ((String) propertis.get(condition)).replaceAll("'", "''") + "'");
                    }
                } else if (condition.matches("(\\w+(!|&|\\|)?\\{\\})+")) {  // 表示这是一个多条件类型
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        // 是否匹配这种类型：<20.3,>3.3,=3.3
                        if (!exp.matches("(\\s?(>|<|>=|<=|=|<>)+\\s?((\\-|\\+)?\\d+(\\.\\d+)?)+)+(\\s?,\\s?((>|<|>=|<=|=|<>)+\\s?((\\-|\\+)?\\d+(\\.\\d+)?)))*")) {
                            throw new RuntimeException(condition + "的格式不正确，正确使用方式如: >10,<20");
                        }
                        if (condition.endsWith("|{}")) {
                            String[] terms = exp.replaceAll("\\s", "").split(",");
                            String columnName = condition.replaceAll("\\|\\{\\}", "");
                            assemblySQLOrAnd(list, OR, table.aliasName, columnName, terms);
                        } else {
                            getLastList().add(AND);
                            String[] terms = exp.replaceAll("\\s", "").split(",");
                            String columnName = condition.replaceAll("\\{\\}", "").replaceAll("&", "").replaceAll("!", "");
                            assemblySQLOrAnd(list, AND, table.aliasName, columnName, terms);
                        }
                    } else if (propertis.get(condition) instanceof JSONArray) {
                        JSONArray array = (JSONArray) propertis.get(condition);
                        String columnName = condition.replaceAll("\\{\\}", "").replaceAll("&", "").replaceAll("!", ""); // 分离出字段名
                        if (condition.endsWith("!{}")) {
                            assemblyArrConditions(list, array, table.aliasName, columnName, false);
                        } else {
                            assemblyArrConditions(list, array, table.aliasName, columnName, true);
                        }
                    }
                } else if (condition.matches("\\w+~")) { // 字符串查询，包含
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        String columnName = condition.replaceAll("~", "");
                        list.add(table.aliasName + "." + columnName + " LIKE '%" + exp.replaceAll("'", "''") + "%'");
                    } else {
                        throw new RuntimeException(condition + "的值必须要是字符串");
                    }
                } else if (condition.matches("\\w+\\$")) { // 字符串查询，LIKE
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        String columnName = condition.replaceAll("\\$", "");
                        list.add(table.aliasName + "." + columnName + " LIKE '" + exp.replaceAll("'", "''") + "'");
                    } else {
                        throw new RuntimeException(condition + "的值必须要是字符串");
                    }
                } else if (condition.matches("\\w+\\?")) { // 字符串查询，正则
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        String columnName = condition.replaceAll("\\?", "");
                        list.add(" regexp_like(" + table.aliasName + "." + columnName + ",'" + exp.replaceAll("'", "''") + "')");
                    } else {
                        throw new RuntimeException(condition + "的值必须要是字符串");
                    }
                } else if (condition.matches("\\w+@")) {   // 内连接
                    if (propertis.get(condition) instanceof String) {
                        String exp = (String) propertis.get(condition);
                        String columnName = condition.replaceAll("@", "");
                        if (exp.matches("/\\w+/\\w+")) {
                            String[] args = exp.split("/");
                            String refTable = args[1];
                            String refColumn = args[2];
                            list.add(refTable + "." + refColumn + " = " + table.aliasName + "." + columnName);
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
        return dealJoin("@innerJoin");
    }

    /** 左外连接 */
    @Override
    public List<String> getLeftOuterJoin() {
        return dealJoin("@leftOuterJoin");
    }

    /** 右外链接 */
    @Override
    public List<String> getRightOuterJoin() {
        return dealJoin("@rightOuterJoin");
    }

    /** join连接 */
    @Override
    public List<String> getJoin() {
        return dealJoin("@join");
    }

    /** 外连接 */
    @Override
    public List<String> getOuterJoin() {
        return dealJoin("@outerJoin");
    }

    public List<String> dealJoin(String joinType) {
        List<String> list = new ArrayList<>();
        if (isSelectOperation()==false) { return list; }
        JSONArray stms = join.get(joinType)!= null? join.getJSONArray(joinType):new JSONArray();
        for (int i = 0; i < stms.size(); i++) {
            if (stms.get(i) instanceof String) {
                String joinStr = (String) stms.get(i);
                if (joinStr.matches("\\w+\\.\\w+\\s?=\\s?\\w+\\.\\w+")) {
                    String[] tcs = joinStr.replaceAll("\\s", "").split("=");
                    String tableName = tcs[0].split("\\.")[0];
                    validateTable(tableName);
                    if (aliasNameMap.get(tableName)!=null && tableName.equals(aliasNameMap.get(tableName))==false) {
                        list.add(aliasNameMap.get(tableName) + " "+ tableName + " ON " + joinStr);
                    } else {
                        list.add(tableName + " ON " + joinStr);
                    }
                } else {
                    throw new RuntimeException(joinType+"的格式必须是：table1.column1 = table2.column2,相当于"+tableJoinConvertMap.get(joinType)+" table1 ON table1.column1=table2.column2");
                }
            } else {
                throw new RuntimeException(joinType+"的类型必须是String类型，填写的值如：table1.column1 = table2.column2,相当于"+tableJoinConvertMap.get(joinType)+" table1 ON table1.column1=table2.column2。注意：表有别名的应该写表别名");
            }
        }
        return list;
    }

    /** 解析分组 "@group":"store_id" */
    @Override
    public List<String> getGroupBy() {
        List<String> list = new ArrayList();
        if (isSelectOperation()==false) { return list; }
        for (String tableName : tableNames) {
            // 获取请求@group的值
            JSONObject propertis = request.getJSONObject(tableName);
            String columnsValue = propertis.getString("@group");
            if (columnsValue != null) {
                if (!columnsValue.matches("\\w+(\\s?,\\s?\\w+)*")) {
                    throw new RuntimeException("字段@group：" + columnsValue + "格式不符合，正确请求如：a,b,c");
                }
                String[] columnNames = columnsValue.replaceAll("\\s", "").split(",");
                for (String colmunName : columnNames) {
                    list.add(new Table(tableName).aliasName + "." + colmunName);
                }
            }

        }
        return list;
    }

    /** 解析排序逻辑 column1+,column2-,+表示升序，-表示降序 */
    @Override
    public List<String> getOrderBy() {
        List<String> list = new ArrayList();
        if (isSelectOperation()==false) { return list; }
        for (String tableName : tableNames) {
            Table table = new Table(tableName);
            JSONObject propertis = request.getJSONObject(tableName); // 获取请求@column的值
            String columnsValue = propertis.getString("@orders");
            if (columnsValue != null) {
                if (!columnsValue.matches("(\\w+(\\+|\\-)?)+(\\s?,\\s?(\\w+(\\+|\\-)?)+)*")) {
                    throw new RuntimeException("字段@orders：" + columnsValue + "格式不符合，正确格式如：column1+,column2-,+表示升序，-表示降序。");
                }
                //没有填写@orders字段，默认为全部
                String[] columnNames = columnsValue.replaceAll("\\s", "").split(",");
                for (String columnName : columnNames) {
                    if (columnName.endsWith("+")) {
                        list.add(table.aliasName + "." + columnName.replaceAll("\\+", "") + " ASC");
                    } else if (columnName.endsWith("-")) {
                        list.add(table.aliasName + "." + columnName.replaceAll("\\-", "") + " DESC");
                    }
                }
            }
        }
        return list;
    }

    @Override
    public List<String> getInsertFields() {
        List<String> list = new ArrayList();
        if (isInsertOperation()==false) { return list; }
        for (String tableName:tableNames) {
            JSONObject propertis = request.getJSONObject(tableName);  // 遍历过滤条件
            for (String condition:propertis.keySet()) {
                if (condition.endsWith("@")) { continue; }            // 关键字则跳过
                if (condition.matches("\\w+")) {  // 纯字段名
                    validateColumn(tableName, condition);
                    list.add(condition);
                } else {
                    throw new RuntimeException("新增时，" + condition + "必须是字段名");
                }
            }
        }
        return list;
    }

    @Override
    public List<String> getInsertValues() {
        List<String> list = new ArrayList();
        if (isInsertOperation()==false) { return list; }
        for (String tableName:tableNames) {
            JSONObject propertis = request.getJSONObject(tableName);
            for (String condition : propertis.keySet()) {   // 遍历过滤条件
                if (condition.endsWith("@")) { continue; }  // 关键字则跳过
                if (condition.matches("\\w+")) {      // 纯字段名
                    Object condValue = propertis.get(condition);
                    if (condValue instanceof Integer || condValue instanceof Float || condValue instanceof Double || condValue instanceof BigDecimal) {
                        list.add(condValue.toString());
                    } else if (condValue instanceof String) {
                        list.add("'" + ((String) condValue).replaceAll("'", "''") + "'");
                    }
                } else {
                    throw new RuntimeException("新增时，" + condition + "必须是字段名");
                }
            }
        }
        return list;
    }

    /**
     * 更新字段
     * "@description": "20190101,元旦快乐"
     * 有@标记的才会更新，否则会被认为是WHERE的条件
     */
    @Override
    public List<String> getUpdateFields() {
        List<String> list = new ArrayList();
        if (isUpdateOperation()==false) { return list; }
        for (String tableName:tableNames) {
            if (tableName.contains(ALIAS_SPLIT)) {                   // 是否有自定义别名
                throw new RuntimeException("更新时，表不需要有别名");
            }
            JSONObject propertis = request.getJSONObject(tableName); // 遍历过滤条件
            for (String condition:propertis.keySet()) {
                if (condition.matches("@\\w+")) { // 关键字则跳过
                    String columnName = condition.replaceAll("@", "");  // 要更新的字段
                    Object condValue = propertis.get(condition);
                    if (condValue instanceof Integer || condValue instanceof Float || condValue instanceof Double || condValue instanceof BigDecimal) {
                        validateColumn(tableName, columnName);
                        list.add(tableName + "." + columnName + "=" + condValue.toString());
                    } else if (propertis.get(condition) instanceof String) {
                        validateColumn(tableName, columnName);
                        list.add(tableName + "." + columnName + "=" + "'" + ((String) condValue).replaceAll("'", "''") + "'");
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

    public static class Table {
        private final String ALIAS_SPLIT = ":";
        public String realName;
        public String aliasName;
        public Table() { }
        public Table(String tableNameStr) {
            if (tableNameStr.contains(ALIAS_SPLIT)) {
                String[] arr = tableNameStr.split(ALIAS_SPLIT);
                this.realName  = arr[0];
                this.aliasName = arr[1];
            } else {
                this.realName = this.aliasName = tableNameStr;
            }
        }
    }
}