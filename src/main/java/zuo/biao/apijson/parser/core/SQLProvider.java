package zuo.biao.apijson.parser.core;

import java.util.ArrayList;
import java.util.List;

/** 实现这个接口提供SQLBuilder获取构造SQL语句所需要的数据 */
public class SQLProvider {
    private StatementType statementType;
    private List<String> updateFields = new ArrayList();
    private List<String> selectFields = new ArrayList();
    private List<String> insertFields = new ArrayList();
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
    private List<String> values = new ArrayList();
    private boolean distinct;

    /** StatementType表示要生成的是SELECT,INSERT,UPDATE,DELETE中的哪种 */
    public StatementType getStatementType() {
        return statementType;
    }

    public void setStatementType(StatementType statementType) {
        this.statementType = statementType;
    }

    public boolean isSelectOperation() {
        if (StatementType.SELECT==statementType) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isInsertOperation() {
        if (StatementType.INSERT==statementType) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isUpdateOperation() {
        if (StatementType.UPDATE==statementType) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isDeleteOperation() {
        if (StatementType.DELETE==statementType) {
            return true;
        } else {
            return false;
        }
    }
    /** StatementType为UPDATE时，UPDATE要更新的字段 */
    public List<String> getUpdateFields() {
        return updateFields;
    }

    /** StatementType为SELECT时，SELECT要查询的字段 */
    public List<String> getSelectFields() {
        return selectFields;
    }

    /** StatementType所有类型都将使用这个方法，SQL所涉及的表 */
    public List<String> getTables() {
        return tables;
    }

    public List<String> getJoin() {
        return join;
    }

    public List<String> getInnerJoin() {
        return innerJoin;
    }

    public List<String> getOuterJoin() {
        return outerJoin;
    }

    public List<String> getLeftOuterJoin() {
        return leftOuterJoin;
    }

    public List<String> getRightOuterJoin() {
        return rightOuterJoin;
    }

    /** SQL过滤条件 */
    public List<String> getWhere() {
        return where;
    }

    public List<String> getHaving() {
        return having;
    }

    /** StatementType为SELECT时，SQL查询时的分组 */
    public List<String> getGroupBy() {
        return groupBy;
    }

    /** StatementType为SELECT时，SQL查询时的排序 */
    public List<String> getOrderBy() {
        return orderBy;
    }

    public List<String> getLastList() {
        return lastList;
    }

    /** StatementType为INSERT时，SQL新增的字段 */
    public List<String> getInsertFields() {
        return insertFields;
    }

    /** StatementType为INSERT时，SQL新增字段所对应的值 */
    public List<String> getInsertValues() {
        return values;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setUpdateFields(List<String> updateFields) {
        this.updateFields = updateFields;
    }

    public void setSelectFields(List<String> selectFields) {
        this.selectFields = selectFields;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public void setJoin(List<String> join) {
        this.join = join;
    }

    public void setInnerJoin(List<String> innerJoin) {
        this.innerJoin = innerJoin;
    }

    public void setOuterJoin(List<String> outerJoin) {
        this.outerJoin = outerJoin;
    }

    public void setLeftOuterJoin(List<String> leftOuterJoin) {
        this.leftOuterJoin = leftOuterJoin;
    }

    public void setRightOuterJoin(List<String> rightOuterJoin) {
        this.rightOuterJoin = rightOuterJoin;
    }

    public void setWhere(List<String> where) {
        this.where = where;
    }

    public void setHaving(List<String> having) {
        this.having = having;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public void setOrderBy(List<String> orderBy) {
        this.orderBy = orderBy;
    }

    public void setLastList(List<String> lastList) {
        this.lastList = lastList;
    }

    public void setInsertFields(List<String> insertFields) {
        this.insertFields = insertFields;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

}
