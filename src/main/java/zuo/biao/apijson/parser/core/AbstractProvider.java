package zuo.biao.apijson.parser.core;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractProvider implements SQLProvider {
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

    private Message message = new Message();

    public Message getMessage() {
        return message;
    }

    /**
     * 接收错误消息
     * @param err
     */
    public void error(String err) {
        StackTraceElement ste = new Throwable().getStackTrace()[1];
        this.message.error(err, ste);
    }

    @Override
    public StatementType getStatementType() {
        return statementType;
    }

    public void setStatementType(StatementType statementType) {
        this.statementType = statementType;
    }

    @Override
    public List<String> getSets() {
        return sets;
    }

    @Override
    public List<String> getSelect() {
        return select;
    }

    @Override
    public List<String> getTables() {
        return tables;
    }

    @Override
    public List<String> getJoin() {
        return join;
    }

    @Override
    public List<String> getInnerJoin() {
        return innerJoin;
    }

    @Override
    public List<String> getOuterJoin() {
        return outerJoin;
    }

    @Override
    public List<String> getLeftOuterJoin() {
        return leftOuterJoin;
    }

    @Override
    public List<String> getRightOuterJoin() {
        return rightOuterJoin;
    }

    @Override
    public List<String> getWhere() {
        return where;
    }

    @Override
    public List<String> getHaving() {
        return having;
    }

    @Override
    public List<String> getGroupBy() {
        return groupBy;
    }

    @Override
    public List<String> getOrderBy() {
        return orderBy;
    }

    @Override
    public List<String> getLastList() {
        return lastList;
    }

    @Override
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public List<String> getValues() {
        return values;
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    public void setSets(List<String> sets) {
        this.sets = sets;
    }

    public void setSelect(List<String> select) {
        this.select = select;
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

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

}
