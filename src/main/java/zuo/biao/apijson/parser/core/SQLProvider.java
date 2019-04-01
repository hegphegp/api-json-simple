package zuo.biao.apijson.parser.core;

import java.util.List;

/**
 * 实现这个接口提供SQLBuilder获取构造SQL语句所需要的数据
 */
public interface SQLProvider {
    /**
     * 消息类，如果Provider中有异常，通过这个方法可以传递消息
     * @return
     */
    Message getMessage();

    /**
     * StatementType表示要生成的是SELECT,INSERT,UPDATE,DLEETE中的哪种
     * @return StatementType
     */
    StatementType getStatementType();

    /**
     * StatementType为SELECT时
     * SELECT要查询的字段
     * @return List<String>
     */
    List<String> getSelect();

    /**
     * StatementType为UPDATE时
     * UPDATE要更新的字段
     * @return List<String>
     */
    List<String> getSets();

    /**
     * StatementType所有类型都将使用这个方法
     * SQL所涉及的表
     * @return
     */
    List<String> getTables();

    List<String> getJoin();

    List<String> getInnerJoin();

    List<String> getOuterJoin();

    List<String> getLeftOuterJoin();

    List<String> getRightOuterJoin();

    /**
     * SQL过滤条件
     * @return
     */
    List<String> getWhere();

    List<String> getHaving();

    /**
     * StatementType为SELECT时
     * SQL查询时的分组
     * @return
     */
    List<String> getGroupBy();

    /**
     * StatementType为SELECT时
     * SQL查询时的排序
     * @return
     */
    List<String> getOrderBy();

    List<String> getLastList();

    /**
     * StatementType为INSERT时
     * SQL新增的字段
     * @return
     */
    List<String> getColumns();

    /**
     * StatementType为INSERT时
     * SQL新增字段所对应的值
     * @return
     */
    List<String> getValues();

    boolean isDistinct();
}
