package com.ly.task.OfflineRecommender;

import com.ly.dataSource.HbaseTableSource;
import com.ly.sink.HbaseSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import javax.xml.crypto.Data;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StatisticsTask {
    // 创建执行环境
    private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    // 创建批处理表环境
    private static BatchTableEnvironment benv = BatchTableEnvironment.create(env);

    /*
     * 分析历史热门，最近热门，历史好评商品
     * */
    public static void main(String[] args) throws Exception {
        // 重新获取执行环境
        env = ExecutionEnvironment.getExecutionEnvironment();

        // 从 HBase 表中创建数据集
        DataSet dataSet = env.createInput(new HbaseTableSource());

        // 将数据集转换为表，并重命名列
        Table table = benv.fromDataSet(dataSet, "f0, f1, f2, f3")
                .renameColumns("f0 as userId, f1 as productId, f2 as score, f3 as timestamp");

        // 注册表为 “rating”
        benv.registerTable("rating", table);

        // 调用分析方法
        historyHotProducts();
//        recentHotProducts(dataSet);
        goodProducts();

        // 执行任务
        env.execute();
    }

    // 分析历史热门产品
    public static void historyHotProducts() throws Exception {
        // 定义 SQL 查询，计算每个产品的热度，并限制只取前 100 条记录
        String sql2 = " SELECT * , ROW_NUMBER() OVER (PARTITION BY productId ORDER BY hot DESC) as rowNumber " +
                "FROM (SELECT productId, COUNT(productId) as hot FROM rating " +
                "GROUP BY productId ORDER BY hot DESC)";

        // 只保存前 100 热门数据
        String sql = "SELECT productId, COUNT(productId) as hot FROM rating GROUP BY productId ORDER BY hot DESC LIMIT 100";

        // 执行 SQL 查询
        Table table1 = benv.sqlQuery(sql);

        // 定义结果类型
        TupleTypeInfo tupleType = new TupleTypeInfo<Tuple2<String, Long>>(Types.STRING, Types.LONG);

        // 将表转换为数据集，并输出到 HBase 表 'historyHotProducts'
        DataSet result = benv.toDataSet(table1, tupleType);
        result.print();
        result.output(new HbaseSink("historyHotProducts"));
    }

    // 分析近期热门产品
    public static void recentHotProducts(DataSet<Tuple4<String, String, Double, String>> dataSet) throws Exception {
        // 转换时间格式
        DataSet source = dataSet.map(new MapFunction<Tuple4<String, String, Double, String>, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map(Tuple4<String, String, Double, String> s) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMM");
                String time = format.format(new Date(Long.parseLong(s.f3) * 1000L));
                return new Tuple3<String, Double, String>(s.f1, s.f2, time);
            }
        });

        // 将数据集转换为表，并重命名列
        Table table = benv.fromDataSet(source).renameColumns("f0 as productId, f1 as score, f2 as yearmonth");
        benv.registerTable("recentTable", table);

        // 执行 SQL 查询，按月份和热度排序
        Table table2 = benv.sqlQuery("select productId, count(productId) as hot, yearmonth " +
                "from recentTable group by yearmonth, productId order by yearmonth desc, hot desc");

        // 定义结果类型
        TupleTypeInfo tupleType = new TupleTypeInfo<Tuple2<String, Long>>(Types.STRING, Types.LONG, Types.STRING);

        // 将表转换为数据集，并输出到 HBase 表 'recentHotProducts'
        DataSet result = benv.toDataSet(table2, tupleType);
//        result.print();
        result.output(new HbaseSink("recentHotProducts"));
    }

    // 分析好评产品
    public static void goodProducts() throws Exception {
        // 执行 SQL 查询，计算每个产品的平均评分，并限制只取前 100 条记录
        Table table = benv.sqlQuery("select productId, avg(score) as avgScore from rating group by productId order by avgScore desc limit 100");

        // 定义结果类型
        TupleTypeInfo tupleType = new TupleTypeInfo<Tuple2<String, Double>>(Types.STRING, Types.DOUBLE);

        // 将表转换为数据集，并输出到 HBase 表 'goodProducts'
        DataSet result = benv.toDataSet(table, tupleType);
//        result.print();
        result.output(new HbaseSink("goodProducts"));
    }
}
