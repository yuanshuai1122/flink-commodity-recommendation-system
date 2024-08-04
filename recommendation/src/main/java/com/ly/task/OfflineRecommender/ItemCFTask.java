package com.ly.task.OfflineRecommender;

import com.ly.client.HbaseClient;
import com.ly.dataSource.HbaseTableSource;
import com.ly.entity.RecommendEntity;
import com.ly.entity.RecommendReduceEntity;
import com.ly.map.CalculateSimilarityMapFunction;
import com.ly.map.RecommendEntityMapFunction;
import com.ly.util.ReduceRecommend;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ItemCFTask {
    // 未使用 Flink 的方法，该方法内部使用了 Map，如果数据量巨大可能会导致内存溢出
    public static void itemSimilarity() throws Exception {
        // 存储所有用户的列表
        List<String> allUser = new ArrayList<>();
        try {
            // 从 HBase 中获取所有用户的键
            allUser = HbaseClient.getAllKey("userProduct");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 存储产品相似度的映射
        Map<String, Integer> similarityMap = new HashMap<>();
        // 存储每个产品被用户评分的数量
        Map<String, Integer> productUserCountMap = new HashMap<>();

        // 遍历所有用户
        for (String user : allUser) {
            // 获取用户评分的产品列表
            List<Map.Entry> products = HbaseClient.getRow("userProduct", user);
            for (Map.Entry product : products) {
                // 更新产品的评分用户数量
                productUserCountMap.put((String) product.getKey(), productUserCountMap.getOrDefault(product.getKey(), 0) + 1);
                for (Map.Entry product2 : products) {
                    // 如果是同一个产品则跳过
                    if (product.getKey().equals(product2.getKey())) continue;
                    // 生成产品对的键
                    String key = product.getKey() + "_" + product2.getKey();
                    // 更新产品对的相似度
                    similarityMap.put(key, similarityMap.getOrDefault(key, 0) + 1);
                }
            }
        }
        // 计算相似度并保存到 HBase
        for (String key : similarityMap.keySet()) {
            String[] products = key.split("_");
            String product1 = products[0];
            String product2 = products[1];
            // 计算相似度
            Double similarity = similarityMap.get(key) / Math.sqrt(productUserCountMap.get(product1) * productUserCountMap.get(product2));
            // 保留五位小数
            String res = String.format("%.5f", similarity);
            System.out.println(product1 + "\t" + product2 + "\t" + res);
            // 将相似度数据存入 HBase
            HbaseClient.putData("similarity", product1, "p", product2, res);
        }
    }

    // 使用 Flink 计算产品相似度
    public static void calSimilarityUsingFlink() throws Exception {
        // 创建 Flink 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment bbTableEnv = BatchTableEnvironment.create(env);

        // 从 HBase 表中创建数据集，并映射为 userId|productId|score
        DataSet<Tuple3<String, String, Double>> dataSet = env.createInput(new HbaseTableSource()).map(new MapFunction<Tuple4<String, String, Double, String>, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(Tuple4<String, String, Double, String> s) throws Exception {
                return new Tuple3<>(s.f0, s.f1, s.f2);
            }
        });

        // 统计每个产品的评分次数，生成 userId|productId|score|count
        DataSet<Tuple4<String, String, Double, Integer>> productCount = dataSet.flatMap(new FlatMapFunction<Tuple3<String, String, Double>, Tuple4<String, String, Double, Integer>>() {
            @Override
            public void flatMap(Tuple3<String, String, Double> s, Collector<Tuple4<String, String, Double, Integer>> collector) throws Exception {
                collector.collect(new Tuple4<>(s.f0, s.f1, s.f2, 1));
            }
        }).groupBy(1).sum(3);

        // 计算产品对的相似度
        DataSet<Tuple5<String, String, Integer, String, Integer>> joinedByUserId = productCount.join(productCount).where(0).equalTo(0).flatMap(new FlatMapFunction<Tuple2<Tuple4<String, String, Double, Integer>, Tuple4<String, String, Double, Integer>>, Tuple5<String, String, Integer, String, Integer>>() {
            @Override
            public void flatMap(Tuple2<Tuple4<String, String, Double, Integer>, Tuple4<String, String, Double, Integer>> t, Collector<Tuple5<String, String, Integer, String, Integer>> collector) throws Exception {
                collector.collect(new Tuple5<>(t.f0.f0, t.f0.f1, t.f0.f3, t.f1.f1, t.f1.f3));
            }
        });

        // 将数据集转换为表，并重命名列
        Table table = bbTableEnv.fromDataSet(joinedByUserId).renameColumns("f0 as userId, f1 as product1, f2 as count1, f3 as product2, f4 as count2");
        bbTableEnv.registerTable("joined", table);

        // 执行 SQL 查询，计算产品对的 co-count、count1 和 count2
        Table table3 = bbTableEnv.sqlQuery(
                "SELECT product1, product2, COUNT(userId) as cocount, AVG(count1) as count1, AVG(count2) as count2 FROM joined GROUP BY product1, product2"
        );

        // 注册自定义相似度计算函数
        ScalarFunction calSim = new CalculateSimilarityMapFunction();
        bbTableEnv.registerFunction("calSim", calSim);

        // 使用自定义函数计算相似度，并过滤掉相同的产品对
        Table table4 = table3.map("calSim(product1, product2, cocount, count1, count2)").as("product1, product2, sim").filter("product1 != product2");

        // 定义结果类型
        TupleTypeInfo tupleType = new TupleTypeInfo<>(Types.STRING, Types.STRING, Types.DOUBLE);

        // 将表转换为数据集，并处理结果
        DataSet<Tuple3<String, String, Double>> dsRow = bbTableEnv.toDataSet(table4, tupleType);
        DataSet<RecommendReduceEntity> newRow = dsRow.map(new MapFunction<Tuple3<String, String, Double>, RecommendReduceEntity>() {
            @Override
            public RecommendReduceEntity map(Tuple3<String, String, Double> s) throws Exception {
                ArrayList<RecommendEntity> tmp = new ArrayList<>();
                tmp.add(new RecommendEntity(s.f1, s.f2));
                return new RecommendReduceEntity(s.f0, tmp);
            }
        }).groupBy("productId").reduce(new ReduceRecommend()).map(new RecommendEntityMapFunction());

        // 打印最终结果
        newRow.print();
        env.execute();
    }

    // 主方法，调用计算相似度的方法
    public static void main(String[] args) throws Exception {
        calSimilarityUsingFlink();
    }
}
