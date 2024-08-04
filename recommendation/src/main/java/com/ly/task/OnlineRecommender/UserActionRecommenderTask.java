package com.ly.task.OnlineRecommender;

import com.ly.client.HbaseClient;
import com.ly.map.OnlineRecommendMapFunction;
import com.ly.util.Property;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class UserActionRecommenderTask {
    /**
     * 基于用户评分行为，对用户进行实时推荐
     */

    // 定义用户行为推荐方法
    public static void userActionRecommender() throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取 Kafka 配置
        Properties properties = Property.getKafkaProperties("rating");

        // 从 Kafka 中获取数据流
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("rating", new SimpleStringSchema(), properties));

        // 处理数据流
        dataStream.map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        // 分割输入的评分数据
                        String[] tmp = s.split(",");
                        // 生成 rowkey，格式为 userId_productId_timestamp
                        String rowkey = tmp[0] + "_" + tmp[1] + "_" + tmp[3];
                        // 打印 rowkey
                        System.out.println(rowkey);

                        // 记录用户评分信息到 HBase
                        HbaseClient.putData("rating", rowkey, "log", "productId", tmp[1]);
                        HbaseClient.putData("rating", rowkey, "log", "userId", tmp[0]);
                        HbaseClient.putData("rating", rowkey, "log", "score", tmp[2]);
                        HbaseClient.putData("rating", rowkey, "log", "timestamp", tmp[3]);

                        // 记录用户-产品信息到 HBase
                        HbaseClient.increamColumn("userProduct", tmp[0], "product", tmp[1]);

                        // 记录产品-用户信息到 HBase（暂时不入库）
                        // HbaseClient.increamColumn("productUser", tmp[1], "user", tmp[0]);

                        return s;
                    }
                })
                // 在线推荐处理并打印结果
                .map(new OnlineRecommendMapFunction())
                .print();

        // 执行任务
        env.execute();
    }

    // 主方法，调用用户行为推荐方法
    public static void main(String[] args) throws Exception {
        userActionRecommender();
    }
}
