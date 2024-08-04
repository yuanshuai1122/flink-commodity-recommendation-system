package com.ly.task.OfflineRecommender;

import com.ly.util.Property;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringValueUtils;
import scala.Tuple3;


public class ContentRecommender {
    public static void contentRecommend() throws Exception {
        // 创建 Flink 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 定义 MySQL 数据源的字段类型
        TypeInformation[] fieldTypes = new TypeInformation[] {
                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
        };

        // 创建 RowTypeInfo 对象，指定行数据的类型
        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        // 配置 JDBC 输入格式
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(new String(Property.getStrValue("mysql.driver"))) // 设置 MySQL 驱动程序
                .setDBUrl(new String(Property.getStrValue("mysql.url"))) // 设置 MySQL 数据库 URL
                .setUsername(new String(Property.getStrValue("mysql.username"))) // 设置 MySQL 用户名
                .setPassword(new String(Property.getStrValue("mysql.password"))) // 设置 MySQL 密码
                .setQuery("select productId, name, tags from product") // 设置查询 SQL 语句
                .setRowTypeInfo(rowTypeInfo) // 设置行数据的类型信息
                .finish();

        // 创建数据集，从 MySQL 中读取数据
        DataSet<Row> dataSet = env.createInput(jdbcInputFormat, rowTypeInfo);

        // 处理数据集，将 tags 字段中的“|”替换为空格
        dataSet.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row r) throws Exception {
                // 将 tags 字段中的“|”替换为空格，并返回新的 Row 对象
                String s = r.getField(2).toString().replace("|", " ");
                return Row.of(r.getField(0), r.getField(1), s);
            }
        }).print(); // 打印处理后的数据

        // TODO 计算 tf-idf（待实现）

        // 执行 Flink 作业
        env.execute();
    }

    // 主方法，调用内容推荐方法
    public static void main(String[] args) throws Exception {
        contentRecommend();
    }
}
