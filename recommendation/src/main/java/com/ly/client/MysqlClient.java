package com.ly.client;

import com.ly.entity.ProductEntity;
import com.ly.entity.RatingEntity;
import com.ly.util.Property;

import java.sql.*;

public class MysqlClient {
    private static String url = Property.getStrValue("mysql.url");
    private static String username = Property.getStrValue("mysql.username");
    private static String password = Property.getStrValue("mysql.password");
    private static Statement stmt;

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(url, username, password);
            stmt = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean putData(Object object) throws SQLException {
        String sql="";
        if(object instanceof ProductEntity) {
            ProductEntity product = (ProductEntity) object;
            sql = String.format("insert into product (productId, name, imageUrl, categories, tags) values (%d, \"%s\", \"%s\", \"%s\", \"%s\") ",
                    product.getProductId(), format(product.getName()), format(product.getImageUrl()),
                    format(product.getCategories()), format(product.getTags()));
        } else if(object instanceof RatingEntity) {
            RatingEntity rating = (RatingEntity) object;
            sql = String.format("insert into rating (userId, productId, score, timestamp) values (%d, %d, %f, %d)",
                    rating.getUserId(), rating.getProductId(),
                    rating.getScore(), rating.getTimestamp());
        }
        System.out.println(sql);
        return stmt.execute(sql);
    }
    public static String format(String str) {
        str.replaceAll("\"", "\\\"");
        if(str.startsWith("\"")) {
            str = "\\\""+str.substring(1, str.length()-1) + "\\\"";
        }
        return str;
    }

    public static void main(String[] args) throws SQLException {

        ProductEntity productEntity = new ProductEntity(1111, "\"致我们终将逝去的青春(附\"\"致青春\"\"珍藏卡册)\"",
                "https://images-cn-4.ssl-images-amazon.com/images/I/31QPvUDNavL._SY300_QL70_.jpg",
                "外设产品|鼠标|电脑/办公",
                "富勒|鼠标|电子产品|好用|外观漂亮");
        putData(productEntity);
    }


}