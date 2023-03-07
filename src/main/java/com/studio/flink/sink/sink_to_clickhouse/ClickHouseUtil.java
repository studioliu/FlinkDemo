package com.studio.flink.sink.sink_to_clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ClickHouseUtil {
    private static Connection conn;

    private ClickHouseUtil() {
    }

    public static Connection getConnection(String host, int port, String database) throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        return DriverManager.getConnection(address);
    }

    public static Connection getConnection(String host, int port) throws SQLException, ClassNotFoundException {
        return getConnection(host, port, "default");
    }

    public static void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
