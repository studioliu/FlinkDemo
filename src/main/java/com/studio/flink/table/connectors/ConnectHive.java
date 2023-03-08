package com.studio.flink.table.connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class ConnectHive {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //设置方言 不同数据库的语句有差别
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        String name = "myhive"; // Catalog名称，定义一个唯一的名称表示
        String defaultDatabase = "default"; // 默认Hive数据库名称
//        String hiveConfDir = "/usr/hive/apache-hive-3.1.3-bin/conf";  //hive-site.xml路径，服务器文件位置
        String hiveConfDir = "src/main/resources";  //把hive-site.xml拷贝到resources下，本地文件位置
        String VERSION = "3.1.2";   //hive版本

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, VERSION);
        tableEnv.registerCatalog("myhive", hive);   // 注册Catalog
        tableEnv.useCatalog("myhive");   // 使用注册的Catalog ，不使用的话查不到数据

        tableEnv.executeSql("show databases").print();
    }
}

/**
 * @Maven 依赖
 * <!-- Flink Dependency -->
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-connector-hive_2.12</artifactId>
 * <version>1.16.0</version>
 * <scope>provided</scope>
 * </dependency>
 *
 * <dependency>
 * <groupId>org.apache.flink</groupId>
 * <artifactId>flink-table-api-java-bridge_2.12</artifactId>
 * <version>1.16.0</version>
 * <scope>provided</scope>
 * </dependency>
 * <p>
 * <!-- Hive Dependency -->
 * <dependency>
 * <groupId>org.apache.hive</groupId>
 * <artifactId>hive-exec</artifactId>
 * <version>${hive.version}</version>
 * <scope>provided</scope>
 * </dependency>
 * <p>
 * <p>
 * Flink SQL
 * <p>
 * show
 * -- 列出catalog
 * SHOW CATALOGS;
 * -- 列出数据库
 * SHOW DATABASES;
 * --列出表
 * SHOW TABLES;
 * --列出函数
 * SHOW FUNCTIONS;
 * -- 列出所有激活的 module
 * SHOW MODULES;
 * <p>
 * create
 * CREATE 语句用于向当前或指定的 Catalog 中注册表、视图或函数。注册后的表、视图和函数可以在 SQL 查询中使用。
 * CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
 * (
 * { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
 * [ <watermark_definition> ]
 * [ <table_constraint> ][ , ...n]
 * )
 * [COMMENT table_comment]
 * [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
 * WITH (key1=val1, key2=val2, ...)
 * [ LIKE source_table [( <like_options> )] ]
 * <p>
 * <p>
 * -- 例如
 * CREATE TABLE Orders_with_watermark (
 * `user` BIGINT,
 * product STRING,
 * order_time TIMESTAMP(3),
 * WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
 * ) WITH (
 * 'connector' = 'kafka',
 * 'scan.startup.mode' = 'latest-offset'
 * );
 * <p>
 * drop
 * DROP 语句可用于删除指定的 catalog，也可用于从当前或指定的 Catalog 中删除一个已经注册的表、视图或函数。
 * --删除表
 * DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
 * --删除数据库
 * DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]
 * --删除视图
 * DROP [TEMPORARY] VIEW  [IF EXISTS] [catalog_name.][db_name.]view_name
 * --删除函数
 * DROP [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name;
 * <p>
 * alter
 * ALTER 语句用于修改一个已经在 Catalog 中注册的表、视图或函数定义。
 * --修改表名
 * ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name
 * --设置或修改表属性
 * ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
 * --修改视图名
 * ALTER VIEW [catalog_name.][db_name.]view_name RENAME TO new_view_name
 * --在数据库中设置一个或多个属性。若个别属性已经在数据库中设定，将会使用新值覆盖旧值。
 * ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)
 * <p>
 * insert
 * INSERT 语句用来向表中添加行(INTO是追加，OVERWRITE是覆盖)
 * -- 1. 插入别的表的数据
 * INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name [PARTITION part_spec] select_statement
 * <p>
 * -- 2. 将值插入表中
 * INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name VALUES [values_row , values_row ...]
 * <p>
 * <p>
 * <p>
 * -- 追加行到该静态分区中 (date='2019-8-30', country='China')
 * INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
 * SELECT user, cnt FROM page_view_source;
 * <p>
 * -- 追加行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
 * INSERT INTO country_page_view PARTITION (date='2019-8-30')
 * SELECT user, cnt, country FROM page_view_source;
 * <p>
 * -- 覆盖行到静态分区 (date='2019-8-30', country='China')
 * INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30', country='China')
 * SELECT user, cnt FROM page_view_source;
 * <p>
 * -- 覆盖行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
 * INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30')
 * SELECT user, cnt, country FROM page_view_source;
 * <p>
 * <p>
 * Flink SQL
 * <p>
 * show
 * -- 列出catalog
 * SHOW CATALOGS;
 * -- 列出数据库
 * SHOW DATABASES;
 * --列出表
 * SHOW TABLES;
 * --列出函数
 * SHOW FUNCTIONS;
 * -- 列出所有激活的 module
 * SHOW MODULES;
 * <p>
 * create
 * CREATE 语句用于向当前或指定的 Catalog 中注册表、视图或函数。注册后的表、视图和函数可以在 SQL 查询中使用。
 * CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
 * (
 * { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
 * [ <watermark_definition> ]
 * [ <table_constraint> ][ , ...n]
 * )
 * [COMMENT table_comment]
 * [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
 * WITH (key1=val1, key2=val2, ...)
 * [ LIKE source_table [( <like_options> )] ]
 * <p>
 * <p>
 * -- 例如
 * CREATE TABLE Orders_with_watermark (
 * `user` BIGINT,
 * product STRING,
 * order_time TIMESTAMP(3),
 * WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
 * ) WITH (
 * 'connector' = 'kafka',
 * 'scan.startup.mode' = 'latest-offset'
 * );
 * <p>
 * drop
 * DROP 语句可用于删除指定的 catalog，也可用于从当前或指定的 Catalog 中删除一个已经注册的表、视图或函数。
 * --删除表
 * DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
 * --删除数据库
 * DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]
 * --删除视图
 * DROP [TEMPORARY] VIEW  [IF EXISTS] [catalog_name.][db_name.]view_name
 * --删除函数
 * DROP [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name;
 * <p>
 * alter
 * ALTER 语句用于修改一个已经在 Catalog 中注册的表、视图或函数定义。
 * --修改表名
 * ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name
 * --设置或修改表属性
 * ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
 * --修改视图名
 * ALTER VIEW [catalog_name.][db_name.]view_name RENAME TO new_view_name
 * --在数据库中设置一个或多个属性。若个别属性已经在数据库中设定，将会使用新值覆盖旧值。
 * ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)
 * <p>
 * insert
 * INSERT 语句用来向表中添加行(INTO是追加，OVERWRITE是覆盖)
 * -- 1. 插入别的表的数据
 * INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name [PARTITION part_spec] select_statement
 * <p>
 * -- 2. 将值插入表中
 * INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name VALUES [values_row , values_row ...]
 * <p>
 * <p>
 * <p>
 * -- 追加行到该静态分区中 (date='2019-8-30', country='China')
 * INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
 * SELECT user, cnt FROM page_view_source;
 * <p>
 * -- 追加行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
 * INSERT INTO country_page_view PARTITION (date='2019-8-30')
 * SELECT user, cnt, country FROM page_view_source;
 * <p>
 * -- 覆盖行到静态分区 (date='2019-8-30', country='China')
 * INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30', country='China')
 * SELECT user, cnt FROM page_view_source;
 * <p>
 * -- 覆盖行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
 * INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30')
 * SELECT user, cnt, country FROM page_view_source;
 */

/**
 *
 * Flink SQL
 *
 * show
 * -- 列出catalog
 * SHOW CATALOGS;
 * -- 列出数据库
 * SHOW DATABASES;
 * --列出表
 * SHOW TABLES;
 * --列出函数
 * SHOW FUNCTIONS;
 * -- 列出所有激活的 module
 * SHOW MODULES;
 *
 * create
 * CREATE 语句用于向当前或指定的 Catalog 中注册表、视图或函数。注册后的表、视图和函数可以在 SQL 查询中使用。
 * CREATE TABLE [IF NOT EXISTS] [catalog_name.][db_name.]table_name
 *   (
 *     { <physical_column_definition> | <metadata_column_definition> | <computed_column_definition> }[ , ...n]
 *     [ <watermark_definition> ]
 *     [ <table_constraint> ][ , ...n]
 *   )
 *   [COMMENT table_comment]
 *   [PARTITIONED BY (partition_column_name1, partition_column_name2, ...)]
 *   WITH (key1=val1, key2=val2, ...)
 *   [ LIKE source_table [( <like_options> )] ]
 *
 *
 * -- 例如
 * CREATE TABLE Orders_with_watermark (
 *     `user` BIGINT,
 *     product STRING,
 *     order_time TIMESTAMP(3),
 *     WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
 * ) WITH (
 *     'connector' = 'kafka',
 *     'scan.startup.mode' = 'latest-offset'
 * );
 *
 * drop
 * DROP 语句可用于删除指定的 catalog，也可用于从当前或指定的 Catalog 中删除一个已经注册的表、视图或函数。
 * --删除表
 * DROP TABLE [IF EXISTS] [catalog_name.][db_name.]table_name
 * --删除数据库
 * DROP DATABASE [IF EXISTS] [catalog_name.]db_name [ (RESTRICT | CASCADE) ]
 * --删除视图
 * DROP [TEMPORARY] VIEW  [IF EXISTS] [catalog_name.][db_name.]view_name
 * --删除函数
 * DROP [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF EXISTS] [catalog_name.][db_name.]function_name;
 *
 * alter
 * ALTER 语句用于修改一个已经在 Catalog 中注册的表、视图或函数定义。
 * --修改表名
 * ALTER TABLE [catalog_name.][db_name.]table_name RENAME TO new_table_name
 * --设置或修改表属性
 * ALTER TABLE [catalog_name.][db_name.]table_name SET (key1=val1, key2=val2, ...)
 * --修改视图名
 * ALTER VIEW [catalog_name.][db_name.]view_name RENAME TO new_view_name
 * --在数据库中设置一个或多个属性。若个别属性已经在数据库中设定，将会使用新值覆盖旧值。
 * ALTER DATABASE [catalog_name.]db_name SET (key1=val1, key2=val2, ...)
 *
 * insert
 * INSERT 语句用来向表中添加行(INTO是追加，OVERWRITE是覆盖)
 * -- 1. 插入别的表的数据
 * INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name [PARTITION part_spec] select_statement
 *
 * -- 2. 将值插入表中
 * INSERT { INTO | OVERWRITE } [catalog_name.][db_name.]table_name VALUES [values_row , values_row ...]
 *
 *
 *
 * -- 追加行到该静态分区中 (date='2019-8-30', country='China')
 * INSERT INTO country_page_view PARTITION (date='2019-8-30', country='China')
 *   SELECT user, cnt FROM page_view_source;
 *
 * -- 追加行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
 * INSERT INTO country_page_view PARTITION (date='2019-8-30')
 *   SELECT user, cnt, country FROM page_view_source;
 *
 * -- 覆盖行到静态分区 (date='2019-8-30', country='China')
 * INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30', country='China')
 *   SELECT user, cnt FROM page_view_source;
 *
 * -- 覆盖行到分区 (date, country) 中，其中 date 是静态分区 '2019-8-30'；country 是动态分区，其值由每一行动态决定
 * INSERT OVERWRITE country_page_view PARTITION (date='2019-8-30')
 *   SELECT user, cnt, country FROM page_view_source;
 */