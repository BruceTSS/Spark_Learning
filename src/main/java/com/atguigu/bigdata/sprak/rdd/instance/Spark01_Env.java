package com.atguigu.bigdata.sprak.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark01_Env {
    public static void main(String[] args) {
        // TODO 构建Spark的运行环境

        // TODO 创建sprak配置对象
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // TODO 释放资源
        javaSparkContext.close();
    }
}
