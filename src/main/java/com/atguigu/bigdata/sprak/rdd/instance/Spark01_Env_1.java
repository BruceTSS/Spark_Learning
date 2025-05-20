package com.atguigu.bigdata.sprak.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark01_Env_1 {
    public static void main(String[] args) {
        // TODO 构建Spark的运行环境

        // TODO 创建sprak配置对象

        JavaSparkContext javaSparkContext = new JavaSparkContext("local[2]", "Spark");

        // TODO 释放资源
        javaSparkContext.close();
    }
}
