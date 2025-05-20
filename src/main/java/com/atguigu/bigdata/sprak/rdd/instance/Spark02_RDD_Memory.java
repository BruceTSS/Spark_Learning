package com.atguigu.bigdata.sprak.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark02_RDD_Memory {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // TODO 构建RDD数据处理模型
        //      利用环境对象对接内存数据源，构建RDD对象
        List<String> names = Arrays.asList("zhangsan", "lisi", "wangwu", "zhaoliu");

        // TODO parallelize（并行）方法可以传递参数：集合
        //       RDD数据模型存在泛型
        JavaRDD<String> javaRDD = javaSparkContext.parallelize(names);

        List<String> collect = javaRDD.collect();

        collect.forEach(System.out::println);


        javaSparkContext.close();
    }
}
