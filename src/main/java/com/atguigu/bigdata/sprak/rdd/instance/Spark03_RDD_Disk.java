package com.atguigu.bigdata.sprak.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark03_RDD_Disk {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // TODO 构建RDD数据处理模型
        //      利用环境对象对接磁盘数据源（文件），构建RDD对象

        //  textFile方法可以传递一个参数，文件路径
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("D:\\yuandiannao\\zhuomian\\Java_code\\spark\\data\\test.txt");

        List<String> collect = stringJavaRDD.collect();

        collect.forEach(System.out::println);


        javaSparkContext.close();
    }
}
