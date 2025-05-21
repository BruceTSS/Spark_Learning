package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Spark04_Operate_Transform_Flatmap_1 {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> javaRDD = javaSparkContext.textFile("data/test.txt");

        // TODO map方法只负责转换数据（A -> B[B1, B2, B3]），不能将数据拆分后独立使用
        //      line     => Hadoop Hive Spark
        //      string[] => [Hadoop,Hive,Spark]
        //      flatMap可以将数据拆分后独立使用（A -> B1, B2, B3）
//        JavaRDD<String[]> mapRDD = javaRDD.map(
//                line -> line.split(" ")
//        );

        JavaRDD<String> flatMapRDD = javaRDD.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        );

        flatMapRDD.collect().forEach(System.out::println);


        javaSparkContext.close();
    }
}
