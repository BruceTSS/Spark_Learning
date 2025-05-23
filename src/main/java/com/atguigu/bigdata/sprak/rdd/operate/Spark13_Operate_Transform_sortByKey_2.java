package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark13_Operate_Transform_sortByKey_2 {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, Integer> rdd = javaSparkContext.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 4),
                new Tuple2<>("a", 3),
                new Tuple2<>("a", 2)
        ));

        // (a, 1) => (1, (a, 1))
        // (a, 2) => (2, (a, 2))
        // (a, 3) => (3, (a, 3))
        // (a, 4) => (4, (a, 4))
        rdd.mapToPair(
                kv  -> new Tuple2<>(kv._2, kv)
        ).sortByKey()
                .map(
                        kv -> kv._2
                ).collect().forEach(System.out::println);




        javaSparkContext.close();
    }
}
