package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Spark08_Operate_Transform_KV_1 {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> nums = Arrays.asList(1, 2, 3, 4);

        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(nums);
        // TODO 单值类型数据可以和KV类型数据进行转换
        javaRDD.mapToPair(
                num -> new Tuple2<>(num, num * 2)
        )
                        .mapValues(
                                num -> num * 2
                        )
                        .collect()
                        .forEach(System.out::println);


        javaSparkContext.close();
    }
}
