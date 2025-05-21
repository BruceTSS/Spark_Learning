package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Spark02_Operate_Transform_Map {
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

        // TODO RDD方法
        // 【1,2】  【3,4】
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(
               nums,2
        );

        // TODO RDD的转换方法：map
        //      对单值数据进行处理

        // map : 映射 (K -> V)，将A(K)转换成B(V)
        //       将指定的值转换为其他的值的场合
        //       [1,2,3,4] -> [2,4,6,8]
        // 学习方法的重点：
        //      1. 名字
        //      2. IN参数
        //      3. OUT参数
        JavaRDD<Object> mapRDD = javaRDD.map(new Function<Integer, Object>() {
            @Override
            public Object call(Integer in) throws Exception {
                return in * 2;
            }
        });

        mapRDD.collect().forEach(System.out::println);
        System.out.println("=====================================");
        System.out.println(nums);


        javaSparkContext.close();
    }
}
