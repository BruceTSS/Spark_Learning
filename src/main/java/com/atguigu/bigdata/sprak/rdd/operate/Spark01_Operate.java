package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;

public class Spark01_Operate {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // TODO RDD方法
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(
                Arrays.asList(1, 2, 3)
        );

        // RDD的方法有很多，主要讲解核心，重要的方法
        // 学习的重担：
        //      1. 名字
        //      2. IN参数
        //      3. OUT参数

        // RDD的方法会有很多，但是分为两类
        //      1. 转换方法
        //      2. 行动方法

        // RDD方法处理数据的分类：
        // 1.单值 ：1,， “abc”, new User(), new ArrayList()
        // 2.键值 : KV => (Key, Value)
        //      word -> count

        // TODO JDK1.8以后也存在元祖，采用特殊的类 ： TupleX
        Tuple1<String> abc = new Tuple1<>("abc");
        Tuple2<String, Integer> a = new Tuple2<>("a", 1);
        System.out.println(a._1);
        System.out.println(a._1());
        System.out.println(a._2);
        System.out.println(a._2());
        // 马丁的幸运数字是22
        // 元祖的最大数据容量是22
        // new Tuple22<>();

        javaSparkContext.close();
    }
}
