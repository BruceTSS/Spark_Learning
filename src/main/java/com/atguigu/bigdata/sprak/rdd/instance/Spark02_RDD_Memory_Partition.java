package com.atguigu.bigdata.sprak.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark02_RDD_Memory_Partition {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");
        sparkConf.set("spark.default.parallelism","4");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // TODO Kafka可以将数据进行切片（减小规模），也称之为分区，这个分区操作是底层完成的。
        //      loacl环境中，分区数量和环境核数相关，但是一般不推荐
        //      分区数量需要手动设定
        //          Spark在读取集合数据时，分区设定存在3种不同场合1.优先使用方法参数
        //              1.优先使用方法参数
        //              2.使用配置参数:spark.default.parallelism
        //              3.采用环境默认值
        List<String> names = Arrays.asList("zhangsan", "lisi", "wangwu", "zhaoliu");

        //  parallelize方法可以传递2个参数
        //      第一个参数：表示对接的数据源集合
        //      第二个参数：表示切片（分区）数量，可以不需要指定，spark会采用默认值进行分区（切片）
        //                    numSlices = scheduler,conf.getInt("spark.default.parallelism", totalCores)
        //                    从配置对象中获取配置参数:spark.default.parallelism(默认并行度)
        //                    如果配置参数不存在，那么采用环境默认值totalCores(当前环境总的虚拟核数)
        JavaRDD<String> javaRDD = javaSparkContext.parallelize(names,3);

        // TODO 将数据模型分区后的数据保存到磁盘文件中
        //      saveAsTextFile（）方法可以传递一个参数：表示输出的文件路径，路径可以为绝对路径，也可以为相对路径。
        //      IDEA默认的项目路径以项目的根目录为准。
        javaRDD.saveAsTextFile("output");


        javaSparkContext.close();
    }
}
