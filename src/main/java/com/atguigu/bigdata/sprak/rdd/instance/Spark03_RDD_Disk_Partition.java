package com.atguigu.bigdata.sprak.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Spark03_RDD_Disk_Partition {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");
        sparkConf.set("spark.default.parallelism","1");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // TODO Spark 读取文件可以传递路径，这个路径可以是绝对路径，也可以是相对路径
        //      IDEA种默认的相对路径以项目的根路径为基准。不是以模块的根路径基准

        // TODO 文件数据源分区设定也存在多个位置
        //          1.textFile可以传递第二个参数：  minPartitions(最小分区数)
        //                参数可以不需要传递的，那么Spark会采用默认值
        //                      minPartitions =math.min(defaultParallelism, 2)
        //          2.使用配置参数:spark.default.parallelism => 4 => math.min(参数，2)
        //          3.采用环境默认总核值 => math.min(总核数，2)

        // TODO Spark框架基于MR开发的。
        //      Spark框架文件的操作没有自己的实现的。采用MR库(Hadoop)来实现
        //      当前读取文件的切片数量不是由Spark决定的，而是由Hadoop决定

        // Hadoop切片规则:
        //      totalsize : 7 byte
        //      goalsize  : totalsize / min-part-num => 7 / 3 => 2 byte
        //      part-num  : totalsize / goalsize => 7 / 2 => 3...1 => 10% => 3 + 1
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("data/test.txt");

        stringJavaRDD.saveAsTextFile("output");


        javaSparkContext.close();
    }
}
