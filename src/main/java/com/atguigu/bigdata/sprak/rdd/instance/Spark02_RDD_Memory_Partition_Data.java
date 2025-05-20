package com.atguigu.bigdata.sprak.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark02_RDD_Memory_Partition_Data {
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


        List<String> names = Arrays.asList("zhangsan", "lisi", "wangwu", "zhaoliu");
        /*
        【1】
        【2,3】
        【4】
        【5,6】
        ------------------------------------------------
        len=6，partnum=4

        (0 until 4)=>[0,1，2,3]

        0 => ((i * length) / numSlices,(((i + 1) * length) / numSlices))
          => ((0 * 6) / 4,(((0 + 1) * 6) / 4))
          => (0，1) => 1
        1 => ((i * length) / numSlices,(((i + 1) * length) / numSlices))
          => ((1 * 6) / 4,(((1 + 1) * 6) / 4))
          => (1，3) => 2
        2 => ((i * length) / numSlices,(((i + 1) * length) / numSlices))
          => ((2 * 6) / 4,(((2 + 1) * 6) / 4))
          => (3，4) => 1
        3 => ((i * length) / numSlices,(((i + 1) * length) / numSlices))
          => ((3 * 6) / 4,(((3 + 1) * 6) / 4))
          => (4，6) => 1
         */


        // TODO spark分区数据的存储基本原则:平均分
        JavaRDD<String> javaRDD = javaSparkContext.parallelize(names,3);


        javaRDD.saveAsTextFile("output");


        javaSparkContext.close();
    }
}
