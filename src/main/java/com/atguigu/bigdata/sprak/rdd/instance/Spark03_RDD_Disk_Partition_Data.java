package com.atguigu.bigdata.sprak.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark03_RDD_Disk_Partition_Data {
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

        // TODO Spark进行分区处理时，需要对每个分区的数据尽快能地平均分配
        //      totalsize=7
        //      goalsize = totalsize / minpartnum = 7 / 2 = 3
        //      partnum = totalsize / goalsize = 7 / 3 = 2...1 => 2 + 1 = 3
        // TODO Spark不文持文件操作的。文件操作都是由Hadoop完成的
        //      Hadoop进行文件切片数最的计算核文件数据存计规则不一样。
        //      1.分区数最计算的时候，考虑的是尽可能的平均 : 按字节来计算
        //      2.分区数据的存储是考虑业务数据的完整性 : 按照行来读取
        //        读取数据时，还需要考虑数据偏移量，偏移最从0开始的。
        //        读取数据时，相同的偏移量不能重复读取。

        /*
            【3】 => [0,3]
            【3】 => [3,6]
            【1】 => [6,7]
        ----------------------------------------------------------
             1@@ => 012
             2@@ => 345
             3 => 4
        ----------------------------------------------------------
             [0,3] => 【1,2】
             [3,6] => 【3】
             [3,6] =>
         */

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("data/test.txt");

        stringJavaRDD.saveAsTextFile("output");


        javaSparkContext.close();
    }
}
