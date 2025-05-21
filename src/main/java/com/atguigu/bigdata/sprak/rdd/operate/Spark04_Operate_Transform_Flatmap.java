package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Spark04_Operate_Transform_Flatmap {
    public static void main(String[] args) {
        // 添加这行代码设置hadoop.home.dir
        System.setProperty("hadoop.home.dir", "D:\\software\\hadoop");  // 替换为你的实际路径

        // 添加这行解决Windows下的Hadoop原生库问题
        System.load("D:\\software\\hadoop\\bin\\hadoop.dll");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("Spark");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<List<Integer>> datas = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4)
        );

        JavaRDD<List<Integer>> javaRDD = javaSparkContext.parallelize(
               datas,2
        );

        // TODO RDD的转换方法：flatmap（扁平映射）
        //      flat（数据扁平化） + map（映射）

        JavaRDD<Integer> flatMapRDD = javaRDD.flatMap(new FlatMapFunction<List<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(List<Integer> List) throws Exception {
                ArrayList<Integer> nums = new ArrayList<>();
                List.forEach(
                        num -> nums.add(num * 2)
                );
                return nums.iterator();
            }
        });


        flatMapRDD.collect().forEach(System.out::println);


        javaSparkContext.close();
    }
}
