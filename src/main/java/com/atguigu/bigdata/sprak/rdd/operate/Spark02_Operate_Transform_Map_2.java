package com.atguigu.bigdata.sprak.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Spark02_Operate_Transform_Map_2 {
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

        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(
               nums,2
        );
        // JDK1.8来自于Scala语言，由马丁引入，能省则省
//        JavaRDD<Integer> mapRDD = javaRDD.map(new Function<Integer, Integer>() {
//            @Override
//            public Integer call(Integer in) throws Exception {
//                return in * 2;
//            }
//        });
        // TODO 如果Java中接口采用注解@FunctionalInterface声明，那么接口的使用就可以采用JDK提供的函数式编程的语法实现（Lambda表达式）
        //      1.return 可以省略 : map方法就需要返回值，所以不屑return
        //      2、分号 可以省略 : 可以采用换行的方式表示代码逻辑
        //      3.大括号 可以省略 : 如果逻辑代码只有一行
        //      4.小括号 可以省略 : 参数列表中的参数只有一个
        //      5.参数和箭头 可以省略 : 参数在逻辑中只使用了一次(需要有对象来实现功能)
//        JavaRDD<Integer> mapRDD = javaRDD.map(
//                 in -> in * 2
//        );

        JavaRDD<Integer> mapRDD = javaRDD.map(
                NumberTest::mul2
        );


        mapRDD.collect().forEach(System.out::println);
        System.out.println("=====================================");
        System.out.println(nums);


        javaSparkContext.close();
    }
}

class NumberTest {
    public static int mul2(Integer num) {
        return 2 * num;
    }
}