package org.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ReadTextToRDD {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("RDD的打印")
                .setMaster("local[2]")
                .set("spark.executor.memory", "2g");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("ERROR");

        // 创建 List<String>
        List<String> list = Arrays.asList("A-1", "A-2", "B-1", "B-1", "C-1", "E-1");
        // List<String> 转 JavaRDD<String>
        JavaRDD<String> javaRDD = jsc.parallelize(list);

        // 使用collect对JavaRDD<String>打印
/*        List<String> collect = javaRDD.collect();
        for (String str : collect) {
            System.out.println(String.format("JavaRDD<String>打印:%s", str));
        }*/
        //使用foreach对JavaRDD<String>打印
        javaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String str) throws Exception {
                System.out.println(String.format("JavaRDD<String>打印:%s", str));
            }
        });

        //JavaRDD<String> 转 JavaRDD<Row>
        JavaRDD<Row> javaRddRow = javaRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] sp = s.split("-");
                return RowFactory.create(sp[0], sp[1]);
            }
        });
        // 使用Row对JavaRDD<Row >打印
/*        List<Row> rowList = javaRddRow.collect();
        for (Row row : rowList) {
            System.out.println(String.format("JavaRDD<Row>打印:%s", row.toString()));
        }*/

        // 使用foreach对JavaRDD<Row >打印
        javaRddRow.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(String.format("JavaRDD<Row>打印:%s", row.toString()));
            }
        });

        // JavaRDD<String> 转 JavaPairRDD
        JavaPairRDD<String, Integer> javaPairRDD = javaRDD.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] sp = s.split("-");
                        return new Tuple2<String, Integer>(sp[0], Integer.parseInt(sp[1]));
                    }
                });
        // 使用collect对JavaPairRDD打印
/*        for (Tuple2<String, Integer> str : javaPairRDD.collect()) {
            System.out.println(String.format("JavaPairRDD打印:%s", str.toString()));
        }*/
        // 使用foreach对JavaPairRDD打印
        javaPairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(String.format("JavaPairRDD打印:%s", tuple.toString()));
            }
        });
    }
}
