package org.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

public class Spark1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("OneApp");
        //SparkSession spark = SparkSession.builder().master("local[*]").appName("Spark").getOrCreate();
        //final JavaSparkContext ctx = JavaSparkContext.fromSparkContext(spark.sparkContext());
        final JavaSparkContext ctx = new JavaSparkContext(conf);

        System.out.println("======== begin RDD ==========");

        //JavaRDD<String> source = spark.read().textFile("stuInfo.txt").javaRDD();
        JavaRDD<String> source = ctx.textFile("./stuInfo.txt");

        source.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        JavaRDD<Student> rowRDD = source.map(new Function<String, Student>() {
            @Override
            public Student call(String line) throws Exception {
                String[] parts = line.split(",");
                Student stu = new Student();
                stu.setSid(parts[0]);
                stu.setSname(parts[1]);
                stu.setSage(Integer.parseInt(parts[2]));
                System.out.println(stu);
                return stu;
            }
        });

        rowRDD.foreach(new VoidFunction<Student>() {
            @Override
            public void call(Student student) throws Exception {
                System.out.println(">>> " + student);
            }
        });

        System.out.println("======== finish RDD ==========");

        //SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Spark").getOrCreate();
        //Dataset<Row> df = sparkSession.createDataFrame(rowRDD, Student.class);

        SQLContext sqlContext = new SQLContext(ctx);
        Dataset<Row> df = sqlContext.createDataFrame(rowRDD, Student.class);
//        df.select("sid", "sname", "sage").
//                coalesce(1).
//                write().
//                mode(SaveMode.Append).
//                parquet("parquet.res");
        df.show();

        System.out.println("======== finish DF ==========");
    }
}
