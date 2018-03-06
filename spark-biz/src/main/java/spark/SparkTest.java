package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Created by qinjun on 18/1/16.
 *
 * cd /usr/local/Cellar/hadoop/2.8.2/sbin  ./start-dfs.sh 命令来启动Hadoop了。
 *  spark-shell
 *
 */
public class SparkTest {

    public static void main(String[] args) {

        //创建一个RDD对象
        SparkConf conf=new SparkConf().setAppName("Simple").setMaster("local");

        //创建spark上下文对象，是数据的入口
        JavaSparkContext sc=new JavaSparkContext(conf);

        //JavaSparkContext.jarOfClass(IPAddressStats.class)

        //test(sc);
        //sqlTest(sc);
        //SQLQueriesTest(sc);
       // hiveTest(sc);
        //jdbcTest(sc);

        jdbcTest1(sc);


        // runJdbcDatasetExample(sc);

        sc.stop();
    }

    /**
     * test past
     * @param spark
     */
    public static void test(JavaSparkContext spark){


        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        //并行集合，是通过对于驱动程序中的集合调用JavaSparkContext.parallelize来构建的RDD
        JavaRDD<Integer> distData = spark.parallelize(data);


        JavaRDD<Integer> lineLengths = distData.map(a->a);

        // 运行reduce 这是一个动作action 这时候，spark才将计算拆分成不同的task，
        // 并运行在独立的机器上，每台机器运行他自己的map部分和本地的reducation，并返回结果集给去驱动程序
        int totalLength = lineLengths.reduce((a,b)->a+b);

        System.out.println("总和" + totalLength);
        // 为了以后复用 持久化到内存...



    }

    public static void streamTest(JavaSparkContext sc){
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
    }

    /**
     * test past
     * @param sc
     */
    public static void sqlTest(JavaSparkContext sc){
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        DataFrame df = sqlContext.read().json("/Users/qinjun/work/sparkTest/src/main/resources/people.json");
        // Displays the content of the DataFrame to stdout
        df.show();
        // Print the schema in a tree format
        df.printSchema();

        // Select only the "name" column
        df.select("name").show();
        df.select(df.col("name"), df.col("age").plus(1)).show();
        // Select people older than 21
        df.filter(df.col("age").gt(21)).show();

        // Count people by age
        df.groupBy("age").count().show();
    }

    /**
     * test past
     * @param sc
     */
    public static void SQLQueriesTest(JavaSparkContext sc){
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        // Load a text file and convert each line to a JavaBean.
        JavaRDD<Person> people = sc.textFile("/Users/qinjun/work/sparkTest/src/main/resources/people.txt").map(
            new Function<String, Person>() {
                public Person call(String line) throws Exception {
                    String[] parts = line.split(",");

                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));

                    return person;
                }
            });

        // Apply a schema to an RDD of JavaBeans and register it as a table.
        DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
        schemaPeople.registerTempTable("people");
        // SQL can be run over RDDs that have been registered as tables.
        DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();

    }

    /**
     * fail ,need hive
     * @param sc
     */
    public static void hiveTest(JavaSparkContext sc){
        // sc is an existing JavaSparkContext.
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());
        sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        sqlContext.sql("LOAD DATA LOCAL INPATH '/Users/qinjun/work/sparkTest/src/main/resources/kv1.txt' INTO TABLE src");
        // Queries are expressed in HiveQL.
        Row[] results = sqlContext.sql("FROM src SELECT key, value").collect();
        for (int i = 0; i < results.length; i++) {
            System.out.println(results[i]);
        }
    }

    /**
     * test past
     * @param sc
     */
    public static void jdbcTest(JavaSparkContext sc){
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://199.199.199.199:3309/test?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true");
        options.put("dbtable", "test.test");
        options.put("user","test");
        options.put("password","test");
        //options.put("driver","");

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        //sqlContext.
        DataFrame jdbcDF = sqlContext.read().format("jdbc").options(options).load();
        //jdbcDF.
        jdbcDF.select("col1").show();
    }

    public static void jdbcTest1(JavaSparkContext sc){
        String fundId = "000001";

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        String jdbcUrl = "jdbc:mysql://199.199.199.199:3306/test?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true";
        String user = "test";
        String password = "test";
        Properties p = new Properties();
        p.put("user",user);
        p.put("password",password);
        //这里需要指定driver,本地直接运行main方法的时候不报错,但提交到服务器上就会报找不到driver
        p.put("driver","com.mysql.jdbc.Driver");


        DataFrame jdbcDF = sqlContext.read().jdbc(jdbcUrl,"test",p).
                select("col1","col2","col2").where("  col1='123'");
        //jdbcDF.javaRDD().
        jdbcDF.show();

    }

    /**
     * 一个表关联方法举例
     * @param sc
     */
    public static DataFrame jdbcTest2(JavaSparkContext sc){
        String fundId = "000001";

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        String jdbcUrl = "jdbc:mysql://199.199.199.199:3306/test?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true";
        String user = "test";
        String password = "test";
        Properties p = new Properties();
        p.put("user",user);
        p.put("password",password);
        //这里需要指定driver,本地直接运行main方法的时候不报错,但提交到服务器上就会报找不到driver
        p.put("driver","com.mysql.jdbc.Driver");

        DataFrame a = sqlContext.read().format("jdbc").jdbc(jdbcUrl,"tablename1", p).
                select("col1","col2","col3","col4","col5");
        DataFrame b = sqlContext.read().format("jdbc").jdbc(jdbcUrl,"tablename2", p)
                .select("col1","col2","col3","col4");
        DataFrame c = a.join(b,
                a.col("col1").equalTo(b.col("col1")).
                        and(b.col("col2").equalTo(10))).
                where(b.col("col3").equalTo("12345678").
                        and(a.col("col3").equalTo("12345678")).
                        and(a.col("col4").equalTo(1)).
                        and(a.col("col2").equalTo("12345678")).
                        and((b.col("col3").equalTo("12345678")).or(b.col("col4").$greater$eq("0998843543")))
                ).select(a.col("col1"));
        return  c;

    }


}
