package test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import spark.Person;

import java.util.List;

/**
 * Created by BF400254 on 2018/5/21.
 */
public class SparkCacheTest {

    public static void main(String[] args) {
        //创建一个RDD对象
        SparkConf conf=new SparkConf().setAppName("Simple").setMaster("local");
        //创建spark上下文对象，是数据的入口
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD txtRdd = sc.textFile("spark-biz/src/main/resources/people.txt");
        //优化，2种缓存方式
        txtRdd.cache();
        //txtRdd.persist(StorageLevel.MEMORY_AND_DISK());
        //转成对象
        Function<String,Person> parFun = (line)->{
            String[] parts = line.split(",");
            Person person = new Person();
            person.setName(parts[0]);
            person.setAge(Integer.parseInt(parts[1].trim()));
            return person;
        };

        String name = "Justin3";
        Function<String,Boolean> filterFun = (line)-> {
            if( line.split(",")[0].equals(name)){
                return true;
            }
            return false;

        };
        //转成对象
        JavaRDD<Person> personJavaRdd = txtRdd.map(parFun);
        JavaRDD<String> fileLine = txtRdd.filter(filterFun);
        System.out.println("person each");
        personJavaRdd.foreachPartition(personIterator -> {
            while (personIterator.hasNext()){
                System.out.println(personIterator.next());
            }
        });

        System.out.println("============================");
        System.out.println("filter line");
        fileLine.foreach(e->{
                System.out.println(e);
           }
        );
        sc.stop();
    }
}
