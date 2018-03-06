package spark;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

/**
 * Created by qinjun on 18/1/16.
 * 一些基本rdd操作举例,只是提供一些思路
 *
 * cd /usr/local/Cellar/hadoop/2.8.2/sbin  ./start-dfs.sh 命令来启动Hadoop了。
 *  spark-shell
 *
 */
public class SparkRdd {

    public static void rdd(JavaSparkContext sc){
        /**
         * 创建一些rdd 数据测试数据,在测试的时候经常使用
         */
        List<String> list = Arrays.asList("1","3","4","5");
        JavaRDD<String> rddList =  sc.parallelize(list);


        //这里可以手动创建一些rdd row数据,来测试
         JavaRDD<Row> javaRddRow = sc.parallelize(
                Arrays.asList(
                        RowFactory.create("310318","20130606"),
                        RowFactory.create("202211","20180115"),
                        RowFactory.create("005090","19000101"),
                        RowFactory.create("004734","19000101")
                )
        );

        //对LIst 查出相关数据  对javaRddRow 指定分区数,因为有jdbc或redis操作,需要指定大小 避免连接数过多,对db造成太大压力
        JavaPairRDD<String, BigDecimal> javaPair = javaRddRow.repartition(20).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, BigDecimal>() {
            @Override
            public Iterable<Tuple2<String, BigDecimal>> call(Iterator<Row> rowIterator) throws Exception {
                List<Tuple2<String, BigDecimal>> resList = new ArrayList<>();
                List<String> dbList = new ArrayList<>();
                List<String> dbListId = new ArrayList<>();
                String eachFundId;
                String eachEndDate;
                //获取jedis,连接
                Row eachRow;
                while(rowIterator.hasNext()){
                    eachRow = rowIterator.next();
                    eachFundId = eachRow.getString(0);
                    eachEndDate = eachRow.getString(1);
                    //select from redis
                    List<String > listResNvAsc =null;
                    if(CollectionUtils.isEmpty(listResNvAsc)){
                        dbListId.add(eachFundId);
                    }else{
                        dbList.add(eachFundId);
                        resList.add(new Tuple2<>(eachFundId,BigDecimal.ONE));
                    }
                }
                //从db查询复权净值
                if(CollectionUtils.isNotEmpty(dbList)){
                    //create db collection
                    //select
                    //close
                    BigDecimal valueString = BigDecimal.ONE;
                    for (String attrTuplePo : dbList) {
                        //key,value
                        resList.add(new Tuple2<>(attrTuplePo,valueString));
                    }
                }
                return resList;
            }
        }).cache();


        //计算这一时期的同类均值
        Avg avg = new Avg(BigDecimal.ZERO,0);
        //对相同的key求平均值,并对Key排序
        JavaPairRDD<String, Avg> avgCounts = javaPair.aggregateByKey(avg,count,combine).sortByKey();

        List<Tuple2<String,Avg>> countMap = avgCounts.collect();

        //process with countMap
        //end






    }



    public static class Avg implements Serializable {
        public BigDecimal sum;
        public int num;
        public Avg(BigDecimal total,int num){
            this.sum=total;
            this.num=num;
        }
        public BigDecimal avg(){
            return sum.divide(new BigDecimal(String.valueOf(num)),2,BigDecimal.ROUND_HALF_UP);
        }
    }
    static Function2<Avg,BigDecimal, Avg> count=
            new Function2<Avg, BigDecimal, Avg>() {
                public Avg call(Avg a, BigDecimal x) throws Exception {
                    a.sum= a.sum.add(x);
                    a.num+=1;
                    return a;
                }
            };
    static Function2<Avg, Avg, Avg> combine=
            new Function2<Avg, Avg, Avg>() {
                public Avg call(Avg a, Avg b) throws Exception {
                    a.sum = a.sum.add(b.sum);
                    a.num+=b.num;
                    return a;
                }
            };






}
