import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

import static org.apache.hadoop.mapreduce.lib.map.RegexMapper.PATTERN;

/**
 * @author xuechenyang(morsuning @ gmail.com)
 * @date 2020/11/19 19:08
 */
public class SparkExample {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("sparkBoot").setMaster("local");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 创建 RDD
        JavaRDD<String> lines = sparkContext.textFile("/usr/local/data").cache();

        // 将每个数据传给 Func 进行格式化，返回新 RDD
        lines.map(new Function<String, Object>() {
            @Override
            public Object call(String s) {
                return s;
            }
        });

        // 将所有数据传入 Func 进行格式化
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            // 将每个元素用" "分割
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        // 对每个词记为 1
        JavaPairRDD<String, Integer> wordsOnes = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 将 Key 相同的键值对将 Value 相加合并
        JavaPairRDD<String, Integer> wordsCounts = wordsOnes.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer value, Integer toValue) {
                return value + toValue;
            }
        });

        // 筛选出只含 error 的，形成新 RDD
        JavaRDD<String> inputRDD = sparkContext.textFile("/usr/local/log");
        JavaRDD<String> errorRDD = inputRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("error");
            }
        });

        // 统计次数
        long errorRDDCount = errorRDD.count();
        System.out.println("errorRDD 的总数为: "+errorRDDCount);

        // 从中取固定数目数据
        for (String rddLine:errorRDD.take(10)){
            System.out.println("errorRDD的数据是:"+rddLine);
        }

        // 获得全部数据(能在单台服务器中放下)
        List<String> allData = errorRDD.collect();

        // 可以传入如下的具体类代替上面所示的匿名内部类

        //定义具体类
        class ContainsErrorDev implements Function<String,Boolean>{
            private String query;
            public ContainsErrorDev(String query){
                this.query = query;
            }
            public Boolean call(String v1) {
                return v1.contains(query);
            }
        }
        JavaRDD<String> errorRDD2 = inputRDD.filter(new ContainsErrorDev("Error"));

        // 写入结果
        wordsCounts.saveAsTextFile("/usr/local/data1");
        errorRDD.saveAsObjectFile("./");

        // 这里我们直接创建RDD
        JavaRDD<Integer> num = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));

        // 新生成的RDD元素
        JavaRDD<Integer> result = num.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * v1;
            }
        });
        System.out.println(StringUtils.join(result.collect(),","));

        // 对每个输入元素生成多个输出元素。实现该功能的操作叫作 flatMap()
        JavaRDD<String> lines2 = sparkContext.parallelize(Arrays.asList("hello world", "hi"));

        JavaRDD<String> flatMapResult  = lines2.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(PATTERN.split(s)).iterator();
            }
        });

        flatMapResult.first();
        //结果:hello

        // 去重，开销很大
        flatMapResult.distinct();

        // 将两个相同的 RDD 合并，此操作会去重
        flatMapResult.union(errorRDD2);

        // 只返回两个 RDD 中都有的元素，也会去重，开销大于 union
        flatMapResult.intersection(errorRDD2);

        // 从 RDD1 中移除 RDD2 数据
        flatMapResult.subtract(errorRDD2);

        // 返回 RDD 中元素出现的总数
        Map<String, Long> map = flatMapResult.countByValue();

        // 返回排序后的前 2 个元素
        flatMapResult.top(2);

        // 返回排序后的前 2 个元素，可以指定也可不指定排序器
        flatMapResult.takeOrdered(2, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return 0;
            }
        });

        /**
         * RDD的操作分为两种
         * 转换操作 转化操作RDD是惰性求值
         * 行动操作 返回非RDD的新元素
         */

        // RDD 缓存策略 persist方法或cache方法可以将前面的计算结果缓存

        // 行动操作
        // reduce 接收一个函数作为参数,操作两个 RDD 的元素,并且返回一个相同类型的新元素
        JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
        Integer sum = rdd.reduce(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }
        );
        System.out.println(sum.intValue());
        // 最后输出:55

        // 如果输入输出参数不一致，使用aggregate
        /**
         * aggregate():
         * aggregate() 函数则把我们从返回值类型必须与所操作的RDD类型相同的限制中解放出来
         * 使用 aggregate() 时，需要提供我们期待返回的类型的初始值
         * 然后通过一个函数把RDD中的元素合并起来放入累加器,考虑到每个节点是在本地进行累加的
         * 最终.还需要提供第二个函数来将累加器两两合并
         * 计算平均值
         */
        class RddAvg {
            private Integer total;
            private Integer num;

            public RddAvg(Integer total, Integer num) {
                this.total = total;
                this.num = num;
            }

            public double avg() {
                return total / num;
            }

            Function2<RddAvg, Integer, RddAvg> avgFunction2 = new Function2<RddAvg, Integer, RddAvg>() {
                @Override
                public RddAvg call(RddAvg v1, Integer v2) {
                    v1.total += v2;
                    v1.num += 1;
                    return v1;
                }
            };

            Function2<RddAvg,RddAvg,RddAvg> rddAvgFunction2 = new Function2<RddAvg, RddAvg, RddAvg>() {
                @Override
                public RddAvg call(RddAvg v1, RddAvg v2) {
                    v1.total += v2.total;
                    v1.num += v2.num;
                    return v1;
                }
            };

            public void rddAvg(JavaSparkContext sparkContext) {
                JavaRDD<Integer> javaRDD = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
                RddAvg rddAvg = new RddAvg(0, 0);
                RddAvg result = javaRDD.aggregate(rddAvg, rddAvg.avgFunction2, rddAvg.rddAvgFunction2);
                System.out.println(result.avg());
            }
        }





    }

}
