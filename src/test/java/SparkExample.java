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

    /**
     * 一些无状态转换操作的含义：
     * map(func) ：对源DStream的每个元素，采用func函数进行转换，得到一个新的DStream；
     * flatMap(func)： 与map相似，但是每个输入项可用被映射为0个或者多个输出项；
     * filter(func)： 返回一个新的DStream，仅包含源DStream中满足函数func的项；
     * repartition(numPartitions)： 通过创建更多或者更少的分区改变DStream的并行程度；
     * union(otherStream)： 返回一个新的DStream，包含源DStream和其他DStream的元素；
     * count()：统计源DStream中每个RDD的元素数量；
     * reduce(func)：利用函数func聚集源DStream中每个RDD的元素，返回一个包含单元素RDDs的新DStream；
     * countByValue()：应用于元素类型为K的DStream上，返回一个（K，V）键值对类型的新DStream，每个键的值是在原DStream的每个RDD中的出现次数；
     * reduceByKey(func, [numTasks])：当在一个由(K,V)键值对组成的DStream上执行该操作时，返回一个新的由(K,V)键值对组成的DStream，每一个key的值均由给定的recuce函数（func）聚集起来；
     * join(otherStream, [numTasks])：当应用于两个DStream（一个包含（K,V）键值对,一个包含(K,W)键值对），返回一个包含(K, (V, W))键值对的新DStream；
     * cogroup(otherStream, [numTasks])：当应用于两个DStream（一个包含（K,V）键值对,一个包含(K,W)键值对），返回一个包含(K, Seq[V], Seq[W])的元组；
     * transform(func)：将 DStream 转化成 RDD，通过对源DStream的每个RDD应用RDD-to-RDD函数，创建一个新的DStream。支持在新的DStream中做任何RDD操作。
     */

    /**
     * 一些窗口转换操作
     * window(windowLength,slideInterval): 返回一个基于源 DStream 的窗口批次计算得到新的 DStream
     * countByWindow(windowLength,slideInterval): 返回基于滑动窗口的 DStream 中的元素的数量
     * reduceByWindow(func,windowLength,slideInterval): 基于滑动窗口对源 DStream 中的元素进行聚合操作，得到一个新的 DStream
     * reduceByKeyAndWindow(func,windowLength,slideInterval,[numTasks]): 基于滑动窗口对 <K,V> 键值对类型的 DStream 中的值按 K 使用聚合函数 func 进行聚合操作，得到一个新的 DStream
     * reduceByKeyAndWindow(func,invFunc,windowLength,slideInterval,[numTasks]): 一个更高效的实现版本，先对滑动窗口中新的时间间隔内的数据进行增量聚合，再移去最早的同等时间间隔内的数据统计量。例如，计算 t+4 秒这个时刻过去 5 秒窗口的 WordCount 时，可以将 t+3 时刻过去 5 秒的统计量加上 [t+3,t+4] 的统计量，再减去 [t-2,t-1] 的统计量，这种方法可以复用中间 3 秒的统计量，提高统计的效率
     * countByValueAndWindow(windowLength,slideInterval,[numTasks]): 基于滑动窗口计算源 DStream 中每个 RDD 内每个元素出现的频次，并返回 DStream[<K,Long>]，其中，K 是 RDD 中元素的类型，Long 是元素频次。Reduce 任务的数量可以通过一个可选参数进行配置
     */

    /**
     * 输出操作
     * print(): 在 Driver 中打印出 DStream 中数据的前 10 个元素
     * saveAsTextFiles(prefix,[suffix]): 将 DStream 中的内容以文本的形式保存为文本文件，其中，每次批处理间隔内产生的文件以 prefix-TIME_IN_MS[.suffix] 的方式命名
     * saveAsObjectFiles(prefix,[suffix]): 将 DStream 中的内容按对象序列化，并且以 SequenceFile 的格式保存，其中，每次批处理间隔内产生的文件以 prefix—TIME_IN_MS[.suffix]的方式命名
     * saveAsHadoopFiles(prefix,[suffix]): 将 DStream 中的内容以文本的形式保存为 Hadoop 文件，其中，每次批处理间隔内产生的文件以 prefix-TIME_IN_MS[.suffix] 的方式命名
     * foreachRDD(func): 最基本的输出操作，将 func 函数应用于 DStream 中的 RDD 上，这个操作会输出数据到外部系统，例如，保存 RDD 到文件或者网络数据库等。需要注意的是，func 函数是在该 Streaming 应用的 Driver 进程里执行的
     */

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
        System.out.println("errorRDD 的总数为: " + errorRDDCount);

        // 从中取固定数目数据
        for (String rddLine : errorRDD.take(10)) {
            System.out.println("errorRDD的数据是:" + rddLine);
        }

        // 获得全部数据(能在单台服务器中放下)
        List<String> allData = errorRDD.collect();

        // 可以传入如下的具体类代替上面所示的匿名内部类

        //定义具体类
        class ContainsErrorDev implements Function<String, Boolean> {
            private String query;

            public ContainsErrorDev(String query) {
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
        System.out.println(StringUtils.join(result.collect(), ","));

        // 对每个输入元素生成多个输出元素。实现该功能的操作叫作 flatMap()
        JavaRDD<String> lines2 = sparkContext.parallelize(Arrays.asList("hello world", "hi"));

        JavaRDD<String> flatMapResult = lines2.flatMap(new FlatMapFunction<String, String>() {
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
        JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
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

            Function2<RddAvg, RddAvg, RddAvg> rddAvgFunction2 = new Function2<RddAvg, RddAvg, RddAvg>() {
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
