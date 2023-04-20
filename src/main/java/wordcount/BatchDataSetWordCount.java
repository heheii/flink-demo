package wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * dataSet api
 * 不推荐使用
 */
@Deprecated
public class BatchDataSetWordCount {

    public static void main(String[] args) {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取数据
        DataSource<String> stringDataSource = env.readTextFile("src/main/resources/words.txt");
        // 分词
        FlatMapOperator<String, Tuple2<String, Long>> returns = stringDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        try {
            returns.groupBy(0).sum(1).print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
