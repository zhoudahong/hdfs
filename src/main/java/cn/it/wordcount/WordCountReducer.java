package cn.it.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 我们自定的class类继承reducer类表明我们这是一个标准的reducer类
 * 跟k2  v2   k3  v3  四个泛型
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable intWritable = new IntWritable();

    /**
     * 覆写reduce方法，
     *
     * @param key     接收的key  是K2
     * @param values  接收到value是一个集合  集合里面的数据类型是  v2 类型
     * @param context 上下文对象，将数据往外写
     * @throws IOException
     * @throws InterruptedException
     */

       /*
      hello,world,hadoop
     hive,sqoop,flume,hello
     kitty,tom,jerry,world
      hadoop

      结果数据
      hello  <1,1>
      world  <1,1>
      hadoop <1,1>
        */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int a = 0;
        for (IntWritable value : values) {
            int i = value.get();//将IntWritable类型的value转换为int类型
            a += i;
        }
        intWritable.set(a);
        context.write(key, intWritable);
    }
}
