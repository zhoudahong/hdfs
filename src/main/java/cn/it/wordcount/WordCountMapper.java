package cn.it.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper类继承Mapper  表示这个class类是一个标准的mapper类，需要四个泛型（hadoop的数据类型，而不是java的数据类型）
 * k1  v1   k2  v2
 * LongWritable表示行偏移量的数据类型,Text,Text,IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //避免频繁创建对象
    Text text = new Text();
    IntWritable intWritable = new IntWritable();

    /**
     * 覆写父类的map方法，每一行数据要调用一次map方法，处理逻辑都写在这个map方法里面
     * hdfs的最原始数据
     hello,world,hadoop
     hive,sqoop,flume,hello
     kitty,tom,jerry,world
     hadoop

     经过第一步：TextInputFormat之后
     key（行偏移量）   value（每行的内容）
     0      hello,world,hadoop
     18     hive,sqoop,flume,hello
     40     kitty,tom,jerry,world
     61     hadoop
     *
     */

    /**
     * 覆写父类的map方法，每一行数据要调用一次map方法，处理逻辑都写在这个map方法里面
     *
     * @param key     key1   行偏移量 ，一般没啥用，直接可以丢掉
     * @param value   value1   行文本内容，需要切割，然后转换成新的k2  v2  输出
     * @param context 上下文对象，承接上文，把数据传输给下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       /*
      hello,world,hadoop
     hive,sqoop,flume,hello
     kitty,tom,jerry,world
      hadoop
        */
        String line = value.toString();
        String[] split = line.split(",");
        //遍历我们切割出来的单词数组
        for (String word : split) {
            text.set(word);//将单词内容设置为text
            intWritable.set(1);//每个单词算一次
            //写出k2  v2  这里的类型跟k2  v2  保持一致
            context.write(text, intWritable);
        }
    }
}
