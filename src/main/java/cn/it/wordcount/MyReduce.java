package cn.it.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * author: DahongZhou
 * Date:
 */
public class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable longWritable = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int a = 0;
        for (LongWritable value : values) {
            long l = value.get();
            a += l;
        }
        longWritable.set(a);
        context.write(key, longWritable);
    }
}
