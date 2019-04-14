package cn.it.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.fasterxml.jackson.databind.PropertyNamingStrategy;

import java.io.IOException;

/**
 * author: DahongZhou
 * Date:
 */
public class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable longWritable = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int a = 0;
        for (LongWritable value : values) {
            a += value.get();
            longWritable.set(a);
        }
        context.write(key, longWritable);
    }
}
