package cn.it.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * author: DahongZhou
 * Date:
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text text = new Text();
    LongWritable longWritable = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        for (String word : split) {
            text.set(word);
            longWritable.set(1);
            context.write(text, longWritable);
        }
    }
}
