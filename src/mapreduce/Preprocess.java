/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduce;

import io.UserWritable;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author visat
 */
public class Preprocess {

    public static class CMapper extends Mapper<Object, Text, LongWritable, UserWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String userId = tokenizer.nextToken();
            String followerId = tokenizer.nextToken();

            UserWritable followee = new UserWritable(userId);
            context.write(new LongWritable(Long.parseLong(followerId)), followee);

            UserWritable empty = new UserWritable();
            context.write(new LongWritable(Long.parseLong(userId)), empty);
        }
    }

    public static class CReducer extends Reducer<LongWritable, UserWritable, LongWritable, UserWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<UserWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (UserWritable value: values) {
                if (!value.getFollowings().toString().isEmpty())
                    sb.append(value.getFollowings()).append(',');
            }
            if (sb.length() == 0)
                sb.append(',');
            UserWritable user = new UserWritable(sb.toString());
            context.write(key, user);
        }
    }
}
