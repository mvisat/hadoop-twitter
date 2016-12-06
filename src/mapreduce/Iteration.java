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
public class Iteration {

    public static class CMapper extends Mapper<LongWritable, Text, LongWritable, UserWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String userId = tokenizer.nextToken();
            String pageRank = tokenizer.nextToken();
            String followings = tokenizer.nextToken();
            String[] followingArray = followings.split(",");

            int n = followingArray.length;
            double pageRankAdd = n > 0 ? Double.parseDouble(pageRank)/(double)n : 0.0;
            for (String following: followingArray) {
                LongWritable followingId = new LongWritable(Long.valueOf(following));
                UserWritable followingUser = new UserWritable(pageRankAdd);
                context.write(followingId, followingUser);
            }

            UserWritable user = new UserWritable(followings);
            context.write(new LongWritable(Long.parseLong(userId)), user);
        }
    }

    public static class CReducer extends Reducer<LongWritable, UserWritable, LongWritable, UserWritable> {

        private static final Double D = 0.85;

        @Override
        public void reduce(LongWritable key, Iterable<UserWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            StringBuilder sb = new StringBuilder();
            for (UserWritable user: values) {
                if (user.getFollowings().toString().isEmpty())
                    sum += user.getPageRank();
                else
                    sb.append(user.getFollowings());
            }
            double pageRank = (1.0-D) + D * sum;
            UserWritable user = new UserWritable(pageRank, sb.toString());
            context.write(key, user);
        }
    }
}
