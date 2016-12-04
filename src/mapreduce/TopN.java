/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduce;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author visat
 */
public class TopN {
    private static final int N = 5;

    protected static class MapEntryKeyComparator<
            K extends Comparable<K>, V extends Comparable<V>>
            implements Comparator<Map.Entry<K, V>> {

        @Override
	public int compare(Entry<K, V> o1, Entry<K, V> o2) {
            int ret = o1.getKey().compareTo(o2.getKey());
            return ret != 0 ? ret : o1.getValue().compareTo(o2.getValue());
	}
    }

    public static class CMapper extends Mapper<LongWritable, Text, DoubleWritable, LongWritable> {
        private PriorityQueue<Map.Entry<Double, Long>> topN;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            topN = new PriorityQueue<>(N, new MapEntryKeyComparator<Double, Long>());
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            Long userId = Long.parseLong(tokenizer.nextToken());
            Double pageRank = Double.parseDouble(tokenizer.nextToken());

            if (topN.size() < N || pageRank >= topN.peek().getKey()) {
                topN.add(new AbstractMap.SimpleEntry<>(pageRank, userId));
                if (topN.size() > N)
                    topN.poll();
            }
        }

        @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Double, Long> entry: topN) {
                context.write(new DoubleWritable(entry.getKey()),
                        new LongWritable(entry.getValue()));
            }
	}
    }

    public static class CReducer extends Reducer<DoubleWritable, LongWritable, LongWritable, DoubleWritable> {
        private PriorityQueue<Map.Entry<Double, Long>> topN;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            topN = new PriorityQueue<>(N, new MapEntryKeyComparator<Double, Long>());
        }

        @Override
        public void reduce(DoubleWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value: values) {
                Long userId = value.get();
                Double pageRank = key.get();
                if (topN.size() < N || pageRank >= topN.peek().getKey()) {
                    topN.add(new AbstractMap.SimpleEntry<>(pageRank, userId));
                    if (topN.size() > N)
                        topN.poll();
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<LongWritable, DoubleWritable>> top = new ArrayList<>();
            for (int i = 0; i < N; ++i) {
                Map.Entry<Double, Long> entry = topN.poll();
                top.add(new AbstractMap.SimpleEntry<>(new LongWritable(entry.getValue()),
                        new DoubleWritable(entry.getKey())));
            }
            for (int i = top.size()-1; i >= 0; --i) {
                Map.Entry<LongWritable, DoubleWritable> entry = top.get(i);
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }
}
