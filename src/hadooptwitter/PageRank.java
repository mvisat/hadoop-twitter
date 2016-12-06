/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadooptwitter;

import io.UserWritable;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import mapreduce.TopN;
import mapreduce.Preprocess;
import mapreduce.Iteration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author visat
 */
public class PageRank extends Configured implements Tool {
    private final int N_ITERATION = 3;
    private final int N_REDUCE = 16;

    private final String ME = "visat";
    private final String PREFIX = String.format("[%s] ", ME);

    private Configuration CONFIG;

    private String getRootDir() {
        return Path.SEPARATOR;
    }

    private String getUserDir() {
        return getRootDir().concat("user").concat(Path.SEPARATOR);
    }

    private String getMyDir() {
        return getUserDir().concat(ME).concat(Path.SEPARATOR);
    }

    private void doClean() throws IOException {
        FileSystem fs = FileSystem.get(CONFIG);
        String path = getMyDir();
        for (int i = 0; i <= N_ITERATION; ++i) {
            String dir = path.concat(String.format("iteration-%d", i));
            fs.delete(new Path(dir), true);
        }
        fs.delete(new Path(path.concat("result")), true);
    }

    private void doPreprocess(String in) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(CONFIG, PREFIX.concat("preprocess"));
        job.setJarByClass(PageRank.class);
        job.setMapperClass(Preprocess.CMapper.class);
        job.setReducerClass(Preprocess.CReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(UserWritable.class);

        String out = getMyDir().concat("iteration-0").concat(Path.SEPARATOR);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.out.println("Preprocess");
        System.out.println("Input: ".concat(in));
        System.out.println("Output: ".concat(out));

        job.setNumReduceTasks(N_REDUCE);
        job.waitForCompletion(true);
    }

    private void doIterate(int iteration) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(CONFIG, PREFIX.concat(String.format("iteration-%d", iteration)));
        job.setJarByClass(PageRank.class);
        job.setMapperClass(Iteration.CMapper.class);
        job.setReducerClass(Iteration.CReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(UserWritable.class);

        String in = getMyDir()
                .concat(String.format("iteration-%d", iteration-1))
                .concat(Path.SEPARATOR);
        String out = getMyDir()
                .concat(String.format("iteration-%d", iteration))
                .concat(Path.SEPARATOR);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.out.println("Iteration ".concat(String.valueOf(iteration)));
        System.out.println("Input: ".concat(in));
        System.out.println("Output: ".concat(out));

        job.setNumReduceTasks(N_REDUCE);
        job.waitForCompletion(true);
    }

    private void doTopN() throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(CONFIG, PREFIX.concat("top-n"));
        job.setJarByClass(PageRank.class);
        job.setMapperClass(TopN.CMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(TopN.CReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        String in = getMyDir()
                .concat(String.format("iteration-%d", N_ITERATION))
                .concat(Path.SEPARATOR);
        String out = getMyDir().concat("result");
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        System.out.println("Top N");
        System.out.println("Input: ".concat(in));
        System.out.println("Output: ".concat(out));

        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: HadoopTwitter <input-dir>");
            ToolRunner.printGenericCommandUsage(System.out);
            return 1;
        }

        CONFIG = getConf();
        try {
            doClean();
            doPreprocess(args[0]);
            for (int i = 1; i <= N_ITERATION; ++i) doIterate(i);
            doTopN();
            return 0;
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            Logger.getLogger(PageRank.class.getName()).log(Level.SEVERE, null, e);
        }
        return 1;
    }

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new PageRank(), args);
        System.exit(result);
    }
}
