package com.pavlenko.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

/**
 * The main application class.
 *
 * @author Sergii Pavlenko
 */
public class Application {
    /**
     * Application entry point.
     */
    public static void main(String[] args) throws Exception {
        // Create the job specification object
        Job job = new Job();
        job.setJarByClass(Application.class);
        job.setJobName("Loading duration measurement");

        // Setup input and output paths
        String logPath = job.getClass().getClassLoader().getResource("log").getPath();
        String outputPath = logPath + "/output";
        deleteFolderIfExists(outputPath);
        FileInputFormat.addInputPath(job, new Path(logPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Set the Mapper and Reducer classes
        job.setMapperClass(DurationMapper.class);
        job.setReducerClass(DurationReducer.class);

        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Wait for the job to finish before terminating
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void deleteFolderIfExists(String outputPath) {
        File index = new File(outputPath);
        if (!index.exists()) {
            return;
        }
        String[] entries = index.list();
        for (String s : entries) {
            File currentFile = new File(index.getPath(), s);
            currentFile.delete();
        }

        index.delete();
    }
}
