package com.pavlenko.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * This is the main Reducer class.
 *
 * @author Sergii Pavlenko
 */
public class DurationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Iterates through all starting durations to find the maximum, minimum and average value.
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        // Finding the max, min and avg values
        int maxDuration = Integer.MIN_VALUE;
        int minDuration = Integer.MAX_VALUE;
        int avgDuration = 0;
        int size = 0;
        for (IntWritable value : values) {
            int duration = value.get();
            maxDuration = Math.max(maxDuration, duration);
            minDuration = Math.min(minDuration, duration);
            size++;
            avgDuration += duration;
        }

        context.write(new Text("Max:"), new IntWritable(maxDuration));
        context.write(new Text("Avg:"), new IntWritable(avgDuration / size));
        context.write(new Text("Min:"), new IntWritable(minDuration));
    }
}
