package com.pavlenko.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is the main Mapper class.
 *
 * @author Sergii Pavlenko
 */
public class DurationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Web app name
    private static final String WAR_FILE_NAME = "WebSiteAnalyzer.war";
    // Simple regexp to find a log of deploying WebSiteAnalyzer.war
    private static final String REGEXP = String.format("(Deployment of web application archive (.*)%s has finished in (\\d+) ms)", WAR_FILE_NAME);

    /**
     * Receives a line of input from the file, checks that it is deployment log and
     * extracts duration in milliseconds from it
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Pattern pattern = Pattern.compile(REGEXP);
        Matcher matcher = pattern.matcher(value.toString());

        if (!matcher.find()) {
            return;
        }

        // Getting duration from the log line
        String group = matcher.group(3);

        // Record the output in the Context object
        context.write(new Text(WAR_FILE_NAME), new IntWritable(Integer.valueOf(group)));
    }
}