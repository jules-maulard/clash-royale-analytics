package clash;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import clash.DataCleaning.CleaningMapper;
import clash.DataCleaning.DeduplicationReducer;
import clash.NodesEdges.NodesEdgesMapper;
import clash.NodesEdges.NodesEdgesCombiner;
import clash.NodesEdges.NodesEdgesReducer;
import clash.Stats.StatsReplicatedJoin;

public class ProjectDriver {

    public static void main(String[] args) throws Exception {

        List<String> positionalArgs = new ArrayList<>();
        boolean useCombiner = true; // default
        int minArchetypeSize = 8;
        String executionMode = "all"; // options: all, clean, graph, stats

        for (String argument : args) {
            if (argument.equalsIgnoreCase("-noCombiner")) {
                useCombiner = false;
            } else if (argument.startsWith("-minSize=")) {
                minArchetypeSize = Integer.parseInt(argument.split("=")[1]);
            } else if (argument.startsWith("-job=")) {
                executionMode = argument.split("=")[1].toLowerCase();
            } else {
                positionalArgs.add(argument);
            }
        }

        if (positionalArgs.size() < 2) {
            System.err.println("Usage: ProjectDriver <raw_input> <base_output_dir> [-noCombiner] [-minSize=X] [-job=clean|graph|stats|all]");
            System.exit(-1);
        }

        Path rawInput = new Path(positionalArgs.get(0));
        Path baseOutputDir = new Path(positionalArgs.get(1));

        Path cleanOutput = new Path(baseOutputDir, "clean");
        Path nodesEdgesOutput = new Path(baseOutputDir, "nodesEdges");
        Path finalOutput = new Path(baseOutputDir, "final");

        boolean runAll = executionMode.equals("all");

        // Job 1: Cleaning
        if (runAll || executionMode.equals("clean")) {
            boolean success = runCleaningJob(rawInput, cleanOutput);
            if (!success) {
                System.err.println("Data Cleaning Job failed");
                System.exit(1);
            }
        }

        // Job 2: Nodes & Edges
        if (runAll || executionMode.equals("graph")) {
            boolean success = runNodesEdgesJob(cleanOutput, nodesEdgesOutput, useCombiner, minArchetypeSize);
            if (!success) {
                System.err.println("Nodes & Edges Job failed");
                System.exit(1);
            }
        }

        // Job 3: Statistics
        if (runAll || executionMode.equals("stats")) {
            boolean success = runStatsJob(nodesEdgesOutput, finalOutput);
            if (!success) {
                System.err.println("Stats Job failed");
                System.exit(1);
            }
        }
    }


    private static boolean runCleaningJob(Path input, Path output) throws Exception {
        Configuration conf = new Configuration();
        String jobName = "Data Cleaning";
        System.out.println(">>> Starting Job: " + jobName);
        Job job = Job.getInstance(conf, jobName);

        job.setJarByClass(ProjectDriver.class);
        job.setMapperClass(CleaningMapper.class);
        job.setReducerClass(DeduplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job.waitForCompletion(true);
    }

    private static boolean runNodesEdgesJob(Path input, Path output, boolean useCombiner, int minArchetypeSize) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("clash.archetype.min.size", minArchetypeSize);
        String jobName = "Nodes & Edges [Combiner=" + (useCombiner ? "ON" : "OFF") + ", MinSize=" + minArchetypeSize + "]";
        System.out.println(">>> Starting Job: " + jobName);
        Job job = Job.getInstance(conf, jobName);

        job.setJarByClass(ProjectDriver.class);
        job.setMapperClass(NodesEdgesMapper.class);
        if (useCombiner) {
            job.setCombinerClass(NodesEdgesCombiner.class);
        }
        job.setReducerClass(NodesEdgesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        MultipleOutputs.addNamedOutput(job, "nodes", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "edges", TextOutputFormat.class, NullWritable.class, Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);
    }

    private static boolean runStatsJob(Path input, Path output) throws Exception {
        Configuration conf = new Configuration();
        String jobName = "Stats";
        System.out.println(">>> Starting Job: " + jobName);
        Job job = Job.getInstance(conf, jobName);

        job.setJarByClass(ProjectDriver.class);
        job.setMapperClass(StatsReplicatedJoin.class);
        job.setNumReduceTasks(0); // Map-only job

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.addCacheFile(new URI(input.toString() + "/nodes/part-r-00000#nodes-cache"));
        
        FileInputFormat.addInputPath(job, new Path(input, "edges"));
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);
    }
}