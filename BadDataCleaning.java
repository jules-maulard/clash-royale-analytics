package clash;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

public class BadDataCleaning {

    /* 
    {
        "date": "2025-11-11T15:47:37Z",
        "game": "pathOfLegend",
        "mode": "Ranked1v1_NewArena2",
        "round": 0,
        "type": "pathOfLegend",
        "winner": 0,
        "players": 
        [
            {
                "utag": "#CJR2U0P80",
                "ctag":"#2L022JC0",
                "trophies":10000,
                "exp":56,
                "league":4,
                "bestleague":8,
                "deck":"0a0e151e264d5a65",
                "evo":"0a0e",
                "tower":"6e",
                "strength":15.25,
                "crown":1,
                "elixir":0.99,
                "touch":1,
                "score":0
            },
            {
                "utag":"#U8Y0RY09",
                "ctag":"#YYGV92UQ",
                "trophies":10000,
                "exp":62,
                "league":4,
                "bestleague":10,
                "deck":"0a1e3b454d65686c",
                "evo":"3b4d",
                "tower":"6e",
                "strength":15.125,
                "crown":0,
                "elixir":6.96,
                "touch":1,
                "score":0
            }
        ]
    }
    */

    public static class DataCleaningMapper
        extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(
            LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            
            String line = value.toString();
            JsonNode gameNode = parseAndValidate(line);
            if (gameNode == null || !hasValidDecks(gameNode)) {
                return;
            }

            List<Text> canonicalKeys = generateCanonicalKey(gameNode);
            for (Text canonicalKey : canonicalKeys) {
                context.write(canonicalKey, value);
            }
        }

        
        private JsonNode parseAndValidate(String content) {
            if (content == null || content.trim().isEmpty()) {
                return null;
            }

            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode node  = mapper.readTree(content);
                return node;
            } catch (Exception e) {
                return null;
            }
        }

        private boolean hasValidDecks(JsonNode node) {
            Integer numberCardPlayer1 = node.get("players").get(0).get("deck").asText().length() / 2;
            Integer numberCardPlayer2 = node.get("players").get(1).get("deck").asText().length() / 2;
            if (numberCardPlayer1 != 8 || numberCardPlayer2 != 8) {
                return false;
            }
            return true;
        }

        private List<Text> generateCanonicalKey(JsonNode node) {
            String p1 = node.get("players").get(0).get("utag").asText();
            String p2 = node.get("players").get(1).get("utag").asText();
            String firstPlayer = (p1.compareTo(p2) < 0) ? p1 : p2;
            String secondPlayer = (p1.compareTo(p2) < 0) ? p2 : p1;

            Integer round = node.get("round").asInt();

            String commonPrefix = firstPlayer + "|" + secondPlayer + "|" + round + "|";

            String exactDate = node.get("date").asText();
            long timestamp = Instant.parse(exactDate).toEpochMilli();
            long windowSize = 5000;

            long bucket = timestamp / windowSize;
            Text canonicKey = new Text(commonPrefix + bucket);
            long shiftedBucket = (timestamp - (windowSize / 2)) / windowSize;
            Text shiftedCanonicKey = new Text(commonPrefix + "S" + shiftedBucket);
            
            return Arrays.asList(canonicKey, shiftedCanonicKey);
        }
    }
    

    
    public static class DataCleaningReducer
        extends Reducer<Text,Text,NullWritable,Text> {

        @Override
        public void reduce(
            Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            Set<String> uniqueGames = new HashSet<>();
            String candidateJson = null;
            
            for (Text val : values) {
                String currentJson = val.toString();
                if (!uniqueGames.contains(currentJson)) {
                    uniqueGames.add(currentJson);

                    if (candidateJson == null || currentJson.compareTo(candidateJson) < 0) {
                        candidateJson = currentJson;
                    }
                }
            }
            if (uniqueGames.size() >= 2) {
                context.write(NullWritable.get(), new Text(candidateJson));
            }
        }
    }



    // uniq
    public static class UniqMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class UniqReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) 
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), key);
        }
    }


    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path uniqPath = new Path(args[1] + "_temp_uniq");
        Path outputPath = new Path(args[1]);




        Configuration conf = new Configuration();
        Job cleaningJob = Job.getInstance(conf, "Cleaning");
        cleaningJob.setJarByClass(BadDataCleaning.class);

        cleaningJob.setMapperClass(DataCleaningMapper.class);
        cleaningJob.setMapOutputKeyClass(Text.class);
        cleaningJob.setMapOutputValueClass(Text.class);
        
        cleaningJob.setReducerClass(DataCleaningReducer.class);
        // cleaningJob.setNumReduceTasks(1);
        cleaningJob.setOutputKeyClass(NullWritable.class);
        cleaningJob.setOutputValueClass(Text.class);

        cleaningJob.setInputFormatClass(TextInputFormat.class);
        cleaningJob.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(cleaningJob, inputPath);
        FileOutputFormat.setOutputPath(cleaningJob, uniqPath);



        boolean job1Success = cleaningJob.waitForCompletion(true);
        if (!job1Success) System.exit(1);

        Job uniqJob = Job.getInstance(conf, "Cleaning (Uniq)");
        uniqJob.setJarByClass(BadDataCleaning.class);  

        uniqJob.setMapperClass(UniqMapper.class);
        uniqJob.setMapOutputKeyClass(Text.class);
        uniqJob.setMapOutputValueClass(NullWritable.class);

        uniqJob.setReducerClass(UniqReducer.class);
        uniqJob.setOutputKeyClass(NullWritable.class);
        uniqJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(uniqJob, uniqPath); 
        FileOutputFormat.setOutputPath(uniqJob, outputPath);

        boolean uniqJobSuccess = uniqJob.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(uniqPath, true);

        System.exit(uniqJobSuccess ? 0 : 1);
    }

}