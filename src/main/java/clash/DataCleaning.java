package clash;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

public class DataCleaning {


    public static class CleaningMapper
        extends Mapper<LongWritable, Text, Text, Text>{

        private final ObjectMapper objectMapper = new ObjectMapper();


        @Override
        protected void map(
            LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            
            String jsonLine = value.toString();
            JsonNode matchNode = parseJson(jsonLine);

            if (matchNode == null) return; // invalid JSON format
            if (!hasValidFields(matchNode)) return;
            if (!isValidDeckSize(matchNode)) return; 

            Text canonicalKey = createCanonicalKey(matchNode);
            context.write(canonicalKey, value);
        }

        
        private JsonNode parseJson(String content) {
            if (content == null || content.trim().isEmpty()) {
                return null;
            }

            try {
                return objectMapper.readTree(content);
            } catch (Exception e) {
                return null;
            }
        }

        private boolean hasValidFields(JsonNode node) {
            if (
                !node.has("date") || 
                !node.has("round") || 
                !node.has("winner") ||
                !node.has("players")
            ) {
                return false;
            }

            if (
                !node.get("players").isArray() || 
                node.get("players").size() != 2
            ) {
                return false;
            }

            JsonNode p1 = node.get("players").get(0);
            JsonNode p2 = node.get("players").get(1);

            if (
                !p1.has("utag") || !p1.has("deck") || 
                !p2.has("utag") || !p2.has("deck")
            ) {
                return false;
            }

            return true;
        }

        private boolean isValidDeckSize(JsonNode node) {
            Integer player1DeckSize = node.get("players").get(0).get("deck").asText().length();
            Integer player2DeckSize = node.get("players").get(1).get("deck").asText().length();
            return  player1DeckSize == 16 && player2DeckSize == 16;
        }

        private Text createCanonicalKey(JsonNode node) {
            String player1Tag = node.get("players").get(0).get("utag").asText();
            String player2Tag = node.get("players").get(1).get("utag").asText();
            String firstTag = (player1Tag.compareTo(player2Tag) < 0) ? player1Tag : player2Tag;
            String secondTag = (player1Tag.compareTo(player2Tag) < 0) ? player2Tag : player1Tag;

            Integer round = node.get("round").asInt();

            String key = firstTag + "|" + secondTag + "|" + round;
            return new Text(key);
        }
    }
    

    
    public static class DeduplicationReducer
        extends Reducer<Text,Text,NullWritable,Text> {

        private final ObjectMapper objectMapper = new ObjectMapper();
        private static final long TIME_THRESHOLD_SECONDS = 3;
        
        @Override
        public void reduce(
            Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            
            List<String> uniqueMatches = collectUniqueMatches(values);
            sortMatchesChronologically(uniqueMatches);

            // ignore games with close timestamps (3 seconds)
            Instant previousMatchTime = null;
            for (String matchJson : uniqueMatches) {
                Instant currentMatchTime = Instant.parse(getDateString(matchJson));

                if (
                    previousMatchTime == null || 
                    currentMatchTime.isAfter(previousMatchTime.plusSeconds(TIME_THRESHOLD_SECONDS))
                ) {    
                    context.write(NullWritable.get(), new Text(matchJson));
                    previousMatchTime = currentMatchTime;
                }
            }
        }

        private List<String> collectUniqueMatches(Iterable<Text> values) {
            Set<String> uniqueSet = new HashSet<>();
            for (Text val : values) {
                uniqueSet.add(val.toString());
            }
            return new ArrayList<>(uniqueSet);
        }

        private String getDateString(String json) {
            try {
                return objectMapper.readTree(json).get("date").asText();
            } catch (Exception e) {
                return "";
            }
        }

        private void sortMatchesChronologically(List<String> matches) {
            Collections.sort(matches, (m1, m2) -> {
                String d1 = getDateString(m1);
                String d2 = getDateString(m2);
                return d1.compareTo(d2);
            });
        }
    }
}