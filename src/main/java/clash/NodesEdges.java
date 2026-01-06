package clash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

public class NodesEdges {
    

    public static class NodesEdgesMapper
        extends Mapper<LongWritable, Text, Text, Text>{

        private final ObjectMapper objectMapper = new ObjectMapper();
        private int minArchetypeSize;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minArchetypeSize = context.getConfiguration().getInt("clash.archetype.min.size", 8);
        }
        
        @Override
        protected void map(
            LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String jsonLine = value.toString();
            JsonNode matchNode = parseJson(jsonLine);
            if (matchNode == null) return; // invalid JSON format

            JsonNode player1Node = matchNode.get("players").get(0);
            JsonNode player2Node = matchNode.get("players").get(1);

            int winnerIndex = matchNode.get("winner").asInt();
            boolean player1Wins = (winnerIndex == 0);
            boolean player2Wins = (winnerIndex == 1);
            
            String player1SortedDeck = sortDeckCards(player1Node.get("deck").asText());
            String player2SortedDeck = sortDeckCards(player2Node.get("deck").asText());

            for (int size = minArchetypeSize; size <= 8; size++) {
                List<String> player1Archetypes = getArchetypesOfSize(player1SortedDeck, size);
                List<String> player2Archetypes = getArchetypesOfSize(player2SortedDeck, size);

                // Nodes
                for (String archetype : player1Archetypes) {
                    context.write(new Text("N" + archetype), new Text(player1Wins ? "1" : "0"));
                }
                for (String archetype : player2Archetypes) {
                    context.write(new Text("N" + archetype), new Text(player2Wins ? "1" : "0"));
                }

                // Edges
                for (String archetype1 : player1Archetypes) {
                    for (String archetype2 : player2Archetypes) {
                        if (archetype1.compareTo(archetype2) < 0) {
                            context.write(new Text("E" + archetype1 + ";" + archetype2), new Text(player1Wins ? "1" : "0"));
                        } else {
                            context.write(new Text("E" + archetype2 + ";" + archetype1), new Text(player2Wins ? "1" : "0"));
                        }
                    }
                }
            }
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

        private String sortDeckCards(String deck) {
            String[] cards = new String[8];
            for (int i = 0; i < 8; i++) {
                cards[i] = deck.substring(i * 2, i * 2 + 2);
            }
            Arrays.sort(cards);
            return String.join("", cards);
        }

        private List<String> getArchetypesOfSize(String sortedDeck, int targetSize) {
            List<String> archetypes = new ArrayList<>();
            
            String[] cards = new String[8];
            for (int i = 0; i < 8; i++) cards[i] = sortedDeck.substring(i * 2, i * 2 + 2);

            int maxCombinations = 1 << 8; // 256
            for (int mask = 0; mask < maxCombinations; mask++) {
                
                if (Integer.bitCount(mask) != targetSize) continue;

                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < 8; j++) {
                    if ((mask & (1 << j)) != 0) {
                        sb.append(cards[j]);
                    }
                }
                archetypes.add(sb.toString());
            }
            return archetypes;
        }
    }
    


    public static class NodesEdgesCombiner
        extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(
            Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {

            if (!isNodeKey(key) && !isEdgeKey(key)) return;

            int count = 0;
            int win = 0;

            for (Text val : values) {
                count++;
                if (val.toString().equals("1")) win++;
            }
            
            context.write(key, new Text(count + ";" + win));
        }
    }



    public static class NodesEdgesReducer
        extends Reducer<Text,Text,NullWritable,Text> {

        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(
            Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {

            if (!isNodeKey(key) && !isEdgeKey(key)) return;

            int count = 0;
            int win = 0;

            for (Text val : values) {
                String stringVal = val.toString();

                if (stringVal.contains(";")) { // from combiner
                    String[] parts = stringVal.split(";");
                    count += Integer.parseInt(parts[0]);
                    win += Integer.parseInt(parts[1]);
                
                } else { // direct from mapper
                    count++;
                    if (stringVal.equals("1")) win++;
                }
            }
            
            String cleanKey = key.toString().substring(1);
            Text statsLine = new Text(cleanKey + ";" + count + ";" + win);

            if (isNodeKey(key)) {
                multipleOutputs.write("nodes", NullWritable.get(), statsLine, "nodes/part");
            } else {
                multipleOutputs.write("edges", NullWritable.get(), statsLine, "edges/part");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }


    private static boolean isNodeKey(Text key) {
        return key.toString().startsWith("N");
    }

    private static boolean isEdgeKey(Text key) {
        return key.toString().startsWith("E");
    }
}
