package clash;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Stats {
    
    public static class StatsReplicatedJoin extends Mapper<Object, Text, NullWritable, Text> {

        private Map<String, Long> nodesMap = new HashMap<>();
        private long[] totalCountByArchetypeSize = new long[9];

        private static final int MIN_COUNT_THRESHOLD = 10;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("Nodes cache file not found in Distributed Cache");
            }
            
            try (BufferedReader reader = new BufferedReader(new FileReader("nodes-cache"))) {

                String line;
                while ((line = reader.readLine()) != null) {

                    String[] parts = line.split(";");
                    if (parts.length < 3) continue;

                    String archetype = parts[0];
                    long count = Long.parseLong(parts[1]);
                    
                    int size = archetype.length() / 2;
                    totalCountByArchetypeSize[size] += count;

                    if (count >= MIN_COUNT_THRESHOLD) {
                        nodesMap.put(archetype, count);
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split(";");
            if (parts.length < 4) return;

            String archSource = parts[0];
            String archTarget = parts[1];
            long edgeCount = Long.parseLong(parts[2]);
            long edgeWin = Long.parseLong(parts[3]);

            if (
                !nodesMap.containsKey(archSource) || 
                !nodesMap.containsKey(archTarget)
            ) {
                return;
            }

            long countSource = nodesMap.get(archSource);
            long countTarget = nodesMap.get(archTarget);
            
            int archetypeSize = archSource.length() / 2;
            double prediction = (double) (countSource * countTarget) / totalCountByArchetypeSize[archetypeSize];

            StringBuilder sb = new StringBuilder();
            sb.append(archSource).append(";").append(archTarget).append(";")
              .append(edgeCount).append(";").append(edgeWin).append(";")
              .append(countSource).append(";").append(countTarget).append(";")
              .append(String.format("%.2f", prediction));

            context.write(NullWritable.get(), new Text(sb.toString()));
        }
    }

}
