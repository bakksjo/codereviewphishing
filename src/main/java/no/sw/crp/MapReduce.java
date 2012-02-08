package no.sw.crp;

import java.util.ArrayList;
import java.util.Map;

public class MapReduce {
    Mapper mapper;
    Reducer reducer;

    public MapReduce(String mapper, String reducer) {
        if (mapper.equals("WordCountMapper")) {
            this.mapper = new WordCountMapper();
        } else if (mapper.equals("NoOpMapper")) {
            this.mapper = new NoOpMapper();
        }
        if (mapper.equals("WordCountMapper")) {
            this.reducer = new WordCountReducer();
        } else if (reducer.equals("NoOpReducer")) {
            this.reducer = new NoOpReducer();
        }
    }

    public void run(ArrayList<String> values) {
        // Phase 1: Map.
        for (int i = 0; i < values.size(); i++) {
            mapper.map(values.get(i));
        }
        // Phase 2: Reduce.
        for (final Map.Entry<String, ArrayList<String>> entry : mapper.output.entrySet()) {
            Thread reduceThread = new Thread() {
                    public void run() {
                        reducer.reduce(entry.getKey(), entry.getValue().toArray(new String[] {}));
                    }
                };
            reduceThread.start();
        }
    }
}