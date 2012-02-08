package no.sw.crp;

// Reducer that does nothing.
public class NoOpReducer extends Reducer {
    public void reduce(String key, String[] values) {
        for (int i = 0; i < values.length; ++i) {
            emit(key, values[i]);
        }
    }
}

