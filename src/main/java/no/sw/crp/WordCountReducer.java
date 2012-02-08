package no.sw.crp;

public class WordCountReducer extends Reducer {
    public void reduce(String key, String[] values) {
        String string = "0";
        for (int i = 0; i < values.length; i++) {
            // Sum each value.
            string = String.valueOf(Integer.valueOf(string) + Integer.valueOf(values[i]));
        }
        emit(key, string);
    }
}

