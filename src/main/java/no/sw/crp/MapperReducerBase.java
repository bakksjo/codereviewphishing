package no.sw.crp;

import java.util.TreeMap;
import java.util.ArrayList;

public class MapperReducerBase {
    // Output map that automatically sorts by key.
    TreeMap<String, ArrayList<String>> output = new TreeMap<String, ArrayList<String>>();

    // Called from map() to emit data.
    public void emit(String key, String value) {
        ArrayList<String> out = output.get(key);
        if (out != null) {
            out.add(value);
        } else {
            out = new ArrayList<String>();
            out.add(value);
            output.put(key, out);
        }
    }
}
