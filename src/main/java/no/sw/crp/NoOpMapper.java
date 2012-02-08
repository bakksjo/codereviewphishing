package no.sw.crp;

// Mapper that does nothing special.
public class NoOpMapper extends Mapper {
    public void map(String input) {
        emit("key", input);
    }
}
