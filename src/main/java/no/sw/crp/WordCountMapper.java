package no.sw.crp;

public class WordCountMapper extends Mapper {
    public void map(String input) {
        // Find words, separated by spaces.
        int index = 0;
        for (;;) {
            if (input.indexOf(' ', index) != -1) {
                String word = input.substring(index, input.indexOf(' ', index));
                emit(word, "1");
                index = input.indexOf(' ', index) + 1; // Move past the word and the space character.
            } else {
                emit(input.substring(index), "1");
                return;
            }
        }
    }
}
