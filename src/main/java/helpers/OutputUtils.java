package helpers;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class OutputUtils {

    private static final char DEFAULT_SEPARATOR = ',';
    private static FileWriter w = null;
    
    public static void writerInit(String output) throws IOException {
    	w = new FileWriter(output);
    }
    
    public static void writerCleanup() throws IOException {
    	w.flush();
        w.close();
    }
    
    public static void writeLine(List<String> values) throws IOException {
        writeLine(values, DEFAULT_SEPARATOR, ' ');
    }

    public static void writeLine(List<String> values, char separators) throws IOException {
        writeLine(values, separators, ' ');
    }

    private static String followCVSformat(String value) {

        String result = value;
        if (result.contains("\"")) {
            result = result.replace("\"", "\"\"");
        }
        return result;

    }

    public static void writeLine(List<String> values, char separators, char customQuote) throws IOException {

        boolean first = true;

        if (separators == ' ') {
            separators = DEFAULT_SEPARATOR;
        }

        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            if (!first) {
                sb.append(separators);
            }
            if (customQuote == ' ') {
                sb.append(followCVSformat(value));
            } else {
                sb.append(customQuote).append(followCVSformat(value)).append(customQuote);
            }

            first = false;
        }
        sb.append("\n");
        w.append(sb.toString());
    }
}
