import java.io.*;
import java.util.*;

public class RESPParser {
    public static class ParseResult {
        List<String> command;
        int bytesConsumed;

        ParseResult(List<String> command, int bytesConsumed) {
            this.command = command;
            this.bytesConsumed = bytesConsumed;
        }
    }

    public static ParseResult parseRESP(InputStream in) throws IOException {
        List<String> result = new ArrayList<>();
        int bytesRead = 0;
        DataInputStream reader = new DataInputStream(in);

        int b = reader.read();
        bytesRead++;
        if ((char) b != '*') {
            throw new IOException("Expected RESP array (starts with '*')");
        }

        String numArgsLine = readLine(reader);
        bytesRead += numArgsLine.length() + 2;
        int numArgs = Integer.parseInt(numArgsLine);

        for (int i = 0; i < numArgs; i++) {
            char prefix = (char) reader.read();
            bytesRead++;
            if (prefix != '$') {
                throw new IOException("Expected bulk string (starts with '$')");
            }

            String lenLine = readLine(reader);
            bytesRead += lenLine.length() + 2;
            int length = Integer.parseInt(lenLine);

            byte[] buf = new byte[length];
            reader.readFully(buf);
            bytesRead += length;
            result.add(new String(buf));

            readLine(reader);
            bytesRead += 2;
        }

        return new ParseResult(result, bytesRead);
    }

    private static String readLine(DataInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            char c = (char) in.readByte();
            if (c == '\r') {
                if ((char) in.readByte() == '\n') break;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    public static String buildArray(String... args) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            sb.append(buildBulkString(arg));
        }
        return sb.toString();
    }

    public static String buildBulkString(String value) {
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }
}