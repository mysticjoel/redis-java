import java.io.*;

public class RDBParser {

    public static void load(File rdbFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(rdbFile);
             DataInputStream dis = new DataInputStream(new BufferedInputStream(fis))) {

            // Skip header
            byte[] header = new byte[9];
            dis.readFully(header);

            while (true) {
                int type;
                try {
                    type = dis.readUnsignedByte();
                } catch (EOFException e) {
                    break;
                }

                // Handle expire first
                long expireTime = -1;
                while (type == 0xFD || type == 0xFC) {
                    if (type == 0xFD) {
                        int seconds = Integer.reverseBytes(dis.readInt());
                        expireTime = seconds * 1000L;
                    } else if (type == 0xFC) {
                        long millis = Long.reverseBytes(dis.readLong());
                        expireTime = millis;
                    }
                    type = dis.readUnsignedByte(); // Next type after expiration
                }

                if (type == 0xFA) { // Metadata
                    readString(dis); // key
                    readString(dis); // value
                    continue;
                }

                if (type == 0xFB) { // RESIZEDB
                    readLength(dis); // db_size
                    readLength(dis); // expires_size
                    continue;
                }

                if (type == 0xFE) { // SELECTDB
                    readLength(dis);
                    continue;
                }

                if (type == 0xFF) { // EOF
                    break;
                }

                if (type == 0x00) { // String key-value
                    String key = readString(dis);
                    String value = readString(dis);
                    ClientHandler.map.put(key, value);
                    if (expireTime != -1) {
                        ClientHandler.expiryMap.put(key, expireTime);
                    }
                    continue;
                }

                System.out.println("Unsupported type: " + type);
            }
        }
    }

    private static int readLength(DataInputStream dis) throws IOException {
        int first = dis.readUnsignedByte();
        int type = (first & 0xC0) >> 6;

        if (type == 0) {
            return first & 0x3F;
        } else if (type == 1) {
            int second = dis.readUnsignedByte();
            return ((first & 0x3F) << 8) | second;
        } else if (type == 2) {
            return dis.readInt(); // already big-endian
        } else {
            throw new IOException("Unsupported length encoding");
        }
    }

    private static String readString(DataInputStream dis) throws IOException {
        int first = dis.readUnsignedByte();
        int prefix = (first & 0xC0) >> 6;

        if (prefix == 0) {
            // 6-bit length
            int length = first & 0x3F;
            byte[] bytes = new byte[length];
            dis.readFully(bytes);
            return new String(bytes);
        } else if (prefix == 1) {
            // 14-bit length
            int second = dis.readUnsignedByte();
            int length = ((first & 0x3F) << 8) | second;
            byte[] bytes = new byte[length];
            dis.readFully(bytes);
            return new String(bytes);
        } else if (prefix == 2) {
            // 32-bit length
            int length = dis.readInt(); // big-endian by default
            byte[] bytes = new byte[length];
            dis.readFully(bytes);
            return new String(bytes);
        } else {
            // Special encoding
            int encType = first & 0x3F;

            switch (encType) {
                case 0: // 8-bit int
                    return String.valueOf(dis.readByte());
                case 1: // 16-bit int (little-endian)
                    return String.valueOf(Short.reverseBytes(dis.readShort()));
                case 2: // 32-bit int (little-endian)
                    return String.valueOf(Integer.reverseBytes(dis.readInt()));
                default:
                    throw new IOException("Unsupported string encoding type: " + encType);
            }
        }
    }

}
