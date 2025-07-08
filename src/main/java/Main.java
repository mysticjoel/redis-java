import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        int port = 6379;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Server started. Listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();

                // Handle client in a new thread using lambda
                new Thread(() -> {
                    try (
                            InputStream inputStream = clientSocket.getInputStream();
                            OutputStream outputStream = clientSocket.getOutputStream()
                    ) {
                        byte[] buffer = new byte[1024];
                        int read = inputStream.read(buffer);
                        if (read > 0) {
                            String input = new String(buffer, 0, read).trim();
                            System.out.println("Received: " + input);

                            if (input.contains("PING")) {
                                outputStream.write("+PONG\r\n".getBytes());
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("IOException in client thread: " + e.getMessage());
                    } finally {
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            System.err.println("Error closing socket: " + e.getMessage());
                        }
                    }
                }).start();
            }

        } catch (IOException e) {
            System.err.println("IOException in main: " + e.getMessage());
        }
    }
}
