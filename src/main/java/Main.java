import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) {
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Server listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                // Handle each client in a new thread
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}

class ClientHandler implements Runnable {
    private Socket clientSocket;

    public ClientHandler(Socket socket) {
        this.clientSocket = socket;
    }

    public void run() {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                OutputStream writer = clientSocket.getOutputStream()
        ) {
            String line;
            HashMap<String,String> map = new HashMap<>();
            while ((line = reader.readLine()) != null) {
                if (line.trim().equalsIgnoreCase("PING")) {
                    writer.write("+PONG\r\n".getBytes());
                    continue;
                }

                if (line.trim().equalsIgnoreCase("ECHO")) {
                    // Read next 2 lines for bulk string:
                    reader.readLine(); // skip $length
                    String value = reader.readLine(); // get the actual value
                    String response = "$" + value.length() + "\r\n" + value + "\r\n";
                    writer.write(response.getBytes());
                }
                if(line.trim().equalsIgnoreCase("SET")){
                    System.out.println(line);
                    reader.readLine();
                    map.put(reader.readLine(),reader.readLine());
                    System.out.println(map);
                    writer.write(("+OK\r\n".getBytes()));
                    continue;
                }
                if(line.trim().equalsIgnoreCase("GET")){
                    System.out.println(line);
                    reader.readLine();
                    String response = reader.readLine();
                    String value = map.get(response);
                    System.out.println(value);
                    String real = "$" + value.length() + "\r\n" + value + "\r\n";
                    writer.write(real.getBytes());
                    //continue;
                }
            }
                // Optionally, handle other Redis-like commands here
            }catch (IOException e) {
            System.out.println("IOException in client handler: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Failed to close client socket: " + e.getMessage());
            }
        }
    }
}