import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

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
                OutputStream outputStream = clientSocket.getOutputStream()
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Trim and check if the command is PING
                if (line.trim().equalsIgnoreCase("PING") || line.trim().endsWith("PING")) {
                    outputStream.write("+PONG\r\n".getBytes());
                }
                int index = line.indexOf("ECHO");
                if(index!=-1){
                    int endIndex = line.indexOf("ECHO")+8;
                    String result = line.substring(endIndex).trim();
                    System.out.println(result);
                    outputStream.write(result.getBytes());
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