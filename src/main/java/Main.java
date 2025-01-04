import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.err.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    OutputStream out = null;
    InputStream in = null;
    int port = 9092;
    short minSupportedAPIVersion = 0;
    short maxSupportedAPIVersion = 4; 
    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      clientSocket = serverSocket.accept();
      in = clientSocket.getInputStream();
      byte[] messageSizeBytes = new byte[4];
      in.read(messageSizeBytes);
      int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
      byte[] requestBytes = new byte[messageSize];
      in.read(requestBytes);
      ByteBuffer buffer = ByteBuffer.wrap(requestBytes);
      short apiKey = readInt16(buffer);
      short apiVersion = readInt16(buffer);
      int correlationId = readInt32(buffer);
    

      int message_size = 4 + 2 + 1 + 2 + 2 + 2 + 1 + 4 + 1;

      ByteBuffer outputBuffer = ByteBuffer.allocate(4 + messageSize);
      outputBuffer.putInt(message_size);
      outputBuffer.putInt(correlationId);

      if(apiVersion < minSupportedAPIVersion || apiVersion > maxSupportedAPIVersion) {
        outputBuffer.putShort((short)35); // Error code for unsupported version
      }else{
        outputBuffer.putShort((short)0); // Error code for no error
      }
      outputBuffer.put((byte)2); // number of api keys supported, since api keys is compact array we use N + 1 to represent N elements
      outputBuffer.putShort((short)apiKey); 
      outputBuffer.putShort(minSupportedAPIVersion); // min Api version
      outputBuffer.putShort(maxSupportedAPIVersion); // max Api version
      outputBuffer.put((byte)0); // api key level tag buffer representing null array in compact array format
      outputBuffer.putInt(0); // throttle time 
      outputBuffer.put((byte)0); // tag buffer of size 0 represents null array in compact array format 
      out = clientSocket.getOutputStream();
      out.write(outputBuffer.array());
      out.flush();

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
        if (out != null) {
          out.close();
        }
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  private static short readInt16(ByteBuffer buffer) {
    return buffer.getShort();
  }

  private static int readInt32(ByteBuffer buffer) {
    return buffer.getInt();
  }
}
