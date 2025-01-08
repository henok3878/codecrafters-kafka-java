import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;

public class Main {
  static short apiKey18MinVersion = 0;
  static short apiKey18MaxVersion = 4;

  static short apiKey75 = 75;
  static short apiKey75MinVersion = 0;
  static short apiKey75MaxVersion = 0;

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.err.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    List<Socket> clientSockets = new java.util.ArrayList<Socket>();
    int port = 9092;

    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      while (true) {
        // Wait for connection from client.
        final Socket client = serverSocket.accept();
        clientSockets.add(client);
        Thread thread = new Thread(() -> {
          handleClient(client);
        });
        thread.start();
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSockets.size() > 0) {
          for (Socket clientSocket : clientSockets) {
            if (clientSocket != null) {
              clientSocket.close();
            }
          }
        }

      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  private static void handleClient(Socket clientSocket) {
    InputStream in = null;
    OutputStream out = null;
    try {
      in = clientSocket.getInputStream();
      out = clientSocket.getOutputStream();
      while (true) {
        byte[] messageSizeBytes = new byte[4];
        in.read(messageSizeBytes);
        int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
        if (messageSize == -1) {
          System.out.println("Connection closed by client");
          break;
        }
        System.out.println("Message size: " + messageSize);
        byte[] requestBytes = new byte[messageSize];
        in.read(requestBytes);
        ByteBuffer buffer = ByteBuffer.wrap(requestBytes);
        short apiKey = readInt16(buffer);
        short apiVersion = readInt16(buffer);
        int correlationId = readInt32(buffer);
        System.out.println("API Key: " + apiKey);
        System.out.println("API Version: " + apiVersion);
        System.out.println("Correlation ID: " + correlationId);
        ByteBuffer outputBuffer = null;

        switch (apiKey) {
          case 18:
            outputBuffer = handleAPIVersionsRequest(apiVersion, correlationId, buffer);
            break;
          case 75:
            outputBuffer = handleDescribeTopicPartitionsRequest(apiVersion, correlationId, buffer);
            break;
          default:
            break;
        }
        if (outputBuffer != null) {
          out.write(outputBuffer.array());
          out.flush();
        }
      }

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

  private static ByteBuffer handleAPIVersionsRequest(int apiVersion, int correlationId, ByteBuffer buffer) {

    int messageSize = 4 + 2 + 1 + 2 + 2 + 2 + 1 + 4 + 1;
    messageSize += (2 + 2 + 2 + 1); // for APIKey 75 entry in the response

    ByteBuffer outputBuffer = ByteBuffer.allocate(4 + messageSize);
    outputBuffer.putInt(messageSize);
    outputBuffer.putInt(correlationId);

    if (apiVersion < apiKey18MinVersion || apiVersion > apiKey18MaxVersion) {
      outputBuffer.putShort((short) 35); // Error code for unsupported version
    } else {
      outputBuffer.putShort((short) 0); // Error code for no error
    }
    outputBuffer.put((byte) 3); // number of api keys supported, since api keys is compact array we use N + 1 to
                                // represent N elements
    outputBuffer.putShort((short) 18);
    outputBuffer.putShort(apiKey18MinVersion); // min Api version
    outputBuffer.putShort(apiKey18MaxVersion); // max Api version
    outputBuffer.put((byte) 0); // api key level tag buffer representing null array in compact array format
    outputBuffer.putShort(apiKey75);
    outputBuffer.putShort(apiKey75MinVersion);
    outputBuffer.putShort(apiKey75MaxVersion);
    outputBuffer.put((byte) 0); // api key level tag buffer representing null array in compact array format
    outputBuffer.putInt(0); // throttle time
    outputBuffer.put((byte) 0); // tag buffer of size 0 represents null array in compact array format

    return outputBuffer;
  }

  private static ByteBuffer handleDescribeTopicPartitionsRequest(int apiVersion, int correlationId,
      ByteBuffer buffer) {
    // Todo: Double check how the client id is encoded in the request
    int clientIdLength = readInt16(buffer); // clientId Lenght
    System.out.println("Client ID Length: " + clientIdLength);
    ByteBuffer clientId = ByteBuffer.allocate(clientIdLength);
    buffer.get(clientId.array(), 0, clientIdLength);
    String clientIdString = new String(clientId.array());
    System.out.println("Client ID: " + clientIdString);
    int tagBuffer = readVarInt(buffer);
    System.out.println("Tag Buffer: " + tagBuffer);
    // Todo: Implement the logic to handle cases where TagBuffer is non zero
    int topicsLength = readVarInt(buffer) - 1; // topics length using compact array enocding
    System.out.println("Topics Length: " + topicsLength);
    String topicName = ""; // Todo: handle multiple topcis later on 
    for (int numOfTopic = 0; numOfTopic < topicsLength; numOfTopic++) {
      int topicNameLength = readVarInt(buffer);
      topicNameLength -= 1;
      if (topicNameLength < 0) {
        continue;
      }
      ByteBuffer topicNameByteBuffer = ByteBuffer.allocate(topicNameLength);
      buffer.get(topicNameByteBuffer.array(), 0, topicNameLength);
      String topicNameString = new String(topicNameByteBuffer.array());
      topicName = topicNameString;
      System.out.println("Topic Name: " + topicNameString);
      int topicTagBuffer = readVarInt(buffer);
      System.out.println("Topic Tag Buffer: " + topicTagBuffer);
    }
    int responsePartitionsLimit = readInt32(buffer);
    System.out.println("Response Partitions Limit: " + responsePartitionsLimit);
    int cursor = readNullableVarInt(buffer);
    System.out.println("Cursor: " + cursor);
    int requestLevelTagBuffer = readVarInt(buffer) - 1;

    System.out.println("Request Level Tag Buffer: " + requestLevelTagBuffer);
    int size = calculateSizeOfDescribeTopicPartitionsResponseV0(topicName); 
    ByteBuffer outputBuffer = ByteBuffer.allocate(size);
    System.out.println("Response size: " + size);
    createDescribeTopicPartitionsResponseV0(outputBuffer,size - 4,  correlationId, topicName);
    return outputBuffer;
  }

  public static int createCompactStringByteBuffer(ByteBuffer buffer, String value) {
    // returns used bytse in the buffer
    if (value == null) {
      buffer.put((byte) 0);
      return 1;
    }

    byte[] valueBytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8); // Ensure UTF-8 encoding
    int length = valueBytes.length; // Compact string length = string length + 1
    int varIntSize = putVarInt(buffer, length + 1); // Write length as VarInt
    buffer.put(valueBytes); // Write string bytes
    return length + varIntSize; // Total bytes written (VarInt + string bytes)
  }

  private static int putVarInt(ByteBuffer buffer, int vlaue){
    int size = 0;
    while (true) {
      byte b = (byte) (vlaue & 0x7F);
      vlaue >>>= 7;
      if (vlaue != 0) {
        b |= 0x80;
      }
      buffer.put(b);
      size++;
      if (vlaue == 0) {
        break;
      }
    }
    return size;
  }
  private static Integer readNullableVarInt(ByteBuffer buffer) {
    // Peek at the first byte without consuming it
    byte firstByte = buffer.get(buffer.position());

    // Check if it's the null marker (0xFF)
    if ((firstByte & 0xFF) == 0xFF) {
        buffer.get(); // Consume the null marker
        return 0;  // Return null
    }

    // Decode as regular VarInt
    return readVarInt(buffer);
}

  private static int readVarInt(ByteBuffer buffer) {
    int value = 0;
    while (true) {
      byte b = buffer.get();
      value = (value << 7) | (b & 0x7F);
      if ((b & 0x80) == 0) {
        break;
      }
    }
    return value;

  }

  private static short readInt16(ByteBuffer buffer) {
    return buffer.getShort();
  }

  private static int readInt32(ByteBuffer buffer) {
    return buffer.getInt();
  }

  private static int addMessageSize(ByteBuffer buffer, int messageSize) {
    buffer.putInt(messageSize);
    return 4;
  }

  private static int addResponseHeaderV1(ByteBuffer buffer, int correlationId) {
    buffer.putInt(correlationId);
    // TAG_BUFFER
    buffer.put((byte) 0);
    return 5;
  }


  private static int calculateVarIntSize(int value) {
    int size = 0;
    while (true) {
      value >>>= 7;
      size++;
      if (value == 0) {
        break;
      }
    }
    return size;
  }


  private static int calculateCompactStringSize(String value) {
    if (value == null) {
      return 1;
    }
    int stringSize = value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length ;
    return stringSize + calculateVarIntSize(stringSize + 1);
  }

  private static int calculateSizeOfDescribeTopicPartitionsResponseV0(String topicName) {
    int size = 0;
    size += 4; // message size
    // response header
    size += 4; // correlation id
    size += 1; // TAG_Buffer 

    // response body 
    size += 4; // throttle time 
    size += 1; // topics array 
    size += 2; // topic error code 
    int topicNameSize = calculateCompactStringSize(topicName); // topic name                          
    size += topicNameSize; 
    // System.out.println("Topic Name Size: " + topicNameSize);
    size += 16; // topic id: UUID 
    size += 1; // is_internal
    size += 4; // topic authorized operations
    size += 1; // tag buffer for the topic
    size += 1; // partitions array
    size += 1; // cursor
    size += 1; // tag buffer 
    return size;
  }

  private static void createDescribeTopicPartitionsResponseV0(ByteBuffer buffer,int messageSize, int correlationId, String topicName) {
  
    addMessageSize(buffer, messageSize); 
    addResponseHeaderV1(buffer, correlationId);
    addDescribeTopicPartitionsResponseV0Body(buffer, topicName); 
    
  }

  private static int addDescribeTopicPartitionsResponseV0Body(ByteBuffer buffer,
      String topicNmae) {
    int size = 0;
    buffer.putInt(0); // throttle time
    size += 4;
    buffer.put((byte) 2); // topics array
    size += 1;
    buffer.putShort((short) 3); // error code:
    size += 2;
    int topicNameSizeAct = createCompactStringByteBuffer(buffer,topicNmae); // topic name                          
    size += topicNameSizeAct;
    // System.out.println("Topic Name Size Actual: " + topicNameSizeAct);
    buffer.putLong((long) 0);
    buffer.putLong((long) 0); // topic id
    size += 16;
    buffer.put((byte) 0); // is_internal
    size += 1;
    buffer.putInt(0);// topic authorized operations
    size += 4;
    buffer.put((byte)0); // tag buffer for the topic 
    size += 1; 
    buffer.put((byte) 0); // partitions array
    size += 1;
    buffer.put((byte) 0xFF); // Indicates the cursor is null 
    size += 1;
    buffer.put((byte) 0); // tag buffer
    size += 1; 
    return size;
  }
}
