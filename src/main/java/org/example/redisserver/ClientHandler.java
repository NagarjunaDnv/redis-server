package org.example.redisserver;

import org.example.redisserver.data.DataProcessor;
import org.example.resp.resp3.RESP3Decoder;
import org.example.resp.resp3.RESP3Encoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final DataProcessor dataProcessor;

    public ClientHandler(Socket socket, DataProcessor dataProcessor) {
        this.clientSocket = socket;
        this.dataProcessor = dataProcessor;
    }

    @Override
    public void run() {


        try (InputStream in = clientSocket.getInputStream();
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
            ) {

            Object decodedValue = RESP3Decoder.decode(in);
            if(decodedValue.getClass().isArray()) {
               String response = dataProcessor.executeCommand((Object[]) decodedValue);
               out.write(response);
            }
            else {
                out.write(RESP3Encoder.encodeSimpleString("OK"));
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {

            try {
                if(clientSocket != null) {
                    clientSocket.close();
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
}
