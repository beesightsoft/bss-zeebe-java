package beesightsoft.com;

import io.zeebe.client.ZeebeClient;

public class App {
    public static void main(String[] args) {
        final ZeebeClient client = ZeebeClient.newClientBuilder()
                // change the contact point if needed
                .brokerContactPoint("192.168.1.235:26500")
                .usePlaintext()
                .build();

        System.out.println("Connected.");

        // ...

        client.close();
        System.out.println("Closed.");
    }
}