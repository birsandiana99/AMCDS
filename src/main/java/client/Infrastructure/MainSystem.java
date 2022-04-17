package client.Infrastructure;

import java.io.IOException;

public class MainSystem {

    public static final String ADDR_HUB = "127.0.0.1";
    public static final int PORT_HUB = 5000;

    public static void main(String[] args) throws IOException {
        Proc proc1 = new Proc(5004, 1, 5001);
        System.out.println("Starting Proc no. 1");

        proc1.socketHub.close();

        Proc proc2 = new Proc(5005, 2, 5002);
        System.out.println("Starting Proc no. 2");

        proc2.socketHub.close();

        Proc proc3 = new Proc(5006, 3, 5003);
        System.out.println("Starting Proc no. 3");

        proc3.socketHub.close();
    }
}
