package client.Infrastructure;

import client.Utilities.MessageUtils;
import main.CommunicationProtocol.*;

import java.io.IOException;

public class MainSystem {

    public static final String ADDR_HUB = "127.0.0.1";
    public static final int PORT_HUB = 5000;

    public static void main(String[] args) throws IOException {
        Proc proc1 = new Proc(5004, 1, 5001);
        System.out.println("Starting Proc no. 1");

        MessageUtils.write(proc1.socketHub.getOutputStream(), Message.newBuilder()
                .setType(Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(
                        NetworkMessage.newBuilder()
                                .setSenderHost(proc1.addr)
                                .setSenderListeningPort(proc1.port)
                                .setMessage(Message.newBuilder()
                                        .setSystemId("sys-1")
                                        .setFromAbstractionId("app")
                                        .setType(Message.Type.PROC_REGISTRATION)
                                        .setProcRegistration(ProcRegistration.newBuilder()
                                                .setOwner(proc1.owner)
                                                .setIndex(proc1.index)
                                                .build())
                                        .build())
                                .build())
                        .build());

        proc1.socketHub.close();

        Proc proc2 = new Proc(5005, 2, 5002);
        System.out.println("Starting Proc no. 2");

        MessageUtils.write(proc2.socketHub.getOutputStream(), Message.newBuilder()
                .setType(Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(
                        NetworkMessage.newBuilder()
                                .setSenderHost(proc2.addr)
                                .setSenderListeningPort(proc2.port)
                                .setMessage(Message.newBuilder()
                                        .setSystemId("sys-1")
                                        .setFromAbstractionId("app")
                                        .setType(Message.Type.PROC_REGISTRATION)
                                        .setProcRegistration(ProcRegistration.newBuilder()
                                                .setOwner(proc2.owner)
                                                .setIndex(proc2.index)
                                                .build())
                                        .build())
                                .build())
                .build());

        proc2.socketHub.close();

        Proc proc3 = new Proc(5006, 3, 5003);
        System.out.println("Starting Proc no. 3");


        MessageUtils.write(proc3.socketHub.getOutputStream(), Message.newBuilder()
                .setType(Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(
                        NetworkMessage.newBuilder()
                                .setSenderHost(proc3.addr)
                                .setSenderListeningPort(proc3.port)
                                .setMessage(Message.newBuilder()
                                        .setSystemId("sys-1")
                                        .setFromAbstractionId("app")
                                        .setType(Message.Type.PROC_REGISTRATION)
                                        .setProcRegistration(ProcRegistration.newBuilder()
                                                .setOwner(proc3.owner)
                                                .setIndex(proc3.index)
                                                .build())
                                        .build())
                                .build())
                .build());

        proc3.socketHub.close();


        Listener eventsListener1 = new Listener("Listener1", 5004, proc1);
        Listener eventsListener2 = new Listener("Listener2", 5005, proc2);
        Listener eventsListener3 = new Listener("Listener3", 5006, proc3);

        eventsListener1.start();
        eventsListener2.start();
        eventsListener3.start();
    }
}
