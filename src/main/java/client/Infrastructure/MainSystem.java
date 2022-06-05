package client.Infrastructure;

import client.Utilities.MessageUtils;
import main.CommunicationProtocol.*;

import java.io.IOException;

public class MainSystem {

    private static Proc startProc(int port, int index, int refPort) throws IOException {
        Proc process = new Proc(port, index, refPort);
        System.out.println("Starting Process no. " + index);

        MessageUtils.write(process.socketHub.getOutputStream(), Message.newBuilder()
                .setType(Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(
                        NetworkMessage.newBuilder()
                                .setSenderHost(process.addr)
                                .setSenderListeningPort(process.port)
                                .setMessage(Message.newBuilder()
                                        .setSystemId("sys-1")
                                        .setFromAbstractionId("app")
                                        .setType(Message.Type.PROC_REGISTRATION)
                                        .setProcRegistration(ProcRegistration.newBuilder()
                                                .setOwner(process.owner)
                                                .setIndex(process.index)
                                                .build())
                                        .build())
                                .build())
                .build());

        process.socketHub.close();
        return process;
    }

    public static void main(String[] args) throws IOException {

        Proc proc1 = startProc(5004, 1, 5001);
        Proc proc2 = startProc(5005, 2, 5002);
        Proc proc3 = startProc(5006, 3, 5003);

        Listener eventsListener1 = new Listener("Listener1", 5004, proc1);
        Listener eventsListener2 = new Listener("Listener2", 5005, proc2);
        Listener eventsListener3 = new Listener("Listener3", 5006, proc3);

        eventsListener1.start();
        eventsListener2.start();
        eventsListener3.start();
    }
}
