package client.Algorithm;

import client.Infrastructure.Proc;
import client.Utilities.MessageUtils;
import main.CommunicationProtocol.*;

import java.io.IOException;
import java.net.Socket;


public class PL implements AbstractionInterface {
    private Proc process;

    @Override
    public void init(Proc p) {
        process = p;
    }

    @Override
    public boolean handle(Message message) {
        switch(message.getType()){
            case NETWORK_MESSAGE:
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_BROADCAST)) {
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.APP_BROADCAST)
                            .setFromAbstractionId("app.pl")
                            .setToAbstractionId("app")
                            .setSystemId("sys-1")
                            .setAppBroadcast(AppBroadcast.newBuilder()
                                    .setValue(message.getNetworkMessage().getMessage().getAppBroadcast().getValue()).build()).build();
                    plDeliver(msg);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_VALUE)) {
                    broadcastPlDeliver(message.getNetworkMessage().getMessage());
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_WRITE)) {
                    Message msg = message.getNetworkMessage().getMessage();
                    Message plDelMsg = Message.newBuilder()
                            .setType(Message.Type.PL_DELIVER)
                            .setFromAbstractionId("app.pl")
                            .setToAbstractionId("app")
                            .setPlDeliver(PlDeliver.newBuilder()
                                    .setMessage(msg)
                                    .build())
                            .build();
                    process.messages.add(plDelMsg);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_READ)) {
                    Message msg = message.getNetworkMessage().getMessage();
                    Message plDelMsg = Message.newBuilder()
                            .setType(Message.Type.PL_DELIVER)
                            .setFromAbstractionId("app.pl")
                            .setToAbstractionId("app")
                            .setPlDeliver(PlDeliver.newBuilder()
                                    .setMessage(msg)
                                    .build())
                            .build();
                    process.messages.add(plDelMsg);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    String toAbstractionId = message.getNetworkMessage().getMessage().getToAbstractionId();

                    Message msg = Message.newBuilder()
                            .setType(Message.Type.PL_DELIVER)
                            .setSystemId("sys-1")
                            .setFromAbstractionId(toAbstractionId + ".beb.pl")
                            .setToAbstractionId(toAbstractionId + ".beb")
                            .setPlDeliver(PlDeliver.newBuilder()
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    process.messages.add(msg);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)) {
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = process.getProcByPort(portSender);

                    Message msg = Message.newBuilder()
                            .setType(Message.Type.PL_DELIVER)
                            .setFromAbstractionId("app.nnar[" + process.register + "].beb.pl")
                            .setToAbstractionId("app.nnar[" + process.register + "].beb")
                            .setPlDeliver(PlDeliver.newBuilder()
                                    .setSender(senderPID)
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    process.messages.add(msg);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = process.getProcByPort(portSender);

                    Message msg = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setFromAbstractionId("app.nnar[" + process.register + "].beb.pl")
                            .setToAbstractionId("app.nnar[" + process.register + "].beb")
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(senderPID)
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    process.messages.add(msg);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)) {
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = process.getProcByPort(portSender);

                    Message msg = Message.newBuilder()
                            .setType(Message.Type.PL_DELIVER)
                            .setFromAbstractionId("app.nnar[" + process.register + "].beb.pl")
                            .setToAbstractionId("app.nnar[" + process.register + "].beb")
                            .setPlDeliver(PlDeliver.newBuilder()
                                    .setSender(senderPID)
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    process.messages.add(msg);
                    return true;
                }
            case PL_SEND:
                if (message.getToAbstractionId().equals("app.beb.pl")) {
                    sendSocket(
                            message.getPlSend().getDestination().getHost(),
                            message.getPlSend().getDestination().getPort(),
                            message.getPlSend().getMessage());
                    return true;
                }
                if (message.getToAbstractionId().equals("app.pl")) {
                    plSendToHub(message.getPlSend().getMessage());
                    return true;
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    Socket socket = null;
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();

                    try {
                        socket = new Socket(senderHost, senderPort);
                        Message newMessage = Message.newBuilder()
                                .setType(Message.Type.NETWORK_MESSAGE)
                                .setSystemId("sys-1")
                                .setFromAbstractionId("app.nnar[" + process.register + "].beb.pl")
                                .setToAbstractionId("app.nnar[" + process.register + "].beb.pl")
                                .setNetworkMessage(NetworkMessage.newBuilder()
                                        .setSenderHost(senderHost)
                                        .setSenderListeningPort(process.port)
                                        .setMessage(Message.newBuilder()
                                                .setType(Message.Type.NNAR_INTERNAL_READ)
                                                .setNnarInternalRead(message.getPlSend().getMessage().getNnarInternalRead())
                                                .setFromAbstractionId("app.nnar[" + process.register + "]")
                                                .setToAbstractionId("app.nnar[" + process.register + "]")
                                                .setSystemId("sys-1")
                                                .build())
                                        .build())
                                .build();
                        MessageUtils.write(socket.getOutputStream(), newMessage);
                        System.out.println("NNAR_INTERNAL_READ: " + newMessage);
                        socket.close();
                        return true;
                    } catch (IOException e) {
                        System.out.println("Socket error - could not send!");
                    }
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)) {
                    Socket socket = null;
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();

                    try {
                        socket = new Socket(senderHost, senderPort);
                        Message newMessage = Message.newBuilder()
                                .setType(Message.Type.NETWORK_MESSAGE)
                                .setSystemId("sys-1")
                                .setFromAbstractionId(message.getToAbstractionId())
                                .setToAbstractionId(message.getToAbstractionId())
                                .setNetworkMessage(NetworkMessage.newBuilder()
                                        .setSenderHost(senderHost)
                                        .setSenderListeningPort(process.port)
                                        .setMessage(message.getPlSend().getMessage())
                                        .build())
                                .build();
                        MessageUtils.write(socket.getOutputStream(), newMessage);
                        System.out.println("NNAR_INTERNAL_VALUE: " + newMessage);
                        socket.close();
                        return true;
                    } catch (IOException e) {
                        System.out.println("Socket error - could not send!");
                    }
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    Socket socket = null;
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();

                    try {
                        socket = new Socket(senderHost, senderPort);
                        Message newMessage = Message.newBuilder()
                                .setType(Message.Type.NETWORK_MESSAGE)
                                .setSystemId("sys-1")
                                .setFromAbstractionId("app.nnar[" + process.register + "].beb.pl")
                                .setToAbstractionId("app.nnar[" + process.register + "].beb.pl")
                                .setNetworkMessage(NetworkMessage.newBuilder()
                                        .setSenderHost(senderHost)
                                        .setSenderListeningPort(process.port)
                                        .setMessage(Message.newBuilder()
                                                .setType(Message.Type.NNAR_INTERNAL_WRITE)
                                                .setNnarInternalWrite(message.getPlSend().getMessage().getNnarInternalWrite())
                                                .setFromAbstractionId("app.nnar[" + process.register + "]")
                                                .setToAbstractionId("app.nnar[" + process.register + "]")
                                                .setSystemId("sys-1")
                                                .build())
                                        .build())
                                .build();
                        MessageUtils.write(socket.getOutputStream(), newMessage);
                        System.out.println("NNAR_INTERNAL_WRITE: " + newMessage);
                        socket.close();
                        return true;
                    } catch (IOException e) {
                        System.out.println("Socket error - could not send!");
                    }
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)) {
                    Socket socket = null;
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();

                    try {
                        socket = new Socket(senderHost, senderPort);
                        Message newMessage = Message.newBuilder()
                                .setType(Message.Type.NETWORK_MESSAGE)
                                .setSystemId("sys-1")
                                .setFromAbstractionId("app.nnar[" + process.register + "].beb.pl")
                                .setToAbstractionId("app.nnar[" + process.register + "].beb.pl")
                                .setNetworkMessage(NetworkMessage.newBuilder()
                                        .setSenderHost(senderHost)
                                        .setSenderListeningPort(process.port)
                                        .setMessage(Message.newBuilder()
                                                .setType(Message.Type.NNAR_INTERNAL_ACK)
                                                .setNnarInternalAck(message.getPlSend().getMessage().getNnarInternalAck())
                                                .setFromAbstractionId("app.nnar[" + process.register + "]")
                                                .setToAbstractionId("app.nnar[" + process.register + "]")
                                                .setSystemId("sys-1")
                                                .build())
                                        .build())
                                .build();
                        MessageUtils.write(socket.getOutputStream(), newMessage);
                        System.out.println("NNAR_INTERNAL_ACK: " + newMessage);
                        socket.close();
                        return true;
                    } catch (IOException e) {
                        System.out.println("Socket error - could not send!");
                    }
                }
            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_WRITE_RETURN)) {
                    Socket socket = null;

                    try {
                        socket = new Socket(Proc.ADDR_HUB, Proc.PORT_HUB);
                        Message newMessage = Message.newBuilder()
                                .setType(Message.Type.NETWORK_MESSAGE)
                                .setSystemId("sys-1")
                                .setFromAbstractionId("app.pl")
                                .setToAbstractionId("app.pl")
                                .setNetworkMessage(NetworkMessage.newBuilder()
                                        .setSenderHost(Proc.ADDR_HUB)
                                        .setSenderListeningPort(Proc.PORT_HUB)
                                        .setMessage(Message.newBuilder()
                                                .setType(Message.Type.APP_WRITE_RETURN)
                                                .setAppWriteReturn(message.getPlDeliver().getMessage().getAppWriteReturn())
                                                .setFromAbstractionId("app")
                                                .setToAbstractionId("hub")
                                                .setSystemId("sys-1")
                                                .build())
                                        .build())
                                .build();
                        MessageUtils.write(socket.getOutputStream(), newMessage);
                        System.out.println("APP_WRITE_RETURN: " + newMessage);
                        socket.close();
                        return true;
                    } catch (IOException e) {
                        System.out.println("Socket error - could not send!");
                    }
                }
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_READ_RETURN)) {
                    Socket socket = null;

                    try {
                        socket = new Socket(Proc.ADDR_HUB, Proc.PORT_HUB);
                        Message newMessage = Message.newBuilder()
                                .setType(Message.Type.NETWORK_MESSAGE)
                                .setSystemId("sys-1")
                                .setFromAbstractionId("app.pl")
                                .setToAbstractionId("app.pl")
                                .setNetworkMessage(NetworkMessage.newBuilder()
                                        .setSenderHost(Proc.ADDR_HUB)
                                        .setSenderListeningPort(Proc.PORT_HUB)
                                        .setMessage(Message.newBuilder()
                                                .setType(Message.Type.APP_READ_RETURN)
                                                .setAppReadReturn(message.getPlDeliver().getMessage().getAppReadReturn())
                                                .setFromAbstractionId("app")
                                                .setToAbstractionId("hub")
                                                .setSystemId("sys-1")
                                                .build())
                                        .build())
                                .build();
                        MessageUtils.write(socket.getOutputStream(), newMessage);
                        System.out.println("APP_READ_RETURN: " + newMessage);
                        socket.close();
                        return true;
                    } catch (IOException e) {
                        System.out.println("Socket error - could not send!");
                    }
                }
        }
        return false;
    }




    // set message type from APP_BROADCAST to PL_DELIVER
    private void plDeliver(Message message) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_DELIVER)
                .setPlDeliver(PlDeliver.newBuilder()
                        .setMessage(message)
                        .build())
                .setFromAbstractionId(message.getFromAbstractionId())
                .setToAbstractionId(message.getToAbstractionId())
                .setSystemId(message.getSystemId())
                .build());
    }

    private void sendSocket(String senderHost, int senderListeningPort, Message message) {
        Socket socket = null;
        try {
            socket = new Socket(senderHost, senderListeningPort);

            Message nm = Message.newBuilder()
                    .setType(Message.Type.NETWORK_MESSAGE)
                    .setToAbstractionId("app.beb.pl")
                    .setFromAbstractionId("app.beb.pl")
                    .setSystemId("sys-1")
                    .setNetworkMessage(NetworkMessage.newBuilder()
                            .setSenderHost(senderHost)
                            .setSenderListeningPort(senderListeningPort)
                            .setMessage(Message.newBuilder()
                                    .setType(Message.Type.APP_VALUE)
                                    .setMessageUuid(message.getMessageUuid())
                                    .setAppValue(message.getAppValue())
                                    .setFromAbstractionId("app.beb.pl")
                                    .setToAbstractionId("app")
                                    .setSystemId("sys-1")
                                    .build())
                    ).build();
            MessageUtils.write(socket.getOutputStream(), nm);
            socket.close();
        } catch (IOException e) {
            System.out.println("Socket error - cannot send");
        }
    }

    private void broadcastPlDeliver(Message message) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_DELIVER)
                .setPlDeliver(PlDeliver.newBuilder()
                        .setMessage(Message.newBuilder()
                                .setType(Message.Type.APP_VALUE)
                                .setFromAbstractionId("app.beb.pl")
                                .setToAbstractionId("app.beb")
                                .setAppValue(message.getAppValue()))
                        .build())
                .setFromAbstractionId("app.beb.pl")
                .setToAbstractionId("app.beb")
                .setSystemId("sys-1")
                .build());
    }

    private void plSendToHub(Message message) {
        Socket socket = null;
        try {
            socket = new Socket(Proc.ADDR_HUB, Proc.PORT_HUB);
            Message newMessage = Message.newBuilder()
                    .setType(Message.Type.NETWORK_MESSAGE)
                    .setMessageUuid(message.getMessageUuid())
                    .setFromAbstractionId("app.pl")
                    .setToAbstractionId("hub")
                    .setSystemId("sys-1")
                    .setNetworkMessage(NetworkMessage.newBuilder()
                            .setSenderHost(Proc.ADDR_HUB)
                            .setSenderListeningPort(process.port)
                            .setMessage(Message.newBuilder()
                                    .setType(Message.Type.APP_VALUE)
                                    .setMessageUuid(message.getMessageUuid())
                                    .setAppValue(message.getAppValue())
                                    .setFromAbstractionId("app.pl")
                                    .setToAbstractionId("hub")
                                    .setSystemId("sys-1")
                                    .build())
                    ).build();
            MessageUtils.write(socket.getOutputStream(), newMessage);
            socket.close();
        } catch (IOException e) {
            System.out.println("Socket error - cannot send");
        }
    }

}
