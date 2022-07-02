package client.Algorithm;

import client.Infrastructure.Proc;
import client.Utilities.MessageUtils;
import main.CommunicationProtocol.*;

import java.io.IOException;
import java.net.Socket;


public class PL implements AbstractionInterface {
    private Proc process;

    @Override
    public void init(Proc process) {
        this.process = process;
    }

    @Override
    public boolean handle(Message message) {
        switch(message.getType()){
            case NETWORK_MESSAGE:
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_BROADCAST)) {
                    Message appBroadcast = Message.newBuilder()
                            .setType(Message.Type.APP_BROADCAST)
                            .setFromAbstractionId("app.pl")
                            .setToAbstractionId("app")
                            .setSystemId("sys-1")
                            .setAppBroadcast(AppBroadcast.newBuilder()
                                    .setValue(message.getNetworkMessage().getMessage().getAppBroadcast().getValue())
                                    .build())
                            .build();
                    this.plDeliverParams(appBroadcast, message.getNetworkMessage().getMessage().getFromAbstractionId(),
                            message.getNetworkMessage().getMessage().getToAbstractionId(), this.process.pid);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_VALUE)) {
                    Message appValue = Message.newBuilder()
                            .setType(Message.Type.APP_VALUE)
                            .setFromAbstractionId("app.beb.pl")
                            .setToAbstractionId("app.beb")
                            .setAppValue(message.getNetworkMessage().getMessage().getAppValue())
                            .build();
                    this.plDeliverParams(appValue,"app.beb.pl","app.beb", this.process.pid);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_WRITE)) {
                    String register = message.getNetworkMessage().getMessage().getAppWrite().getRegister();
                    registerAbstraction(register);
                    Message nnarWrite = Message.newBuilder()
                            .setType(Message.Type.NNAR_WRITE)
                            .setFromAbstractionId("app")
                            .setToAbstractionId(this.process.sharedMemory.appNnar)
                            .setSystemId("sys-1")
                            .setNnarWrite(NnarWrite.newBuilder()
                                    .setValue(message.getNetworkMessage().getMessage().getAppWrite().getValue())
                                    .build())
                            .build();
                    this.process.messages.add(nnarWrite);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)){
                    String toAbstractionId = message.getNetworkMessage().getMessage().getToAbstractionId();
                    this.plDeliverParams(message.getNetworkMessage().getMessage(),toAbstractionId + ".beb.pl",toAbstractionId + ".beb", this.process.pid);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)){
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = this.process.getProcByPort(portSender);

                    this.plDeliverParams(message.getNetworkMessage().getMessage(),this.process.sharedMemory.appNnar + ".beb.pl",
                            this.process.sharedMemory.appNnar, senderPID);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)){
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = this.process.getProcByPort(portSender);
                    bebDeliverParams(message.getNetworkMessage().getMessage(),this.process.sharedMemory.appNnar + ".beb.pl",
                            this.process.sharedMemory.appNnar + ".beb", senderPID);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)){
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = this.process.getProcByPort(portSender);
                    plDeliverParams(message.getNetworkMessage().getMessage(),this.process.sharedMemory.appNnar + ".beb.pl",
                            this.process.sharedMemory.appNnar, senderPID);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_READ)) {
                    String register = message.getNetworkMessage().getMessage().getAppRead().getRegister();
                    registerAbstraction(register);
                    Message nnarRead = Message.newBuilder()
                            .setType(Message.Type.NNAR_READ)
                            .setFromAbstractionId("app")
                            .setToAbstractionId(this.process.sharedMemory.appNnar)
                            .setSystemId("sys-1")
                            .setNnarRead(NnarRead.newBuilder()
                                    .build())
                            .build();
                    this.process.messages.add(nnarRead);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EC_INTERNAL_NEW_EPOCH)){
                    System.out.println("Internal epoch yey");
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = this.process.getProcByPort(portSender);
                    this.bebDeliverParams(message.getNetworkMessage().getMessage(),"app.beb.pl",
                            "ec", senderPID);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EP_INTERNAL_READ)){

                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = this.process.getProcByPort(portSender);
                    this.bebDeliverParams(message.getNetworkMessage().getMessage(),message.getToAbstractionId(),
                            message.getToAbstractionId()+".ec", senderPID);
                    return true;
                }

            case PL_SEND:
                if (message.getToAbstractionId().equals("app.pl")) {
                    Message appValue = Message.newBuilder()
                            .setType(Message.Type.APP_VALUE)
                            .setMessageUuid(message.getPlSend().getMessage().getMessageUuid())
                            .setAppValue(message.getPlSend().getMessage().getAppValue())
                            .setFromAbstractionId("app.pl")
                            .setToAbstractionId("hub")
                            .setSystemId("sys-1")
                            .build();
                    this.sendToHub(appValue, "app.pl", "hub", process.port);
                    return true;
                }
                if (message.getToAbstractionId().equals("app.beb.pl")) {
                    Message appValue = Message.newBuilder()
                            .setType(Message.Type.APP_VALUE)
                            .setMessageUuid(message.getPlSend().getMessage().getMessageUuid())
                            .setAppValue(message.getPlSend().getMessage().getAppValue())
                            .setFromAbstractionId("app.beb.pl")
                            .setToAbstractionId("app")
                            .setSystemId("sys-1")
                            .build();
                    this.sendToProc(appValue, "app.beb.pl", "app.beb.pl",
                            message.getPlSend().getDestination().getHost(),
                            message.getPlSend().getDestination().getPort(), message.getPlSend().getDestination().getPort());
                    return true;
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)){
                    Message nnarInternalRead = Message.newBuilder()
                            .setType(Message.Type.NNAR_INTERNAL_READ)
                            .setNnarInternalRead(message.getPlSend().getMessage().getNnarInternalRead())
                            .setFromAbstractionId(this.process.sharedMemory.appNnar)
                            .setToAbstractionId("app.nnar[" + this.process.sharedMemory.register + "]")
                            .setSystemId("sys-1")
                            .build();

                    this.sendToProc(nnarInternalRead,this.process.sharedMemory.appNnar + ".beb.pl", this.process.sharedMemory.appNnar + ".beb.pl",
                            message.getPlSend().getDestination().getHost(), message.getPlSend().getDestination().getPort(), this.process.port);
                    return true;
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)) {
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();
                    this.sendToProc(message.getPlSend().getMessage(), message.getToAbstractionId(), message.getToAbstractionId(),
                            senderHost, senderPort, this.process.port);
                    return true;
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();
                    this.sendToProc(message.getPlSend().getMessage(),this.process.sharedMemory.appNnar + ".beb.pl",
                            this.process.sharedMemory.appNnar + ".beb.pl", senderHost, senderPort, this.process.port);
                    return true;
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)){
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();
                    this.sendToProc(message.getPlSend().getMessage(),this.process.sharedMemory.appNnar + ".beb.pl",
                            this.process.sharedMemory.appNnar + ".beb.pl", senderHost, senderPort, this.process.port);
                    return true;
                }
                else {
                    this.plSendParams(message.getToAbstractionId(), message.getPlSend().getDestination().getHost(),
                            message.getPlSend().getDestination().getPort(), message.getPlSend().getMessage());
                    return true;
                }
            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_WRITE_RETURN)) {
                    Message appWriteReturn = Message.newBuilder()
                            .setType(Message.Type.APP_WRITE_RETURN)
                            .setAppWriteReturn(message.getPlDeliver().getMessage().getAppWriteReturn())
                            .setFromAbstractionId("app")
                            .setToAbstractionId("hub")
                            .setSystemId("sys-1")
                            .build();
                    this.sendToHub(appWriteReturn, "app.pl", "app.pl", this.process.port);
                    return true;
                }

                if(message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_READ_RETURN)){
                    Message appReadReturn = Message.newBuilder()
                            .setType(Message.Type.APP_READ_RETURN)
                            .setAppReadReturn(message.getPlDeliver().getMessage().getAppReadReturn())
                            .setFromAbstractionId("app")
                            .setToAbstractionId("hub")
                            .setSystemId("sys-1")
                            .build();
                    this.sendToHub(appReadReturn, "app.pl", "app.pl", this.process.port);
                    return true;
                }
        }
        return false;
    }

    private void plSendParams(String toAbstractionId, String hostSender, int listeningPortSender, Message message) {
        Socket socket = null;
        try {
            socket = new Socket(hostSender, listeningPortSender);
            NetworkMessage networkMessage = NetworkMessage.newBuilder()
                    .setSenderHost(Proc.ADDR_HUB)
                    .setSenderListeningPort(this.process.port)
                    .setMessage(message).build();

            MessageUtils.write(socket.getOutputStream(), Message.newBuilder()
                    .setType(Message.Type.NETWORK_MESSAGE)
                    .setToAbstractionId(toAbstractionId)
                    .setSystemId("sys-1")
                    .setNetworkMessage(networkMessage)
                    .build());

            socket.close();
        } catch (IOException e) {
            System.out.println("Consensus socket send error " + e);
        }
    }

    private void sendToProc(Message message, String from, String to, String host, int senderPort, int listeningPort) {
        Socket socket;
        try {
            socket = new Socket(host, senderPort);
            Message msg = Message.newBuilder()
                    .setType(Message.Type.NETWORK_MESSAGE)
                    .setFromAbstractionId(from)
                    .setToAbstractionId(to)
                    .setSystemId("sys-1")
                    .setNetworkMessage(NetworkMessage.newBuilder()
                            .setSenderHost(host)
                            .setSenderListeningPort(listeningPort)
                            .setMessage(message)
                    ).build();
            MessageUtils.write(socket.getOutputStream(), msg);
            socket.close();
        } catch (IOException e) {
            System.out.println("Socket error - cannot send");
        }
    }

    //dynamic call with 'to' and 'from' abstractions for plDeliver
    private void plDeliverParams(Message message, String from, String to, ProcessId pid) {
        this.process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_DELIVER)
                .setPlDeliver(PlDeliver.newBuilder()
                        .setSender(pid)
                        .setMessage(message).build())
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .setSystemId("sys-1")
                .build());
    }

    private void sendToHub(Message message, String from, String to, int port) {
        Socket socket = null;
        try {
            socket = new Socket(Proc.ADDR_HUB, Proc.PORT_HUB);
            Message msg = Message.newBuilder()
                    .setType(Message.Type.NETWORK_MESSAGE)
                    .setMessageUuid(message.getMessageUuid())
                    .setFromAbstractionId(from)
                    .setToAbstractionId(to)
                    .setSystemId("sys-1")
                    .setNetworkMessage(NetworkMessage.newBuilder()
                            .setSenderHost(Proc.ADDR_HUB)
                            .setSenderListeningPort(port)
                            .setMessage(message)
                    ).build();
            MessageUtils.write(socket.getOutputStream(), msg);
            socket.close();
        } catch (IOException e) {
            System.out.println("Socket error - cannot send");
        }
    }

    //dynamic call with 'to' and 'from' abstractions for bebDeliver
    public void bebDeliverParams(Message message, String from, String to, ProcessId pid) {
        this.process.messages.add(Message.newBuilder()
                .setType(Message.Type.BEB_DELIVER)
                .setBebDeliver(BebDeliver.newBuilder()
                        .setMessage(message)
                        .setSender(pid)
                        .build())
                .setSystemId("sys-1")
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .build());
    }

    private void registerAbstraction(String register) {
        if (this.process.abstractionInterfaceMap.get("app.nnar[" + register + "]") == null) {
            this.process.abstractionInterfaceMap.put("app.nnar[" + register + "]", new NNAR());
            this.process.abstractionInterfaceMap.get("app.nnar[" + register + "]").init(this.process);

            this.process.abstractionInterfaceMap.put("app.nnar[" + register + "].pl", new PL());
            this.process.abstractionInterfaceMap.get("app.nnar[" + register + "].pl").init(this.process);

            this.process.abstractionInterfaceMap.put("app.nnar[" + register + "].beb", new BEB());
            this.process.abstractionInterfaceMap.get("app.nnar[" + register + "].beb").init(this.process);

            this.process.abstractionInterfaceMap.put("app.nnar[" + register + "].beb.pl", new PL());
            this.process.abstractionInterfaceMap.get("app.nnar[" + register + "].beb.pl").init(this.process);

            this.process.sharedMemory.register = register;
            this.process.sharedMemory.appNnar = "app.nnar[" + register + "]";

            System.out.println("Registered NNAR on " + this.process.debugName);
        }
    }
}
