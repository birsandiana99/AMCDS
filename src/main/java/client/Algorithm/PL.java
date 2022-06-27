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
                                    .setValue(message.getNetworkMessage().getMessage().getAppBroadcast().getValue())
                                    .build())
                            .build();
                    plDeliverParams(msg, message.getNetworkMessage().getMessage().getFromAbstractionId(),
                            message.getNetworkMessage().getMessage().getToAbstractionId(), process.pid);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_VALUE)) {
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.APP_VALUE)
                            .setFromAbstractionId("app.beb.pl")
                            .setToAbstractionId("app.beb")
                            .setAppValue(message.getNetworkMessage().getMessage().getAppValue())
                            .build();
                    plDeliverParams(msg,"app.beb.pl","app.beb", process.pid);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_WRITE)) {
                    String register = message.getNetworkMessage().getMessage().getAppWrite().getRegister();
                    registerAbstraction(register);
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.NNAR_WRITE)
                            .setFromAbstractionId("app")
                            .setToAbstractionId(process.sharedMemory.appNnar)
                            .setSystemId("sys-1")
                            .setNnarWrite(NnarWrite.newBuilder()
                                    .setValue(message.getNetworkMessage().getMessage().getAppWrite().getValue())
                                    .build())
                            .build();
                    process.messages.add(msg);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)){
                    String toAbstractionId = message.getNetworkMessage().getMessage().getToAbstractionId();
                    plDeliverParams(message.getNetworkMessage().getMessage(),toAbstractionId+".beb.pl",toAbstractionId+".beb", process.pid);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)){
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = process.getProcByPort(portSender);

                    plDeliverParams(message.getNetworkMessage().getMessage(),process.sharedMemory.appNnar+".beb.pl",
                            process.sharedMemory.appNnar, senderPID);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)){
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = process.getProcByPort(portSender);
                    bebDeliverParams(message.getNetworkMessage().getMessage(),process.sharedMemory.appNnar+".beb.pl",
                            process.sharedMemory.appNnar+".beb", senderPID);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)){
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = process.getProcByPort(portSender);
                    plDeliverParams(message.getNetworkMessage().getMessage(),process.sharedMemory.appNnar+".beb.pl",
                            process.sharedMemory.appNnar, senderPID);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_READ)) {
                    String register = message.getNetworkMessage().getMessage().getAppRead().getRegister();
                    registerAbstraction(register);
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.NNAR_READ)
                            .setFromAbstractionId("app")
                            .setToAbstractionId(process.sharedMemory.appNnar)
                            .setSystemId("sys-1")
                            .setNnarRead(NnarRead.newBuilder()
                                    .build())
                            .build();
                    process.messages.add(msg);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EC_INTERNAL_NEW_EPOCH)){
                    System.out.println("Internal epoch yey");
                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = process.getProcByPort(portSender);
                    bebDeliverParams(message.getNetworkMessage().getMessage(),"app.beb.pl",
                            "ec", senderPID);
                    return true;
                }
                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EP_INTERNAL_READ)){

                    int portSender = message.getNetworkMessage().getSenderListeningPort();
                    ProcessId senderPID = process.getProcByPort(portSender);
                    bebDeliverParams(message.getNetworkMessage().getMessage(),message.getToAbstractionId(),
                            message.getToAbstractionId()+".ec", senderPID);
                    return true;
                }

            case PL_SEND:
                if (message.getToAbstractionId().equals("app.pl")) {
                    Message m = Message.newBuilder()
                            .setType(Message.Type.APP_VALUE)
                            .setMessageUuid(message.getPlSend().getMessage().getMessageUuid())
                            .setAppValue(message.getPlSend().getMessage().getAppValue())
                            .setFromAbstractionId("app.pl")
                            .setToAbstractionId("hub")
                            .setSystemId("sys-1")
                            .build();
                    sendToHub(m, "app.pl", "hub", process.port);
                    return true;
                }
                if (message.getToAbstractionId().equals("app.beb.pl")) {
                    Message appVal = Message.newBuilder()
                            .setType(Message.Type.APP_VALUE)
                            .setMessageUuid(message.getPlSend().getMessage().getMessageUuid())
                            .setAppValue(message.getPlSend().getMessage().getAppValue())
                            .setFromAbstractionId("app.beb.pl")
                            .setToAbstractionId("app")
                            .setSystemId("sys-1")
                            .build();
                    sendToProc(appVal, "app.beb.pl", "app.beb.pl",
                            message.getPlSend().getDestination().getHost(),
                            message.getPlSend().getDestination().getPort(), message.getPlSend().getDestination().getPort());
                    return true;
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)){
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.NNAR_INTERNAL_READ)
                            .setNnarInternalRead(message.getPlSend().getMessage().getNnarInternalRead())
                            .setFromAbstractionId(process.sharedMemory.appNnar)
                            .setToAbstractionId("app.nnar["+process.sharedMemory.register+"]")
                            .setSystemId("sys-1")
                            .build();

                    sendToProc(msg,process.sharedMemory.appNnar+".beb.pl", process.sharedMemory.appNnar+".beb.pl",
                            message.getPlSend().getDestination().getHost(), message.getPlSend().getDestination().getPort(), process.port);
                    return true;
                }
                if(message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)){
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();
                    sendToProc(message.getPlSend().getMessage(),message.getToAbstractionId(),message.getToAbstractionId(),
                            senderHost,senderPort, process.port);
                    return true;
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)){
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();
                    sendToProc(message.getPlSend().getMessage(),process.sharedMemory.appNnar+".beb.pl",
                            process.sharedMemory.appNnar+".beb.pl", senderHost, senderPort, process.port);
                    return true;
                }
                if (message.getPlSend().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)){
                    String senderHost = message.getPlSend().getDestination().getHost();
                    int senderPort = message.getPlSend().getDestination().getPort();
                    sendToProc(message.getPlSend().getMessage(),process.sharedMemory.appNnar+".beb.pl",
                            process.sharedMemory.appNnar+".beb.pl", senderHost, senderPort, process.port);
                    return true;
                }
            case PL_DELIVER:
                if(message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_WRITE_RETURN)){
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.APP_WRITE_RETURN)
                            .setAppWriteReturn(message.getPlDeliver().getMessage().getAppWriteReturn())
                            .setFromAbstractionId("app")
                            .setToAbstractionId("hub")
                            .setSystemId("sys-1")
                            .build();
                    sendToHub(msg, "app.pl", "app.pl", process.port);
                    return true;
                }

                if(message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_READ_RETURN)){
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.APP_READ_RETURN)
                            .setAppReadReturn(message.getPlDeliver().getMessage().getAppReadReturn())
                            .setFromAbstractionId("app")
                            .setToAbstractionId("hub")
                            .setSystemId("sys-1")
                            .build();
                    sendToHub(msg, "app.pl", "app.pl", process.port);
                    return true;
                }
        }
        return false;
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
        process.messages.add(Message.newBuilder()
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
        process.messages.add(Message.newBuilder()
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
        if (process.abstractionInterfaceMap.get("app.nnar[" + register + "]") == null) {
            process.abstractionInterfaceMap.put("app.nnar[" + register + "]", new NNAR());
            process.abstractionInterfaceMap.get("app.nnar[" + register + "]").init(process);

            process.abstractionInterfaceMap.put("app.nnar[" + register + "].pl", new PL());
            process.abstractionInterfaceMap.get("app.nnar[" + register + "].pl").init(process);

            process.abstractionInterfaceMap.put("app.nnar[" + register + "].beb", new BEB());
            process.abstractionInterfaceMap.get("app.nnar[" + register + "].beb").init(process);

            process.abstractionInterfaceMap.put("app.nnar[" + register + "].beb.pl", new PL());
            process.abstractionInterfaceMap.get("app.nnar[" + register + "].beb.pl").init(process);

            process.sharedMemory.register = register;
            process.sharedMemory.appNnar = "app.nnar[" + register + "]";

            System.out.println("Registered NNAR on " + process.debugName);
        }
    }
}
