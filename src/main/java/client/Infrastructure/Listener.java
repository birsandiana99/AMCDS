package client.Infrastructure;

import client.Algorithm.*;
import client.Utilities.EventsListener;
import client.Utilities.MessageUtils;
import client.Utilities.ProcUtil;
import main.CommunicationProtocol.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Listener extends Thread {
    private Thread t;
    private String threadName;
    private ServerSocket processSocket;
    private boolean isRunning;
    private Proc process;

    public Listener(String threadName, int port, Proc p) throws IOException {
        this.threadName = threadName;
        this.processSocket = new ServerSocket(port);
        this.isRunning = true;
        this.process = p;
    }

    public void run() {
        Socket socket = null;
        try {
            EventsListener messageListener = new EventsListener(this.process);

            messageListener.start();

            while (this.isRunning) {
                socket = this.processSocket.accept();
                Message message = MessageUtils.read(socket.getInputStream());

                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.PROC_INITIALIZE_SYSTEM)) {
                    System.out.println(this.process.debugName + " received PROC INITIALIZE SYSTEM \n");

                    this.process.processes = message.getNetworkMessage().getMessage().getProcInitializeSystem().getProcessesList();
                    this.process.rank = this.process.processes.get(this.process.index).getRank();

                    List<ProcessId> processesList = message.getNetworkMessage().getMessage().getProcInitializeSystem().getProcessesList();
                    this.process.setProcesses(processesList);

                    this.process.abstractionInterfaceMap.put("app", new APP());
                    this.process.abstractionInterfaceMap.get("app").init(this.process);

                    this.process.abstractionInterfaceMap.put("app.pl", new PL());
                    this.process.abstractionInterfaceMap.get("app.pl").init(this.process);

                    this.process.abstractionInterfaceMap.put("app.beb", new BEB());
                    this.process.abstractionInterfaceMap.get("app.beb").init(this.process);

                    this.process.abstractionInterfaceMap.put("app.beb.pl", new PL());
                    this.process.abstractionInterfaceMap.get("app.beb.pl").init(this.process);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.PROC_DESTROY_SYSTEM)) {
                    System.out.println(this.process.debugName + " received PROC DESTROY SYSTEM \n");
                    this.process.abstractionInterfaceMap.clear();

                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_BROADCAST)) {
                    System.out.println(this.process.debugName + " received APP BROADCAST \n");
                    this.process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_VALUE)) {
                    this.process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_WRITE)) {
                    System.out.println(this.process.debugName + " received APP WRITE \n");
                    this.process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    System.out.println(this.process.debugName + " received NNAR INTERNAL READ \n");
                    String toAbstractionId = message.getNetworkMessage().getMessage().getToAbstractionId();
                    String register = String.valueOf(toAbstractionId.charAt(9));
                    registerAbstraction(register);
                    this.process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)) {
                    System.out.println(this.process.debugName + " received NNAR INTERNAL VALUE \n");
                    this.process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    System.out.println(this.process.debugName + " received NNAR INTERNAL WRITE \n");
                    this.process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)) {
                    System.out.println(this.process.debugName + " received NNAR INTERNAL ACK \n");
                    this.process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_READ)) {
                    System.out.println(this.process.debugName + " received APP READ \n");
                    this.process.messages.add(message);
                }

                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_PROPOSE)) {

                    this.process.value = message.getNetworkMessage().getMessage().getAppPropose().getValue().getV();
                    String topic = message.getNetworkMessage().getMessage().getAppPropose().getTopic();
                    this.process.ucTopic = "app.uc[" + topic + "]";

                    this.process.abstractionInterfaceMap.put(this.process.ucTopic, new UC());
                    this.process.abstractionInterfaceMap.get(this.process.ucTopic).init(this.process);

                    this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ec", new EC());
                    this.process.abstractionInterfaceMap.get(this.process.ucTopic + ".ec").init(this.process);

                    this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ec.eld", new ELD());
                    this.process.abstractionInterfaceMap.get(this.process.ucTopic + ".ec.eld").init(this.process);

                    this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ec.eld.epfd", new EPFD());
                    this.process.abstractionInterfaceMap.get(this.process.ucTopic + ".ec.eld.epfd").init(this.process);

                    this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ec.eld.epfd.pl", new PL());
                    this.process.abstractionInterfaceMap.get(this.process.ucTopic + ".ec.eld.epfd.pl").init(this.process);

                    this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ec.pl", new PL());
                    this.process.abstractionInterfaceMap.get(this.process.ucTopic + ".ec.pl").init(this.process);

                    this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ec.beb", new BEB());
                    this.process.abstractionInterfaceMap.get(this.process.ucTopic + ".ec.beb").init(this.process);

                    this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ec.beb.pl", new PL());
                    this.process.abstractionInterfaceMap.get(this.process.ucTopic + ".ec.beb.pl").init(this.process);

                    this.process.messages.add(Message.newBuilder()
                            .setToAbstractionId(this.process.ucTopic)
                            .setSystemId("sys-1")
                            .setType(Message.Type.UC_PROPOSE)
                            .setUcPropose(UcPropose.newBuilder()
                                    .setValue(Value.newBuilder()
                                            .setDefined(true)
                                            .setV(this.process.sharedMemory.value)
                                            .build())
                                    .build())
                            .build());
                }

                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EC_INTERNAL_NEW_EPOCH)) {

                    if (this.process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null) {
                        if (message.getToAbstractionId().contains("ec")) {
                            this.process.abstractionInterfaceMap.put(message.getToAbstractionId(), new EC());
                        }
                    }
                    System.out.println("NETWORK: New epoch\n");
                    Message msgBeb = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setToAbstractionId(message.getNetworkMessage().getMessage().getToAbstractionId())
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(this.process.getProcByPort(message.getNetworkMessage().getSenderListeningPort()))
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    this.process.messages.add(msgBeb);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EP_INTERNAL_READ)) {
                    if (this.process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null) {
                        if (message.getToAbstractionId().contains("ep")) {
                            EpInternalState state = message.getNetworkMessage().getMessage().getEpInternalState();
                            this.process.abstractionInterfaceMap.put(message.getNetworkMessage().getMessage().getToAbstractionId(),
                                    new EP(this.process, state, this.process.epState.ets, this.process.leader));
                        }
                    }
                    System.out.println("NETWORK: internal read\n");
                    Message msgBeb = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setToAbstractionId(message.getNetworkMessage().getMessage().getToAbstractionId())
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(this.process.getProcByPort(message.getNetworkMessage().getSenderListeningPort()))
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    this.process.messages.add(msgBeb);
                }

                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EP_INTERNAL_WRITE)) {
                    if (this.process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null) {
                        if (message.getToAbstractionId().contains("ep")) {
                            EpInternalState state = message.getNetworkMessage().getMessage().getEpInternalState();
                            this.process.abstractionInterfaceMap.put(message.getNetworkMessage().getMessage().getToAbstractionId(),
                                    new EP(this.process, state, this.process.epState.ets, this.process.leader));
                        }
                    }
                    System.out.println("NETWORK: internal write\n");
                    Message msgBeb = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setToAbstractionId(message.getNetworkMessage().getMessage().getToAbstractionId())
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(this.process.getProcByPort(message.getNetworkMessage().getSenderListeningPort()))
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    this.process.messages.add(msgBeb);
                }

                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EP_INTERNAL_DECIDED)) {
                    if (this.process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null) {
                        if (message.getToAbstractionId().contains("ep")) {
                            EpInternalState state = message.getNetworkMessage().getMessage().getEpInternalState();
                            this.process.abstractionInterfaceMap.put(message.getNetworkMessage().getMessage().getToAbstractionId(),
                                    new EP(this.process, state, this.process.epState.ets, this.process.leader));
                        }
                    }
                    System.out.println("NETWORK: internal decided\n");
                    Message msgBeb = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setToAbstractionId(message.getNetworkMessage().getMessage().getToAbstractionId())
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(this.process.getProcByPort(message.getNetworkMessage().getSenderListeningPort()))
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    this.process.messages.add(msgBeb);
                }
                else {
                    if (this.process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null) {
                        if (message.getToAbstractionId().contains("ep")) {
                            EpInternalState state = message.getNetworkMessage().getMessage().getEpInternalState();
                            this.process.abstractionInterfaceMap.put(message.getToAbstractionId(),
                                    new EP(this.process, state, this.process.epState.ets, this.process.leader));
                        }
                    }

                    this.process.messages.add(
                            Message.newBuilder().setType(Message.Type.PL_DELIVER)
                                    .setToAbstractionId(message.getToAbstractionId())
                                    .setPlDeliver(PlDeliver.newBuilder()
                                            .setSender(ProcUtil.getProcessByAddress(this.process.processes,
                                                    message.getNetworkMessage().getSenderHost(),
                                                    message.getNetworkMessage().getSenderListeningPort()))
                                            .setMessage(message.getNetworkMessage().getMessage())
                                            .build())
                                    .build()
                    );
                }
                socket.close();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public void start() {
        if (this.t == null) {
            this.t = new Thread(this, this.threadName);
            this.t.start();
     }
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
