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
    private boolean running;
    private Proc process;

    public Listener(String threadName, int port, Proc p) throws IOException {
        this.threadName = threadName;
        processSocket = new ServerSocket(port);
        running = true;
        process = p;
    }

    public void run() {
        Socket socket = null;
        try {
            EventsListener messageListener = new EventsListener(process);

            messageListener.start();

            while(running) {
                socket = processSocket.accept();
                Message message = MessageUtils.read(socket.getInputStream());

                if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.PROC_INITIALIZE_SYSTEM)) {
                    System.out.println(process.debugName + " received PROC INITIALIZE SYSTEM \n");

                    process.processes = message.getNetworkMessage().getMessage().getProcInitializeSystem().getProcessesList();
                    process.rank = process.processes.get(process.index).getRank();

                    List<ProcessId> processesList = message.getNetworkMessage().getMessage().getProcInitializeSystem().getProcessesList();
                    process.setProcesses(processesList);

                    process.abstractionInterfaceMap.put("app", new APP());
                    process.abstractionInterfaceMap.get("app").init(process);

                    process.abstractionInterfaceMap.put("app.pl", new PL());
                    process.abstractionInterfaceMap.get("app.pl").init(process);

                    process.abstractionInterfaceMap.put("app.beb", new BEB());
                    process.abstractionInterfaceMap.get("app.beb").init(process);

                    process.abstractionInterfaceMap.put("app.beb.pl", new PL());
                    process.abstractionInterfaceMap.get("app.beb.pl").init(process);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.PROC_DESTROY_SYSTEM)) {
                    System.out.println(process.debugName + " received PROC DESTROY SYSTEM \n");
                    process.abstractionInterfaceMap.clear();

                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_BROADCAST)) {
                    System.out.println(process.debugName + " received APP BROADCAST \n");
                    process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_VALUE)) {
                    process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_WRITE)) {
                    System.out.println(process.debugName + " received APP WRITE \n");
                    process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    System.out.println(process.debugName + " received NNAR INTERNAL READ \n");
                    String toAbstractionId = message.getNetworkMessage().getMessage().getToAbstractionId();
                    String register = String.valueOf(toAbstractionId.charAt(9));
                    registerAbstraction(register);
                    process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)) {
                    System.out.println(process.debugName + " received NNAR INTERNAL VALUE \n");
                    process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    System.out.println(process.debugName + " received NNAR INTERNAL WRITE \n");
                    process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)) {
                    System.out.println(process.debugName + " received NNAR INTERNAL ACK \n");
                    process.messages.add(message);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_READ)) {
                    System.out.println(process.debugName + " received APP READ \n");
                    process.messages.add(message);
                }

                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_PROPOSE)) {

                    process.value = message.getNetworkMessage().getMessage().getAppPropose().getValue().getV();
                    String topic = message.getNetworkMessage().getMessage().getAppPropose().getTopic();
                    process.ucTopic = "app.uc["+topic+"]";

                    process.abstractionInterfaceMap.put(process.ucTopic, new UC());
                    process.abstractionInterfaceMap.get(process.ucTopic).init(process);
                    process.abstractionInterfaceMap.put(process.ucTopic+".ec", new EC());
                    process.abstractionInterfaceMap.get(process.ucTopic+".ec").init(process);
                    process.abstractionInterfaceMap.put(process.ucTopic+".ec"+".eld", new ELD());
                    process.abstractionInterfaceMap.get(process.ucTopic+".ec"+".eld").init(process);
                    process.abstractionInterfaceMap.put(process.ucTopic+".ec"+".eld"+".epfd", new EPFD());
                    process.abstractionInterfaceMap.get(process.ucTopic+".ec"+".eld"+".epfd").init(process);
                    process.abstractionInterfaceMap.put(process.ucTopic+".ec"+".eld"+".epfd"+".pl", new PL());
                    process.abstractionInterfaceMap.get(process.ucTopic+".ec"+".eld"+".epfd"+".pl").init(process);
                    process.abstractionInterfaceMap.put(process.ucTopic+".ec"+".pl", new PL());
                    process.abstractionInterfaceMap.get(process.ucTopic+".ec"+".pl").init(process);
                    process.abstractionInterfaceMap.put(process.ucTopic+".ec"+".beb", new BEB());
                    process.abstractionInterfaceMap.get(process.ucTopic+".ec"+".beb").init(process);
                    process.abstractionInterfaceMap.put(process.ucTopic+".ec"+".beb"+".pl", new PL());
                    process.abstractionInterfaceMap.get(process.ucTopic+".ec"+".beb"+".pl").init(process);

                    process.messages.add(Message.newBuilder()
                            .setToAbstractionId(process.ucTopic)
                            .setSystemId("sys-1")
                            .setType(Message.Type.UC_PROPOSE)
                            .setUcPropose(UcPropose.newBuilder()
                                    .setValue(Value.newBuilder()
                                            .setDefined(true)
                                            .setV(process.sharedMemory.value)
                                            .build())
                                    .build())
                            .build());
                }

                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EC_INTERNAL_NEW_EPOCH)) {

                    if(process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null){
                        if(message.getToAbstractionId().contains("ec")){
                            EpInternalState state = message.getNetworkMessage().getMessage().getEpInternalState();
                            process.abstractionInterfaceMap.put(message.getToAbstractionId(), new EC());
                        }
                    }
                    System.out.println("NETWORK: New epoch\n");
                    Message msgBeb = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setToAbstractionId(message.getNetworkMessage().getMessage().getToAbstractionId())
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(process.getProcByPort(message.getNetworkMessage().getSenderListeningPort()))
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    process.messages.add(msgBeb);
                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EP_INTERNAL_READ)) {
                    if(process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null){
                        if(message.getToAbstractionId().contains("ep")){
                            EpInternalState state = message.getNetworkMessage().getMessage().getEpInternalState();
                            process.abstractionInterfaceMap.put(message.getNetworkMessage().getMessage().getToAbstractionId(), new EP(process, state, process.epState.ets, process.epState.leader));
//                            process.abstractionInterfaceMap.put(message.getNetworkMessage().getMessage().getToAbstractionId(), new EP(process, state, process.epState.ets, process.leader));
                        }
                    }
                    System.out.println("NETWORK: internal read\n");
                    Message msgBeb = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setToAbstractionId(message.getNetworkMessage().getMessage().getToAbstractionId())
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(process.getProcByPort(message.getNetworkMessage().getSenderListeningPort()))
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    process.messages.add(msgBeb);
                }

                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EP_INTERNAL_WRITE)) {
                    if(process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null){
                        if(message.getToAbstractionId().contains("ep")){
                            EpInternalState state = message.getNetworkMessage().getMessage().getEpInternalState();
                            process.abstractionInterfaceMap.put(message.getNetworkMessage().getMessage().getToAbstractionId(), new EP(process, state, process.epState.ets, process.epState.leader));
//                            process.abstractionInterfaceMap.put(message.getNetworkMessage().getMessage().getToAbstractionId(), new EP(process, state, process.epState.ets, process.leader));
                        }
                    }
                    System.out.println("NETWORK: internal write\n");
                    Message msgBeb = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setToAbstractionId(message.getNetworkMessage().getMessage().getToAbstractionId())
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(process.getProcByPort(message.getNetworkMessage().getSenderListeningPort()))
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    process.messages.add(msgBeb);
                }

                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.EP_INTERNAL_DECIDED)) {
                    if(process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null){
                        if(message.getToAbstractionId().contains("ep")){
                            EpInternalState state = message.getNetworkMessage().getMessage().getEpInternalState();
                            process.abstractionInterfaceMap.put(message.getNetworkMessage().getMessage().getToAbstractionId(), new EP(process, state, process.epState.ets, process.epState.leader));
//                            process.abstractionInterfaceMap.put(message.getNetworkMessage().getMessage().getToAbstractionId(), new EP(process, state, process.epState.ets, process.leader));
                        }
                    }
                    System.out.println("NETWORK: internal decided\n");
                    Message msgBeb = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setToAbstractionId(message.getNetworkMessage().getMessage().getToAbstractionId())
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(process.getProcByPort(message.getNetworkMessage().getSenderListeningPort()))
                                    .setMessage(message.getNetworkMessage().getMessage())
                                    .build())
                            .build();
                    process.messages.add(msgBeb);
                }
                else {
                    if(process.abstractionInterfaceMap.get(message.getToAbstractionId()) == null){
                        if(message.getToAbstractionId().contains("ep")){
                            EpInternalState state = message.getNetworkMessage().getMessage().getEpInternalState();
                            process.abstractionInterfaceMap.put(message.getToAbstractionId(), new EP(process, state, process.epState.ets, process.epState.leader));
//                            process.abstractionInterfaceMap.put(message.getToAbstractionId(), new EP(process, state, process.epState.ets, process.leader));
                        }
                    }

                    process.messages.add(
                            Message.newBuilder().setType(Message.Type.PL_DELIVER)
                                    .setToAbstractionId(message.getToAbstractionId())
                                    .setPlDeliver(PlDeliver.newBuilder()
                                            .setSender(ProcUtil.getProcessByAddress(process.processes, message.getNetworkMessage().getSenderHost(), message.getNetworkMessage().getSenderListeningPort()))
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
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
     }
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
