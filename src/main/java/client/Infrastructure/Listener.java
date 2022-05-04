package client.Infrastructure;

import client.Algorithm.APP;
import client.Algorithm.BEB;
import client.Algorithm.PL;
import client.Utilities.EventsListener;
import client.Utilities.MessageUtils;
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

    private static int registerValue = 0;

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
                    System.out.println("Got proc init system " + process.owner + "-" + process.index);

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
                    System.out.println("DESTROY PROC " + process.owner + "-" + process.index);
                    process.abstractionInterfaceMap.clear();

                }
                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_BROADCAST)){
                    System.out.println("Got Network App Broadcast " + process.owner + "-" + process.index);
                    process.messages.add(message);
                }

                else if (message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_VALUE)) {
                    process.messages.add(message);
                }

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
}
