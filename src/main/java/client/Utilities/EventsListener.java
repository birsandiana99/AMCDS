package client.Utilities;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

import java.util.ArrayList;
import java.util.List;


public class EventsListener extends Thread {
    private Thread thread;
    private Proc process;

    public EventsListener(Proc process) {
        this.process = process;
    }

    public void start() {
        if (this.thread ==null) {
            this.thread = new Thread(this);
            this.thread.start();
        }
    }

    public void run() {
        try {
            while (true) {
                Thread.sleep(10);
                if (this.process.messages.size() > 0) {
                    List<Message> copyEventQueue = new ArrayList<>(this.process.messages);
                    for (Message message: copyEventQueue){
                        try {
                            boolean isHandled = this.process.abstractionInterfaceMap.get(message.getToAbstractionId()).handle(message);
                            if (isHandled) {
                                this.process.messages.remove(message);
                            }
                        } catch (NullPointerException e){
                            System.out.println("EXCEPTION ON: \n"+message);
                            System.out.println(this.process.abstractionInterfaceMap);
                            this.process.messages.remove(message);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
