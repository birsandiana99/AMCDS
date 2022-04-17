package client.Utilities;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

import java.util.ArrayList;
import java.util.List;

public class EventsListener extends Thread {
    private Thread t;
    private Proc process;

    public EventsListener(Proc p) {
        process = p;
    }

    public void run() {
        try {
            while(true) {
                Thread.sleep(10);
                if(process.messages.size() > 0) {
                    List<Message> copyEventQueue = new ArrayList<>(process.messages);
                    for(Message m: copyEventQueue) {
                        try{
                            boolean isHandled = process.abstractionInterfaceMap.get(m.getToAbstractionId()).handle(m);
                            if(isHandled) {
                                process.messages.remove(m);
                            }
                        } catch (NullPointerException e){ //on crash
                            System.out.println("CRASH AT: \n"+m);
                            System.out.println(process.abstractionInterfaceMap);
                            process.messages.remove(m);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() {
        if(t==null) {
            t = new Thread(this);
            t.start();
        }
    }
}
