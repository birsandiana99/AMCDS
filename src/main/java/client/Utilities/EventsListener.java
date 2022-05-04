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

    public void start() {
        if(t==null) {
            t = new Thread(this);
            t.start();
        }
    }

    public void run() {
        try {
            while (true) {
                Thread.sleep(10);
                if (process.messages.size() > 0) {
                    List<Message> copyEventQueue = new ArrayList<>(process.messages);
                    for (Message message: copyEventQueue){
                        try {
                            boolean isHandled = process.abstractionInterfaceMap.get(message.getToAbstractionId()).handle(message);
                            if (isHandled) {
                                process.messages.remove(message);
                            }
                        } catch (NullPointerException e){
                            System.out.println("EXCEPTION ON: \n"+message);
                            System.out.println(process.abstractionInterfaceMap);
                            process.messages.remove(message);
                        }
                    }
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
