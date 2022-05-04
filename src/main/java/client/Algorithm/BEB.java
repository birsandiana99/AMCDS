package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public class BEB implements AbstractionInterface {

    private Proc process;
    private static String reg = "";


    @Override
    public void init(Proc p) {
        process=p;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case BEB_BROADCAST:
                if (message.getToAbstractionId().equals("app.beb")) {
                    plSend(message.getBebBroadcast().getMessage());
                    return true;
                }

            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_VALUE)){
                    bebDeliver(message.getPlDeliver().getMessage());
                    return true;
                }
        }
        return false;
    }



    private void plSend(Message message) {
        for (ProcessId pid : process.processes) {
            process.messages.add(Message.newBuilder()
                    .setType(Message.Type.PL_SEND)
                    .setPlSend(PlSend.newBuilder()
                            .setDestination(pid)
                            .setMessage(Message.newBuilder()
                                    .setSystemId("sys-1")
                                    .setFromAbstractionId(message.getToAbstractionId())
                                    .setToAbstractionId(message.getToAbstractionId() + ".pl")
                                    .setAppValue(message.getAppValue())
                                    .build())
                            .build())
                    .setSystemId("sys-1")
                    .setFromAbstractionId(message.getToAbstractionId())
                    .setToAbstractionId(message.getToAbstractionId() + ".pl")
                    .build());

        }
    }

    private void bebDeliver(Message message) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.BEB_DELIVER)
                .setBebDeliver(BebDeliver.newBuilder()
                        .setMessage(Message.newBuilder()
                                .setType(Message.Type.APP_VALUE)
                                .setSystemId("sys-1")
                                .setFromAbstractionId("app.beb")
                                .setToAbstractionId("app")
                                .setAppValue(message.getAppValue())
                                .build())

                        .build())
                .setSystemId("sys-1")
                .setFromAbstractionId("app.beb")
                .setToAbstractionId("app")
                .build());
    }
}
