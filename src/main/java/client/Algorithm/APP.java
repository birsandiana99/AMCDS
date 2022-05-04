package client.Algorithm;
import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public class APP  implements AbstractionInterface{

    private Proc process;


    @Override
    public void init(Proc p) {
        process = p;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_BROADCAST)) {
                    bebBroadcast(message.getPlDeliver().getMessage());
                    return true;
                }
            case BEB_DELIVER:
                plSend(message.getBebDeliver().getMessage());
                return true;
        }
        return false;
    }


    private void bebBroadcast(Message message) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.BEB_BROADCAST)
                .setBebBroadcast(BebBroadcast.newBuilder()
                        .setMessage(Message.newBuilder()
                                .setType(Message.Type.APP_VALUE)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.beb")
                                .setSystemId("sys-1")
                                .setAppValue(AppValue.newBuilder()
                                        .setValue(message.getAppBroadcast().getValue())
                                        .build())
                                .build())
                        .build())
                .setFromAbstractionId("app")
                .setToAbstractionId("app.beb")
                .setSystemId("sys-1")
                .build());

    }

    private void plSend(Message message) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setMessage(Message.newBuilder()
                                .setType(Message.Type.APP_VALUE)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.pl")
                                .setSystemId("sys-1")
                                .setAppValue(message.getAppValue())
                                .build())
                        .build())
                .setFromAbstractionId("app")
                .setToAbstractionId("app.pl")
                .setSystemId("sys-1")
                .build());
    }

}
