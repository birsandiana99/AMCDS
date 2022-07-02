package client.Algorithm;
import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public class APP  implements AbstractionInterface{

    private Proc process;


    @Override
    public void init(Proc process) {
        this.process = process;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_BROADCAST)) {
                    Message appValue = Message.newBuilder()
                            .setType(Message.Type.APP_VALUE)
                            .setFromAbstractionId("app")
                            .setToAbstractionId("app.beb")
                            .setSystemId("sys-1")
                            .setAppValue(AppValue.newBuilder()
                                    .setValue(message.getPlDeliver().getMessage().getAppBroadcast().getValue())
                                    .build())
                            .build();
                    this.bebBroadcastParams(appValue, "app", "app.beb");
                    return true;
                }

            case BEB_DELIVER:
                Message appValue = Message.newBuilder()
                        .setType(Message.Type.APP_VALUE)
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.pl")
                        .setSystemId("sys-1")
                        .setAppValue(message.getBebDeliver().getMessage().getAppValue())
                        .build();

                this.plSendParams(appValue, "app", "app.pl");
                return true;
        }
        return false;
    }


    private void bebBroadcastParams(Message message, String from, String to) {
        this.process.messages.add(Message.newBuilder()
                .setType(Message.Type.BEB_BROADCAST)
                .setBebBroadcast(BebBroadcast.newBuilder()
                        .setMessage(message)
                        .build())
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .setSystemId("sys-1")
                .build());
    }

    private void plSendParams(Message message, String from, String to) {
        this.process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setMessage(message)
                        .build())
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .setSystemId("sys-1")
                .build());
    }
}
