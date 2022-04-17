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
                if(message.getNetworkMessage().getMessage().getType().equals(Message.Type.APP_BROADCAST)) {
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.APP_BROADCAST)
                            .setFromAbstractionId("app.pl")
                            .setToAbstractionId("app")
                            .setSystemId("sys-1")
                            .setAppBroadcast(AppBroadcast.newBuilder()
                                .setValue(message.getNetworkMessage().getMessage().getAppBroadcast().getValue())
                                .build())
                            .build();
                    plDeliver(msg);
                    return true;
                }
            case PL_SEND:
                if (message.getToAbstractionId().equals("app.pl")) {
                    plSend(message.getPlSend().getMessage());
                    return true;
                }

            case PL_DELIVER:
                //TODO:
                return true;
        }
        return false;
    }


    private void plSend( Message message) {
        Socket socket = null;
        try{
            socket = new Socket(Proc.ADDR_HUB, Proc.PORT_HUB);

            Message newMessage = Message.newBuilder()
                    .setType(Message.Type.NETWORK_MESSAGE)
                    .setMessageUuid(message.getMessageUuid())
                    .setFromAbstractionId("app.pl")
                    .setToAbstractionId("hub")
                    .setSystemId("sys-1")
                    .setNetworkMessage(NetworkMessage.newBuilder()
                            .setSenderHost(Proc.ADDR_HUB)
                            .setSenderListeningPort(process.port)
                            .setMessage(Message.newBuilder()
                                    .setType(Message.Type.APP_VALUE)
                                    .setMessageUuid(message.getMessageUuid())
                                    .setAppValue(message.getAppValue())
                                    .setFromAbstractionId("app.pl")
                                    .setToAbstractionId("hub")
                                    .setSystemId("sys-1")
                                    .build())
                        ).build();
            MessageUtils.write(socket.getOutputStream(),newMessage);
            socket.close();


        } catch (IOException e) {
            System.out.println("Socket error - cannot send");
        }
    }

    private void plDeliver(Message message) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_DELIVER)
                .setPlDeliver(PlDeliver.newBuilder()
                        .setMessage(message)
                        .build())
                .setFromAbstractionId(message.getFromAbstractionId())
                .setToAbstractionId(message.getToAbstractionId())
                .setSystemId(message.getSystemId())
                .build());

    }
}
