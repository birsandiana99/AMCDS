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
                if (message.getBebBroadcast().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    Message intRead = message.getBebBroadcast().getMessage();
                    for (ProcessId pid : process.processes) {
                        Message msg = Message.newBuilder()
                                .setType(Message.Type.PL_SEND)
                                .setFromAbstractionId(message.getToAbstractionId())
                                .setToAbstractionId(message.getToAbstractionId() + ".pl")
                                .setPlSend(PlSend.newBuilder()
                                        .setDestination(pid)
                                        .setMessage(intRead)
                                        .build())
                                .setSystemId("sys-1")
                                .build();
                        process.messages.add(msg);
                    }
                    return true;
                }

                if (message.getBebBroadcast().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    Message intWrite = message.getBebBroadcast().getMessage();
                    for (ProcessId pid : process.processes) {
                        Message msg = Message.newBuilder()
                                .setType(Message.Type.PL_SEND)
                                .setFromAbstractionId(message.getToAbstractionId())
                                .setToAbstractionId(message.getToAbstractionId() + ".pl")
                                .setPlSend(PlSend.newBuilder()
                                        .setDestination(pid)
                                        .setMessage(intWrite)
                                        .build())
                                .setSystemId("sys-1")
                                .build();
                        process.messages.add(msg);
                    }
                    return true;
                }

            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_VALUE)){
                    bebDeliver(message.getPlDeliver().getMessage());
                    return true;
                }
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)){
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.BEB_DELIVER)
                            .setFromAbstractionId(message.getToAbstractionId())
                            .setToAbstractionId("app.nnar[" + process.register + "]")
                            .setBebDeliver(BebDeliver.newBuilder()
                                    .setSender(message.getPlDeliver().getSender())
                                    .setMessage(message.getPlDeliver().getMessage())
                                    .build())
                            .setSystemId("sys-1")
                            .build();
                    return true;
                }
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)){
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.PL_DELIVER)
                            .setFromAbstractionId(message.getToAbstractionId())
                            .setToAbstractionId("app.nnar[" + process.register + "]")
                            .setPlDeliver(PlDeliver.newBuilder()
                                    .setSender(message.getPlDeliver().getSender())
                                    .setMessage(message.getPlDeliver().getMessage())
                                    .build())
                            .setSystemId("sys-1")
                            .build();
                    return true;
                }
            case BEB_DELIVER:
                if (message.getBebDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    int ts1 = message.getBebDeliver().getMessage().getNnarInternalWrite().getTimestamp();
                    int wr1 = message.getBebDeliver().getMessage().getNnarInternalWrite().getWriterRank();

                    if (ts1 > process.ts && wr1 > process.writerRank) {
                        process.ts = ts1;
                        process.writerRank = wr1;
                        process.value = message.getBebDeliver().getMessage().getNnarInternalWrite().getValue().getV();
                    }

                    Message msg = Message.newBuilder()
                            .setType(Message.Type.PL_SEND)
                            .setFromAbstractionId(message.getToAbstractionId())
                            .setToAbstractionId(message.getToAbstractionId() + ".pl")
                            .setPlSend(PlSend.newBuilder()
                                    .setDestination(process.pid)
                                    .setMessage(Message.newBuilder()
                                            .setType(Message.Type.NNAR_INTERNAL_ACK)
                                            .setSystemId("sys-1")
                                            .setFromAbstractionId("app.nnar[" + process.register + "]")
                                            .setToAbstractionId("app.nnar[" + process.register + "]")
                                            .setNnarInternalAck(NnarInternalAck.newBuilder()
                                                    .setReadId(message.getBebDeliver().getMessage().getNnarInternalWrite().getReadId())
                                                    .build())
                                            .build())
                                    .build())
                            .setSystemId("sys-1")
                            .build();
                    process.messages.add(msg);
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
