package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public class BEB implements AbstractionInterface {

    private Proc process;


    @Override
    public void init(Proc p) {
        process=p;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case BEB_BROADCAST:
                if (message.getToAbstractionId().equals("app.beb")) {
                    Message msg = Message.newBuilder()
                            .setSystemId("sys-1")
                            .setFromAbstractionId(message.getBebBroadcast().getMessage().getToAbstractionId())
                            .setToAbstractionId(message.getBebBroadcast().getMessage().getToAbstractionId() + ".pl")
                            .setAppValue(message.getBebBroadcast().getMessage().getAppValue())
                            .build();
                    for (ProcessId pid: process.processes) {
                        plSendParams(msg, message.getBebBroadcast().getMessage().getToAbstractionId(),
                                message.getBebBroadcast().getMessage().getToAbstractionId() + ".pl", pid);
                    }
                    return true;
                }
                if (message.getBebBroadcast().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    Message msgInternalRead = message.getBebBroadcast().getMessage();

                    for (ProcessId pid: process.processes) {
                        plSendParams(msgInternalRead, message.getBebBroadcast().getMessage().getToAbstractionId(),
                                message.getBebBroadcast().getMessage().getToAbstractionId() + ".pl", pid);
                    }
                    return true;
                }

                if (message.getBebBroadcast().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    Message msgInternalWrite = message.getBebBroadcast().getMessage();

                    for (ProcessId pid: process.processes) {
                        plSendParams(msgInternalWrite, message.getBebBroadcast().getMessage().getToAbstractionId(),
                                message.getBebBroadcast().getMessage().getToAbstractionId() + ".pl", pid);
                    }
                    return true;
                }
                else{
                    this.beb_broadcast_cons(message.getBebBroadcast().getMessage());
                    return true;
                }

            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_VALUE)){
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.APP_VALUE)
                            .setSystemId("sys-1")
                            .setFromAbstractionId("app.beb")
                            .setToAbstractionId("app")
                            .setAppValue(message.getPlDeliver().getMessage().getAppValue())
                            .build();
                    bebDeliverParams(msg, "app.beb", "app", process.pid);
                    return true;
                }
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)){
                    bebDeliverParams(message.getPlDeliver().getMessage(), message.getToAbstractionId(), process.sharedMemory.appNnar, message.getPlDeliver().getSender());
                    return true;
                }
                else {
                    this.pl_deliver_cons(message.getPlDeliver().getSender(), message.getPlDeliver().getMessage());
                    return true;
                }

            case BEB_DELIVER:
                if (message.getBebDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    int ts1 = message.getBebDeliver().getMessage().getNnarInternalWrite().getTimestamp();
                    int wr1 = message.getBebDeliver().getMessage().getNnarInternalWrite().getWriterRank();

                    if (ts1 > process.sharedMemory.ts || (ts1 == process.sharedMemory.ts && wr1 > process.sharedMemory.writerRank)) {
                        process.sharedMemory.ts = ts1;
                        process.sharedMemory.writerRank = wr1;
                        process.value = message.getBebDeliver().getMessage().getNnarInternalWrite().getValue().getV();
                    }

                    Message msg = Message.newBuilder()
                            .setType(Message.Type.NNAR_INTERNAL_ACK)
                            .setSystemId("sys-1")
                            .setFromAbstractionId(process.sharedMemory.appNnar)
                            .setToAbstractionId(process.sharedMemory.appNnar)
                            .setNnarInternalAck(NnarInternalAck.newBuilder()
                                    .setReadId(message.getBebDeliver().getMessage().getNnarInternalWrite().getReadId())
                                    .build())
                            .build();
                    plSendParams(msg, message.getToAbstractionId(), message.getToAbstractionId() + ".pl", process.pid);
                    return true;
                }
        }
        return false;
    }

    public void bebDeliverParams(Message message, String from, String to, ProcessId pid) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.BEB_DELIVER)
                .setBebDeliver(BebDeliver.newBuilder()
                        .setMessage(message)
                        .setSender(pid)
                        .build())
                .setSystemId("sys-1")
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .build());
    }

    public void plSendParams(Message msg, String from, String to, ProcessId pid) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setDestination(pid)
                        .setMessage(msg)
                        .build())
                .setSystemId("sys-1")
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .build());
    }

    public void beb_broadcast_cons(Message m) {
        for(ProcessId pid : process.processes) {
            process.messages.add(Message.newBuilder()
                    .setType(Message.Type.PL_SEND)
                    .setSystemId("sys-1")
                    .setToAbstractionId("beb")
                    .setPlSend(PlSend.newBuilder()
                            .setDestination(pid)
                            .setMessage(m)
                            .build())
                    .build());
        }
    }

    public void pl_deliver_cons(ProcessId p, Message m) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.BEB_DELIVER)
                .setSystemId("sys-1")
                .setToAbstractionId(m.getToAbstractionId())
                .setBebDeliver(BebDeliver.newBuilder()
                        .setSender(p)
                        .setMessage(m)
                        .build())
                .build());
    }
}
