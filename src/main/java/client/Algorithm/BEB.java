package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public class BEB implements AbstractionInterface {

    private Proc process;


    @Override
    public void init(Proc process) {
        this.process=process;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case BEB_BROADCAST:
                if (message.getToAbstractionId().equals("app.beb")) {
                    Message broadcastMessage = Message.newBuilder()
                            .setSystemId("sys-1")
                            .setFromAbstractionId(message.getBebBroadcast().getMessage().getToAbstractionId())
                            .setToAbstractionId(message.getBebBroadcast().getMessage().getToAbstractionId() + ".pl")
                            .setAppValue(message.getBebBroadcast().getMessage().getAppValue())
                            .build();
                    for (ProcessId pid: this.process.processes) {
                        this.plSendParams(broadcastMessage, message.getBebBroadcast().getMessage().getToAbstractionId(),
                                message.getBebBroadcast().getMessage().getToAbstractionId() + ".pl", pid);
                    }
                    return true;
                }
                if (message.getBebBroadcast().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    Message nnarInternalRead = message.getBebBroadcast().getMessage();

                    for (ProcessId pid: this.process.processes) {
                        this.plSendParams(nnarInternalRead, message.getBebBroadcast().getMessage().getToAbstractionId(),
                                message.getBebBroadcast().getMessage().getToAbstractionId() + ".pl", pid);
                    }
                    return true;
                }

                if (message.getBebBroadcast().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    Message nnarInternalWrite = message.getBebBroadcast().getMessage();

                    for (ProcessId pid: this.process.processes) {
                        this.plSendParams(nnarInternalWrite, message.getBebBroadcast().getMessage().getToAbstractionId(),
                                message.getBebBroadcast().getMessage().getToAbstractionId() + ".pl", pid);
                    }
                    return true;
                }
                else{
                    this.bebBroadcastConsensus(message.getBebBroadcast().getMessage());
                    return true;
                }

            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_VALUE)){
                    Message appValue = Message.newBuilder()
                            .setType(Message.Type.APP_VALUE)
                            .setSystemId("sys-1")
                            .setFromAbstractionId("app.beb")
                            .setToAbstractionId("app")
                            .setAppValue(message.getPlDeliver().getMessage().getAppValue())
                            .build();
                    this.bebDeliverParams(appValue, "app.beb", "app", process.pid);
                    return true;
                }
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    this.bebDeliverParams(message.getPlDeliver().getMessage(), message.getToAbstractionId(),
                            this.process.sharedMemory.appNnar, message.getPlDeliver().getSender());
                    return true;
                }
                else {
                    this.plDeliverConsensus(message.getPlDeliver().getSender(), message.getPlDeliver().getMessage());
                    return true;
                }

            case BEB_DELIVER:
                if (message.getBebDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_WRITE)) {
                    int timestamp = message.getBebDeliver().getMessage().getNnarInternalWrite().getTimestamp();
                    int writerRank = message.getBebDeliver().getMessage().getNnarInternalWrite().getWriterRank();

                    if (timestamp > this.process.sharedMemory.ts || (timestamp == this.process.sharedMemory.ts && writerRank > this.process.sharedMemory.writerRank)) {
                        this.process.sharedMemory.ts = timestamp;
                        this.process.sharedMemory.writerRank = writerRank;
                        this.process.value = message.getBebDeliver().getMessage().getNnarInternalWrite().getValue().getV();
                    }

                    Message nnarInternalAck = Message.newBuilder()
                            .setType(Message.Type.NNAR_INTERNAL_ACK)
                            .setSystemId("sys-1")
                            .setFromAbstractionId(this.process.sharedMemory.appNnar)
                            .setToAbstractionId(this.process.sharedMemory.appNnar)
                            .setNnarInternalAck(NnarInternalAck.newBuilder()
                                    .setReadId(message.getBebDeliver().getMessage().getNnarInternalWrite().getReadId())
                                    .build())
                            .build();
                    this.plSendParams(nnarInternalAck, message.getToAbstractionId(), message.getToAbstractionId() + ".pl", process.pid);
                    return true;
                }
        }
        return false;
    }

    public void bebDeliverParams(Message message, String from, String to, ProcessId pid) {
        this.process.messages.add(Message.newBuilder()
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

    public void plSendParams(Message message, String from, String to, ProcessId pid) {
        this.process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setDestination(pid)
                        .setMessage(message)
                        .build())
                .setSystemId("sys-1")
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .build());
    }

    public void bebBroadcastConsensus(Message message) {
        for (ProcessId pid : this.process.processes) {
            this.process.messages.add(Message.newBuilder()
                    .setType(Message.Type.PL_SEND)
                    .setSystemId("sys-1")
                    .setToAbstractionId("beb")
                    .setPlSend(PlSend.newBuilder()
                            .setDestination(pid)
                            .setMessage(message)
                            .build())
                    .build());
        }
    }

    public void plDeliverConsensus(ProcessId pid, Message message) {
        this.process.messages.add(Message.newBuilder()
                .setType(Message.Type.BEB_DELIVER)
                .setSystemId("sys-1")
                .setToAbstractionId(message.getToAbstractionId())
                .setBebDeliver(BebDeliver.newBuilder()
                        .setSender(pid)
                        .setMessage(message)
                        .build())
                .build());
    }
}
