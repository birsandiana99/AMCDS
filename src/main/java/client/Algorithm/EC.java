package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;
import client.Utilities.ECState;
import client.Utilities.ProcUtil;

public class EC implements AbstractionInterface{

    private Proc process;

    @Override
    public void init(Proc process) {
        this.process = process;
        this.process.ecState = new ECState(this.process);
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case ELD_TRUST:

                this.process.ecState.trusted = message.getEldTrust().getProcess();
                if (message.getEldTrust().getProcess().equals(ProcUtil.getCurrentProcess(this.process))) {
                    this.process.ecState.timestamp = this.process.ecState.timestamp + this.process.processes.size();
                    Message ecInternalNewEpoch = Message.newBuilder()
                            .setType(Message.Type.EC_INTERNAL_NEW_EPOCH)
                            .setSystemId("sys-1")
                            .setFromAbstractionId(this.process.ucTopic+".ec")
                            .setToAbstractionId(this.process.ucTopic+".ec")
                            .setEcInternalNewEpoch(EcInternalNewEpoch.newBuilder().setTimestamp(this.process.ecState.timestamp).build())
                            .build();
                    this.bebBroadcastParams(ecInternalNewEpoch, this.process.ucTopic + ".ec", this.process.ucTopic + ".ec.beb");
                }
                return true;
            case BEB_DELIVER:
                if (message.getBebDeliver().getMessage().getType().equals(Message.Type.EC_INTERNAL_NEW_EPOCH)) {
                    System.out.println("new epoch - EC");
                    this.newEpoch(message.getBebDeliver().getSender(), message.getBebDeliver().getMessage().getEcInternalNewEpoch().getTimestamp());
                    return true;
                }
                return false;
            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.EC_INTERNAL_NACK)) {
                    if (this.process.ecState.trusted.equals(ProcUtil.getCurrentProcess(this.process))) {
                        this.process.ecState.timestamp = this.process.ecState.timestamp + this.process.processes.size();
                        Message ecInternalNewEpoch = Message.newBuilder()
                                .setType(Message.Type.EC_INTERNAL_NEW_EPOCH)
                                .setFromAbstractionId(this.process.ucTopic+".ec")
                                .setToAbstractionId(this.process.ucTopic+".ec")
                                .setSystemId("sys-1")
                                .setEcInternalNewEpoch(EcInternalNewEpoch.newBuilder()
                                        .setTimestamp(this.process.ecState.timestamp)
                                        .build())
                                .build();
                        this.bebBroadcastParams(ecInternalNewEpoch, this.process.ucTopic + ".ec", this.process.ucTopic + ".ec.beb");
                    }
                    return true;
                }
                return false;
        }
        return false;
    }

    private void newEpoch(ProcessId leader, int newTimestamp) {
        if (leader.equals(this.process.ecState.trusted) && newTimestamp > this.process.ecState.lastTimestamp) {

            this.process.ecState.lastTimestamp = newTimestamp;
            this.process.messages.add(Message.newBuilder()
                    .setType(Message.Type.EC_START_EPOCH)
                    .setSystemId("sys-1")
                    .setToAbstractionId(this.process.ucTopic)
                    .setEcStartEpoch(EcStartEpoch.newBuilder()
                            .setNewTimestamp(newTimestamp)
                            .setNewLeader(leader)
                            .build())
                    .build());
        }else {
            Message ecInternalNack = Message.newBuilder()
                    .setFromAbstractionId(this.process.ucTopic+".ec")
                    .setToAbstractionId(this.process.ucTopic+".ec")
                    .setType(Message.Type.EC_INTERNAL_NACK)
                    .setEcInternalNack(EcInternalNack.newBuilder()
                            .build())
                    .build();
            this.plSendParams(ecInternalNack, this.process.ucTopic + ".ec", this.process.ucTopic + ".ec.pl", leader);
        }
    }

    private void bebBroadcastParams(Message message, String from, String to) {
        Message bebBroadcast = Message.newBuilder()
                .setType(Message.Type.BEB_BROADCAST)
                .setBebBroadcast(BebBroadcast.newBuilder()
                        .setMessage(message)
                        .build())
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .build();
        this.process.messages.add(bebBroadcast);
    }

    public void plSendParams(Message message, String from, String to, ProcessId pid) {
        this.process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setMessage(message)
                        .setDestination(pid)
                        .build())
                .setSystemId("sys-1")
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .build());
    }

}
