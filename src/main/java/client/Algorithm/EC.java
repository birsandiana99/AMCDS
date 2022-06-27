package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;
import client.Utilities.ECState;
import client.Utilities.ProcUtil;

public class EC implements AbstractionInterface{

    private Proc process;

    @Override
    public void init(Proc p) {
        process = p;
        process.ecState = new ECState(process);
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case ELD_TRUST:

                process.ecState.trusted = message.getEldTrust().getProcess();
                if(message.getEldTrust().getProcess().equals(ProcUtil.getCurrentProcess(process))) {
                    process.ecState.timestamp = process.ecState.timestamp + process.processes.size();
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.EC_INTERNAL_NEW_EPOCH)
                            .setSystemId("sys-1")
                            .setFromAbstractionId(process.ucTopic+".ec")
                            .setToAbstractionId(process.ucTopic+".ec")
                            .setEcInternalNewEpoch(EcInternalNewEpoch.newBuilder().setTimestamp(process.ecState.timestamp).build())
                            .build();
                    bebBroadcast(msg, process.ucTopic+".ec", process.ucTopic+".ec.beb");
                }
                return true;
            case BEB_DELIVER:
                if(message.getBebDeliver().getMessage().getType().equals(Message.Type.EC_INTERNAL_NEW_EPOCH)) {
                    System.out.println("new epoch - EC");
                    newEpoch(message.getBebDeliver().getSender(), message.getBebDeliver().getMessage().getEcInternalNewEpoch().getTimestamp());
                    return true;
                }
                return false;
            case PL_DELIVER:
                if(message.getPlDeliver().getMessage().getType().equals(Message.Type.EC_INTERNAL_NACK)) {
                    if(process.ecState.trusted.equals(ProcUtil.getCurrentProcess(process))) {
                        process.ecState.timestamp = process.ecState.timestamp + process.processes.size();
                        Message msg = Message.newBuilder()
                                .setType(Message.Type.EC_INTERNAL_NEW_EPOCH)
                                .setFromAbstractionId(process.ucTopic+".ec")
                                .setToAbstractionId(process.ucTopic+".ec")
                                .setSystemId("sys-1")
                                .setEcInternalNewEpoch(EcInternalNewEpoch.newBuilder()
                                        .setTimestamp(process.ecState.timestamp)
                                        .build())
                                .build();
                        bebBroadcast(msg, process.ucTopic+".ec", process.ucTopic+".ec.beb");
                    }
                    return true;
                }
                return false;
        }

        return false;
    }

    private void newEpoch(ProcessId l, int newts) {
        if(l.equals(process.ecState.trusted) && newts > process.ecState.lastTimestamp) {

            process.ecState.lastTimestamp = newts;
            process.messages.add(Message.newBuilder()
                    .setType(Message.Type.EC_START_EPOCH)
                    .setSystemId("sys-1")
                    .setToAbstractionId(process.ucTopic)
                    .setEcStartEpoch(EcStartEpoch.newBuilder()
                            .setNewTimestamp(newts)
                            .setNewLeader(l)
                            .build())
                    .build());
        }else {
            Message msg = Message.newBuilder()
                    .setFromAbstractionId(process.ucTopic+".ec")
                    .setToAbstractionId(process.ucTopic+".ec")
                    .setType(Message.Type.EC_INTERNAL_NACK)
                    .setEcInternalNack(EcInternalNack.newBuilder()
                            .build())
                    .build();
            plSend(msg, process.ucTopic+".ec", process.ucTopic+".ec.pl", l);
        }
    }

    private void bebBroadcast(Message m, String from, String to) {
        Message msg = Message.newBuilder()
                .setType(Message.Type.BEB_BROADCAST)
                .setBebBroadcast(BebBroadcast.newBuilder()
                        .setMessage(m)
                        .build())
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .build();
        process.messages.add(msg);
    }

    public void plSend(Message m, String from, String to, ProcessId pid) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setMessage(m)
                        .setDestination(pid)
                        .build())
                .setSystemId("sys-1")
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .build());
    }

}
