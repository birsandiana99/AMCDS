package client.Algorithm;

import client.Infrastructure.Proc;
import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;
import client.Utilities.EPState;
import client.Utilities.ProcUtil;

import java.util.HashMap;

public class EP implements AbstractionInterface{

    private Proc process;

    @Override
    public void init(Proc p) { }

    public EP(Proc p, EpInternalState state, int ets, ProcessId l) {
        process = p;
        process.epState = new EPState(state, ets, l);
    }

    private Integer getEpFromString(String toAbstractionId){
        String arr[] = toAbstractionId.split("\\.");
        String res = "-1";
        for(String s: arr){
            if(s.contains("ep")){
                res = s;
                res = res.substring(3, res.length() - 1);
            }
        }
        if (!res.equals("") && !res.isEmpty()){
            return Integer.parseInt(res);
        } else {
            return -1;
        }

    }

    @Override
    public boolean handle(Message message) {
        if(process.epState.halted) {
            return false;
        }

        int ts = -1;
        if(message.getToAbstractionId().contains("ep")) {
            ts = getEpFromString(message.getToAbstractionId());
        }

        switch (message.getType()) {
            case EP_PROPOSE:
                System.out.println("EP Propose: EP\n");
                if(ts == process.epState.ets) {
                    if(ProcUtil.getCurrentProcess(process).equals(process.leader)) {
                        process.epState.tmpval = Value.newBuilder().setDefined(true).setV(message.getEpPropose().getValue().getV()).build();
                        Message msg = Message.newBuilder()
                                .setType(Message.Type.EP_INTERNAL_READ)
                                .setEpInternalRead(EpInternalRead.newBuilder().build())
                                .setToAbstractionId(process.ucTopic+".ep["+process.epState.ets+"]")
                                .build();
                        bebBroadcast(msg, process.ucTopic+".ep["+process.epState.ets+"]", process.ucTopic+".ec"+".beb");
                    }
                    return true;
                }
            case BEB_DELIVER:
                BebDeliver m = message.getBebDeliver();
                if(m.getMessage().getToAbstractionId().contains("ep") && Character.isDigit((m.getMessage().getToAbstractionId().charAt(2)))) {
                    ts = Integer.parseInt(m.getMessage().getToAbstractionId().substring(2));
                }
                switch(m.getMessage().getType()) {
                    case EP_INTERNAL_READ:
                        if(ts == process.epState.ets) {
                            System.out.println("internal read - EP");
                            Message msg = Message.newBuilder()
                                    .setType(Message.Type.EP_INTERNAL_STATE)
                                    .setToAbstractionId(process.ucTopic+".ep["+process.epState.ets+"]")
                                    .setEpInternalState(EpInternalState.newBuilder()
                                            .setValueTimestamp(process.epState.st.getValueTimestamp())
                                            .setValue(Value.newBuilder().setDefined(true).setV(process.epState.st.getValue().getV())
                                                    .build()))
                                    .build();

                            plSend(msg, process.ucTopic+".ep["+process.epState.ets+"]", process.ucTopic+".ep["+process.epState.ets+"].pl", m.getSender());
                            return true;
                        }
                    case EP_INTERNAL_WRITE:
                        if(ts == process.epState.ets) {
                            System.out.println("internal write - EP");
                            process.epState.st = EpInternalState.newBuilder()
                                    .setValueTimestamp(process.epState.ets)
                                    .setValue(Value.newBuilder()
                                            .setDefined(true)
                                            .setV(m.getMessage().getEpInternalWrite().getValue().getV())
                                            .build())
                                    .build();
                            Message msg = Message.newBuilder()
                                    .setSystemId("sys-1")
                                    .setFromAbstractionId(process.ucTopic+".ep["+process.epState.ets+"]")
                                    .setToAbstractionId(process.ucTopic+".ep["+process.epState.ets+"]")
                                    .setType(Message.Type.EP_INTERNAL_ACCEPT)
                                    .build();

                            plSend(msg, process.ucTopic+".ep["+process.epState.ets+"]", process.ucTopic+".ep["+process.epState.ets+"].pl", m.getSender());
                            return true;
                        }
                    case EP_INTERNAL_DECIDED:
                        if(ts == process.epState.ets) {
                            System.out.println(process.debugName+" internal decide - EP");
                            process.messages.add(Message.newBuilder().setType(Message.Type.EP_DECIDE)
                                    .setToAbstractionId(process.ucTopic)
                                    .setEpDecide(EpDecide.newBuilder().setEts(process.epState.ets)
                                            .setValue(Value.newBuilder()
                                                    .setDefined(true)
                                                    .setV(m.getMessage().getEpInternalDecided().getValue().getV())
                                                    .build()).build())
                                    .build());
                            return true;
                        }
                }
                return false;

            case PL_DELIVER:
                PlDeliver msg = message.getPlDeliver();
                System.out.println("PL DELIVER EP!!!!!!!!!!::: " + Character.isDigit((msg.getMessage().getToAbstractionId().charAt(2))));

                if(msg.getMessage().getToAbstractionId().contains("ep") && Character.isDigit((msg.getMessage().getToAbstractionId().charAt(2)))) {
                    ts = Integer.parseInt(msg.getMessage().getToAbstractionId().substring(2));
                }
                switch(msg.getMessage().getType()) {
                    case EP_INTERNAL_STATE:
                        if(ts == process.epState.ets) {
                            if(ProcUtil.getCurrentProcess(process).equals(process.leader)) {
                                EpInternalState state = EpInternalState.newBuilder()
                                        .setValueTimestamp(msg.getMessage().getEpInternalState().getValueTimestamp())
                                        .setValue(Value.newBuilder()
                                                .setDefined(true)
                                                .setV(msg.getMessage().getEpInternalState().getValue().getV())
                                                .build())
                                        .build();
                                process.epState.states.put(msg.getSender().getPort(), state);
                                checkStates();
                            }
                            return true;
                        }
                    case EP_INTERNAL_ACCEPT:
                        if(ts == process.epState.ets) {
                            if(ProcUtil.getCurrentProcess(process).equals(process.leader)) {
                                process.epState.accepted++;
                                if(process.epState.accepted > Math.ceil(process.processes.size()/2)) {
                                    process.epState.accepted = 0;
                                    Message msg1 = Message.newBuilder()
                                            .setToAbstractionId(process.ucTopic+".ep["+process.epState.ets+"]")
                                            .setType(Message.Type.EP_INTERNAL_DECIDED)
                                            .setEpInternalDecided(EpInternalDecided.newBuilder()
                                                    .setValue(Value.newBuilder()
                                                            .setDefined(true)
                                                            .setV(process.epState.tmpval.getV())
                                                            .build()))
                                            .build();
                                    bebBroadcast(msg1, process.ucTopic+".ep["+process.epState.ets+"]", process.ucTopic+".ec"+".beb");
                                }
                            }
                            return true;
                        }
                }
                return false;

            case EP_ABORT:
                if(ts == process.epState.ets) {
                    System.out.println("EP abort: EP\n");
                    process.messages.add(Message.newBuilder().setType(Message.Type.EP_ABORTED)
                            .setToAbstractionId(process.ucTopic)
                            .setEpAborted(EpAborted.newBuilder().setEts(process.epState.ets)
                                    .setValue(Value.newBuilder().setDefined(true).setV(process.epState.st.getValue().getV()).build())
                                    .setValueTimestamp(process.epState.st.getValueTimestamp()).build())
                            .build());
                    process.epState.halted = true;
                    return true;
                }
        }
        return false;
    }

    public void checkStates() {
        if(process.epState.states.size() > Math.ceil(process.processes.size()/2)) {
            EpInternalState st = highest();
            if(st.getValue().getDefined()) {
                process.epState.tmpval = Value.newBuilder().setDefined(true).setV(st.getValue().getV()).build();
            }
            process.epState.states = new HashMap<>();
            System.out.println("internal write aici-----------");
            Message msg = Message.newBuilder()
                    .setToAbstractionId(process.ucTopic+".ep["+process.epState.ets+"]")
                    .setType(Message.Type.EP_INTERNAL_WRITE)
                    .setEpInternalWrite(EpInternalWrite.newBuilder()
                            .setValue(Value.newBuilder()
                                    .setDefined(true)
                                    .setV(process.epState.tmpval.getV())
                                    .build()))
                        .build();
            bebBroadcast(msg, process.ucTopic+".ep["+process.epState.ets+"]", process.ucTopic+".ec"+".beb");
        }
    }

    public EpInternalState highest() {
        EpInternalState state = EpInternalState.newBuilder().setValueTimestamp(0).setValue(Value.newBuilder().setDefined(false).build()).build();
        for(Integer port : process.epState.states.keySet()) {
            if(process.epState.states.get(port).getValueTimestamp() > state.getValueTimestamp()) {
                state = process.epState.states.get(port);
            }
        }
        return state;
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
