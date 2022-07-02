package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;
import client.Utilities.EPState;
import client.Utilities.ProcUtil;

import java.util.HashMap;

public class EP implements AbstractionInterface{

    private Proc process;

    @Override
    public void init(Proc process) { }

    public EP(Proc process, EpInternalState state, int ets, ProcessId l) {
        this.process = process;
        this.process.epState = new EPState(state, ets, l);
    }

    private Integer getEpFromString(String toAbstractionId){
        String[] arr = toAbstractionId.split("\\.");
        String res = "-1";
        for (String s: arr){
            if (s.contains("ep")){
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
        if (this.process.epState.halted) {
            return false;
        }

        int timestamp = -1;
        if (message.getToAbstractionId().contains("ep")) {
            timestamp = getEpFromString(message.getToAbstractionId());
        }

        switch (message.getType()) {
            case EP_PROPOSE:
                System.out.println("EP Propose: EP\n");
                if(timestamp == this.process.epState.ets) {
                    if (ProcUtil.getCurrentProcess(this.process).equals(this.process.leader)) {
                        this.process.epState.tmpval = Value.newBuilder().setDefined(true).setV(message.getEpPropose().getValue().getV()).build();
                        Message epInternalRead = Message.newBuilder()
                                .setType(Message.Type.EP_INTERNAL_READ)
                                .setEpInternalRead(EpInternalRead.newBuilder().build())
                                .setToAbstractionId(this.process.ucTopic + ".ep[" + this.process.epState.ets + "]")
                                .build();
                        this.bebBroadcastParams(epInternalRead, this.process.ucTopic + ".ep[" + this.process.epState.ets + "]", this.process.ucTopic + ".ec.beb");
                    }
                    return true;
                }
            case BEB_DELIVER:
                BebDeliver bebDeliver = message.getBebDeliver();
                if (bebDeliver.getMessage().getToAbstractionId().contains("ep") && Character.isDigit((bebDeliver.getMessage().getToAbstractionId().charAt(2)))) {
                    timestamp = Integer.parseInt(bebDeliver.getMessage().getToAbstractionId().substring(2));
                }
                switch (bebDeliver.getMessage().getType()) {
                    case EP_INTERNAL_READ:
                        if (timestamp == process.epState.ets) {
                            System.out.println("internal read - EP");
                            Message epInternalState = Message.newBuilder()
                                    .setType(Message.Type.EP_INTERNAL_STATE)
                                    .setToAbstractionId(this.process.ucTopic + ".ep[" + this.process.epState.ets + "]")
                                    .setEpInternalState(EpInternalState.newBuilder()
                                            .setValueTimestamp(this.process.epState.st.getValueTimestamp())
                                            .setValue(Value.newBuilder().setDefined(true).setV(this.process.epState.st.getValue().getV())
                                                    .build()))
                                    .build();

                            this.plSendParams(epInternalState, this.process.ucTopic + ".ep[" + this.process.epState.ets + "]",
                                    this.process.ucTopic + ".ep[" + this.process.epState.ets + "].pl", bebDeliver.getSender());
                            return true;
                        }
                    case EP_INTERNAL_WRITE:
                        if (timestamp == this.process.epState.ets) {
                            System.out.println("internal write - EP");
                            this.process.epState.st = EpInternalState.newBuilder()
                                    .setValueTimestamp(this.process.epState.ets)
                                    .setValue(Value.newBuilder()
                                            .setDefined(true)
                                            .setV(bebDeliver.getMessage().getEpInternalWrite().getValue().getV())
                                            .build())
                                    .build();
                            Message epInternalAccept = Message.newBuilder()
                                    .setSystemId("sys-1")
                                    .setFromAbstractionId(this.process.ucTopic + ".ep[" + this.process.epState.ets + "]")
                                    .setToAbstractionId(this.process.ucTopic + ".ep[" + this.process.epState.ets + "]")
                                    .setType(Message.Type.EP_INTERNAL_ACCEPT)
                                    .build();

                            this.plSendParams(epInternalAccept, this.process.ucTopic + ".ep[" + this.process.epState.ets + "]",
                                    this.process.ucTopic + ".ep[" + this.process.epState.ets + "].pl", bebDeliver.getSender());
                            return true;
                        }
                    case EP_INTERNAL_DECIDED:
                        if (timestamp == process.epState.ets) {
                            System.out.println(this.process.debugName + " internal decide - EP");
                            this.process.messages.add(Message.newBuilder().setType(Message.Type.EP_DECIDE)
                                    .setToAbstractionId(this.process.ucTopic)
                                    .setEpDecide(EpDecide.newBuilder().setEts(this.process.epState.ets)
                                            .setValue(Value.newBuilder()
                                                    .setDefined(true)
                                                    .setV(bebDeliver.getMessage().getEpInternalDecided().getValue().getV())
                                                    .build()).build())
                                    .build());
                            return true;
                        }
                }
                return false;

            case PL_DELIVER:
                PlDeliver plDeliver = message.getPlDeliver();

                if (plDeliver.getMessage().getToAbstractionId().contains("ep") && Character.isDigit((plDeliver.getMessage().getToAbstractionId().charAt(2)))) {
                    timestamp = Integer.parseInt(plDeliver.getMessage().getToAbstractionId().substring(2));
                }
                switch (plDeliver.getMessage().getType()) {
                    case EP_INTERNAL_STATE:
                        if (timestamp == this.process.epState.ets) {
                            if (ProcUtil.getCurrentProcess(this.process).equals(this.process.leader)) {
                                EpInternalState epInternalState = EpInternalState.newBuilder()
                                        .setValueTimestamp(plDeliver.getMessage().getEpInternalState().getValueTimestamp())
                                        .setValue(Value.newBuilder()
                                                .setDefined(true)
                                                .setV(plDeliver.getMessage().getEpInternalState().getValue().getV())
                                                .build())
                                        .build();
                                this.process.epState.states.put(plDeliver.getSender().getPort(), epInternalState);
                                this.checkStates();
                            }
                            return true;
                        }
                    case EP_INTERNAL_ACCEPT:
                        if (timestamp == this.process.epState.ets) {
                            if (ProcUtil.getCurrentProcess(this.process).equals(this.process.leader)) {
                                this.process.epState.accepted++;
                                if (this.process.epState.accepted > Math.ceil(this.process.processes.size() / 2)) {
                                    this.process.epState.accepted = 0;
                                    Message epInternalDecided = Message.newBuilder()
                                            .setToAbstractionId(this.process.ucTopic + ".ep[" + this.process.epState.ets + "]")
                                            .setType(Message.Type.EP_INTERNAL_DECIDED)
                                            .setEpInternalDecided(EpInternalDecided.newBuilder()
                                                    .setValue(Value.newBuilder()
                                                            .setDefined(true)
                                                            .setV(this.process.epState.tmpval.getV())
                                                            .build()))
                                            .build();
                                    this.bebBroadcastParams(epInternalDecided, this.process.ucTopic + ".ep[" + this.process.epState.ets + "]", this.process.ucTopic + ".ec.beb");
                                }
                            }
                            return true;
                        }
                }
                return false;

            case EP_ABORT:
                if (timestamp == this.process.epState.ets) {
                    System.out.println("EP abort: EP\n");
                    this.process.messages.add(Message.newBuilder().setType(Message.Type.EP_ABORTED)
                            .setToAbstractionId(this.process.ucTopic)
                            .setEpAborted(EpAborted.newBuilder().setEts(this.process.epState.ets)
                                    .setValue(Value.newBuilder().setDefined(true).setV(this.process.epState.st.getValue().getV()).build())
                                    .setValueTimestamp(this.process.epState.st.getValueTimestamp()).build())
                            .build());
                    this.process.epState.halted = true;
                    return true;
                }
        }
        return false;
    }

    public void checkStates() {
        if (this.process.epState.states.size() > Math.ceil(this.process.processes.size() / 2)) {
            EpInternalState st = getHighest();
            if(st.getValue().getDefined()) {
                this.process.epState.tmpval = Value.newBuilder().setDefined(true).setV(st.getValue().getV()).build();
            }
            this.process.epState.states = new HashMap<>();
            Message epInternalWrite = Message.newBuilder()
                    .setToAbstractionId(this.process.ucTopic + ".ep[" + this.process.epState.ets+"]")
                    .setType(Message.Type.EP_INTERNAL_WRITE)
                    .setEpInternalWrite(EpInternalWrite.newBuilder()
                            .setValue(Value.newBuilder()
                                    .setDefined(true)
                                    .setV(process.epState.tmpval.getV())
                                    .build()))
                        .build();
            this.bebBroadcastParams(epInternalWrite, this.process.ucTopic + ".ep[" + this.process.epState.ets + "]", this.process.ucTopic + ".ec.beb");
        }
    }

    public EpInternalState getHighest() {
        EpInternalState epInternalState = EpInternalState.newBuilder().setValueTimestamp(0).setValue(Value.newBuilder().setDefined(false).build()).build();
        for (Integer port : this.process.epState.states.keySet()) {
            if (this.process.epState.states.get(port).getValueTimestamp() > epInternalState.getValueTimestamp()) {
                epInternalState = this.process.epState.states.get(port);
            }
        }
        return epInternalState;
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
