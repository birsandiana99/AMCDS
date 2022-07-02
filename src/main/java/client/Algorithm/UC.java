package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;
import main.CommunicationProtocol.Message;
import main.CommunicationProtocol.Value;
import client.Utilities.MessageUtils;
import client.Utilities.ProcUtil;
import client.Utilities.UCState;

import java.io.IOException;
import java.net.Socket;

public class UC implements AbstractionInterface{

    private Proc process;

    @Override
    public void init(Proc process) {
        this.process = process;
        this.process.ucState = new UCState();
        this.process.leader = ProcUtil.maxRank(this.process.processes);

        EpInternalState state = EpInternalState.newBuilder()
                .setValueTimestamp(0).setValue(
                        Value.newBuilder().setDefined(false).build())
                .build();

        this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ep[0]", new EP(this.process, state, 0, this.process.leader));
        this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ep[0]" + ".pl", new PL());
        this.process.abstractionInterfaceMap.get(this.process.ucTopic + ".ep[0]" + ".pl").init(this.process);

    }

    public void proposeValue() {
        if(this.process.leader.equals(ProcUtil.getCurrentProcess(this.process)) && this.process.ucState.val.getDefined() && !this.process.ucState.proposed) {
            this.process.ucState.proposed = true;
            this.process.messages.add(Message.newBuilder().setType(Message.Type.EP_PROPOSE)
                    .setSystemId("sys-1")
                    .setToAbstractionId(this.process.ucTopic + ".ep[" + this.process.ucState.ets + "]")
                    .setEpPropose(EpPropose.newBuilder()
                            .setValue(Value.newBuilder()
                                    .setDefined(true)
                                    .setV(this.process.ucState.val.getV())
                                    .build())
                            .build())
                    .build());
        }
    }

    private void sendToHub(Message message, String from, String to, int port) {
        Socket socket = null;
        try {
            socket = new Socket(Proc.ADDR_HUB, Proc.PORT_HUB);
            Message nm = Message.newBuilder()
                    .setType(Message.Type.NETWORK_MESSAGE)
                    .setMessageUuid(message.getMessageUuid())
                    .setFromAbstractionId(from)
                    .setToAbstractionId(to)
                    .setSystemId("sys-1")
                    .setNetworkMessage(NetworkMessage.newBuilder()
                            .setSenderHost(Proc.ADDR_HUB)
                            .setSenderListeningPort(port)
                            .setMessage(message)
                    ).build();
            MessageUtils.write(socket.getOutputStream(), nm);
            socket.close();
        } catch (IOException e) {
            System.out.println("Socket error - cannot send");
        }
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case UC_PROPOSE:
                this.process.ucState.val = Value.newBuilder().setDefined(true).setV(message.getUcPropose().getValue().getV()).build();
                proposeValue();
                return true;
            case EC_START_EPOCH:
                System.out.println("Start Epoch: UC\n");

                this.process.ucState.newts = message.getEcStartEpoch().getNewTimestamp();
                this.process.ucState.newl = message.getEcStartEpoch().getNewLeader();
                this.process.messages.add(Message.newBuilder().setType(Message.Type.EP_ABORT)
                        .setSystemId("sys-1")
                        .setToAbstractionId(this.process.ucTopic + ".ep[" + this.process.ucState.ets + "]")
                        .setEpAbort(EpAbort.newBuilder().build())
                        .build());
                return true;
            case EP_ABORTED:
                System.out.println("EP Aborted: UC\n");
                if (message.getEpAborted().getEts() == this.process.ucState.ets) {
                    this.process.ucState.ets = process.ucState.newts;
                    this.process.leader = process.ucState.newl;
                    this.process.ucState.proposed = false;

                    EpInternalState state = EpInternalState.newBuilder().setValueTimestamp(message.getEpAborted().getValueTimestamp()).
                            setValue(Value.newBuilder().setDefined(true).setV(message.getEpAborted().getValue().getV()).build()).build();
                    this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ep[" + this.process.ucState.ets + "]",
                            new EP(this.process, state, this.process.ucState.ets, this.process.leader));

                    this.process.abstractionInterfaceMap.put(this.process.ucTopic + ".ep[" + this.process.ucState.ets + "]", new PL());
                    this.process.abstractionInterfaceMap.get(this.process.ucTopic + ".ep[" + this.process.ucState.ets + "]").init(this.process);

//                    leader == self && ucState.proposed == false
                    if (this.process.ucState.val != null) {
                        proposeValue();
                    }
                    return true;
                }
            case EP_DECIDE:
                System.out.println("EP Decide: UC");
                if (message.getEpDecide().getEts() == this.process.ucState.ets) {

                    if (!this.process.ucState.decided) {
                        this.process.ucState.decided = true;
                        this.process.messages.add(Message.newBuilder().setType(Message.Type.UC_DECIDE)
                                .setToAbstractionId(this.process.ucTopic)
                                .setSystemId("sys-1")
                                .setUcDecide(UcDecide.newBuilder()
                                        .setValue(Value.newBuilder()
                                                .setDefined(true)
                                                .setV(message.getEpDecide().getValue().getV())
                                                .build())
                                        .build())
                                .build());
                    }
                    return true;
                }
            case UC_DECIDE:
                System.out.println("UC Decide: UC");
                Message msg = Message.newBuilder()
                        .setToAbstractionId("app")
                        .setSystemId("sys-1")
                        .setType(Message.Type.APP_DECIDE)
                        .setAppDecide(AppDecide.newBuilder()
                                .setValue(Value.newBuilder().setDefined(true).setV(message.getUcDecide().getValue().getV()).build())
                                .build())
                        .build();

                sendToHub(msg, this.process.ucTopic, "app", this.process.port);
                return true;
        }

        return false;
    }
}
