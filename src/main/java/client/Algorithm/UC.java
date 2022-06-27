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
    public void init(Proc p) {
        process = p;
        process.ucState = new UCState();
        process.leader = ProcUtil.maxRank(process.processes);

        EpInternalState state = EpInternalState.newBuilder().setValueTimestamp(0).setValue(Value.newBuilder().setDefined(false).build()).build();

        process.abstractionInterfaceMap.put(process.ucTopic+".ep[0]", new EP(process, state, 0, process.leader));
        process.abstractionInterfaceMap.put(process.ucTopic+".ep[0]"+".pl", new PL());
        process.abstractionInterfaceMap.get(process.ucTopic+".ep[0]"+".pl").init(process);

    }

    public void proposeValue() {
        System.out.println("aici in pula mea11111111111111111111111111111");
        if(process.leader.equals(ProcUtil.getCurrentProcess(process)) && process.ucState.val.getDefined() && !process.ucState.proposed) {
            process.ucState.proposed = true;
            process.messages.add(Message.newBuilder().setType(Message.Type.EP_PROPOSE)
                    .setSystemId("sys-1")
                    .setToAbstractionId(process.ucTopic+".ep["+ process.ucState.ets +"]")
                    .setEpPropose(EpPropose.newBuilder().setValue(Value.newBuilder().setDefined(true).setV(process.ucState.val.getV()).build()).build())
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
                process.ucState.val = Value.newBuilder().setDefined(true).setV(message.getUcPropose().getValue().getV()).build();
                proposeValue();
                return true;
            case EC_START_EPOCH:
                System.out.println("Start Epoch: UC\n");

                process.ucState.newts = message.getEcStartEpoch().getNewTimestamp();
                process.ucState.newl = message.getEcStartEpoch().getNewLeader();
                process.messages.add(Message.newBuilder().setType(Message.Type.EP_ABORT)
                        .setSystemId("sys-1")
                        .setToAbstractionId(process.ucTopic+".ep[" + process.ucState.ets+"]")
                        .setEpAbort(EpAbort.newBuilder().build())
                        .build());
                return true;
            case EP_ABORTED:
                System.out.println("EP Aborted: UC\n");
                if(message.getEpAborted().getEts() == process.ucState.ets) {
                    process.ucState.ets = process.ucState.newts;
                    process.leader = process.ucState.newl;
                    process.ucState.proposed = false;

                    EpInternalState state = EpInternalState.newBuilder().setValueTimestamp(message.getEpAborted().getValueTimestamp()).
                            setValue(Value.newBuilder().setDefined(true).setV(message.getEpAborted().getValue().getV()).build()).build();
                    process.abstractionInterfaceMap.put(process.ucTopic+".ep["+ process.ucState.ets +"]", new EP(process, state, process.ucState.ets, process.leader));
                    process.abstractionInterfaceMap.put(process.ucTopic+".ep["+ process.ucState.ets +"]", new PL());
                    process.abstractionInterfaceMap.get(process.ucTopic+".ep["+ process.ucState.ets +"]").init(process);

//                    leader == self && ucState.proposed == false
                    if(process.ucState.val!=null)
                        proposeValue();
                    return true;
                }
            case EP_DECIDE:
                System.out.println("EP Decide: UC");
                if(message.getEpDecide().getEts() == process.ucState.ets){

                    if(!process.ucState.decided) {
                        process.ucState.decided = true;
                        process.messages.add(Message.newBuilder().setType(Message.Type.UC_DECIDE)
                                .setToAbstractionId(process.ucTopic)
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

                sendToHub(msg, process.ucTopic, "app", process.port);
                return true;
        }

        return false;
    }
}
