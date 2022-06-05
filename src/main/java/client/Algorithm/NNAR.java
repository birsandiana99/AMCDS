package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public class NNAR implements AbstractionInterface {

    private Proc process;

    @Override
    public void init(Proc p) {

        process = p;
        process.sharedMemory.rid = 0;
        process.sharedMemory.ts = 0;
        process.sharedMemory.value = -1;
        process.sharedMemory.acks = 0;
        process.sharedMemory.writeVal = 0;
        process.sharedMemory.readVal = 0;
        process.sharedMemory.readList = new NnarInternalValue[6];
        process.sharedMemory.reading = false;
    }

    @Override
    public boolean handle(Message message) {

        switch (message.getType()) {
            case NNAR_WRITE:
                process.sharedMemory.rid = process.sharedMemory.rid + 1;
                process.sharedMemory.writeVal = message.getNnarWrite().getValue().getV();

                process.sharedMemory.acks = 0;
                process.sharedMemory.readList = new NnarInternalValue[6];

                Message msgWrite = Message.newBuilder()
                        .setType(Message.Type.NNAR_INTERNAL_READ)
                        .setFromAbstractionId(process.sharedMemory.appNnar)
                        .setToAbstractionId(process.sharedMemory.appNnar)
                        .setSystemId("sys-1")
                        .setNnarInternalRead(NnarInternalRead.newBuilder()
                                .setReadId(process.sharedMemory.rid)
                                .build())
                        .build();
                bebBroadcastParams(msgWrite, process.sharedMemory.appNnar, process.sharedMemory.appNnar + ".beb");
                return true;

            case NNAR_READ:

                process.sharedMemory.rid = process.sharedMemory.rid + 1;
                process.sharedMemory.writeVal = message.getNnarWrite().getValue().getV();
                process.sharedMemory.acks = 0;
                process.sharedMemory.readList = new NnarInternalValue[6];
                process.sharedMemory.reading = true;
                Message msgRead = Message.newBuilder()
                        .setType(Message.Type.NNAR_INTERNAL_READ)
                        .setFromAbstractionId(process.sharedMemory.appNnar)
                        .setToAbstractionId(process.sharedMemory.appNnar)
                        .setSystemId("sys-1")
                        .setNnarInternalRead(NnarInternalRead.newBuilder()
                                .setReadId(process.sharedMemory.rid)
                                .build())
                        .build();
                bebBroadcastParams(msgRead, process.sharedMemory.appNnar, process.sharedMemory.appNnar + ".beb");
                return true;
            case BEB_DELIVER:
                if (message.getBebDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    Message msg = Message.newBuilder()
                            .setType(Message.Type.NNAR_INTERNAL_VALUE)
                            .setFromAbstractionId(process.sharedMemory.appNnar)
                            .setToAbstractionId(process.sharedMemory.appNnar)
                            .setNnarInternalValue(NnarInternalValue.newBuilder()
                                    .setTimestamp(process.sharedMemory.ts)
                                    .setReadId(message.getBebDeliver().getMessage().getNnarInternalRead().getReadId())
                                    .setValue(Value.newBuilder()
                                            .setDefined(true)
                                            .setV(process.sharedMemory.value)
                                            .build())
                                    .setWriterRank(process.sharedMemory.writerRank)
                                    .build())
                            .build();
                    plSendParams(msg, message.getToAbstractionId(), message.getToAbstractionId() + ".pl", message.getBebDeliver().getSender());

                    return true;
                }

            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)) {
                    if (message.getPlDeliver().getMessage().getNnarInternalValue().getReadId() == process.sharedMemory.rid) {
                        int processIndex = message.getPlDeliver().getSender().getPort() - 5001;

                        process.sharedMemory.readList[processIndex] = message.getPlDeliver().getMessage().getNnarInternalValue();
                        if (readListOccupied() > 3) {
                            NnarInternalValue maxTs = maxTimestamp();
                            process.sharedMemory.readVal = maxTs.getValue().getV();
                            process.sharedMemory.readList = new NnarInternalValue[6];
                            Message msgWriteInternal = null;
                            if (process.sharedMemory.reading) {
                                msgWriteInternal = Message.newBuilder()
                                        .setType(Message.Type.NNAR_INTERNAL_WRITE)
                                        .setToAbstractionId(process.sharedMemory.appNnar)
                                        .setFromAbstractionId(process.sharedMemory.appNnar)
                                        .setNnarInternalWrite(NnarInternalWrite.newBuilder()
                                                .setTimestamp(maxTs.getTimestamp())
                                                .setValue(Value.newBuilder()
                                                        .setV(process.sharedMemory.readVal)
                                                        .setDefined(true)
                                                        .build())
                                                .setWriterRank(maxTs.getWriterRank())
                                                .setReadId(process.sharedMemory.rid)
                                                .build())
                                        .build();
                            } else {
                                msgWriteInternal = Message.newBuilder()
                                        .setType(Message.Type.NNAR_INTERNAL_WRITE)
                                        .setToAbstractionId(process.sharedMemory.appNnar)
                                        .setFromAbstractionId(process.sharedMemory.appNnar)
                                        .setNnarInternalWrite(NnarInternalWrite.newBuilder()
                                                .setTimestamp(maxTs.getTimestamp() + 1)
                                                .setValue(Value.newBuilder()
                                                        .setDefined(true)
                                                        .setV(process.sharedMemory.writeVal).build())
                                                .setWriterRank(process.rank)
                                                .setReadId(process.sharedMemory.rid)
                                                .build())
                                        .build();
                            }

                            bebBroadcastParams(msgWriteInternal, process.sharedMemory.appNnar, process.sharedMemory.appNnar + ".beb");
                        }
                    }
                    return true;
                }

                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)) {

                    if (message.getPlDeliver().getMessage().getNnarInternalAck().getReadId() == process.sharedMemory.rid) {
                        process.sharedMemory.acks += 1;

                        if (process.sharedMemory.acks > 3) {
                            process.sharedMemory.acks = 0;
                            if (process.sharedMemory.reading) {
                                process.sharedMemory.reading = false;
                                Message msgReadReturn = Message.newBuilder()
                                        .setType(Message.Type.APP_READ_RETURN)
                                        .setToAbstractionId("hub")
                                        .setFromAbstractionId("app")
                                        .setAppReadReturn(AppReadReturn.newBuilder()
                                                .setRegister(process.sharedMemory.register)
                                                .setValue(Value.newBuilder()
                                                        .setDefined(true)
                                                        .setV(process.sharedMemory.readVal)
                                                        .build())
                                                .build())
                                        .build();
                                plDeliverParams(msgReadReturn, message.getToAbstractionId(), message.getToAbstractionId() + ".pl", message.getPlDeliver().getSender());
                            } else {
                                Message msgWriteReturn = Message.newBuilder()
                                        .setType(Message.Type.APP_WRITE_RETURN)
                                        .setToAbstractionId("hub")
                                        .setFromAbstractionId("app")
                                        .setAppWriteReturn(AppWriteReturn.newBuilder()
                                                .setRegister(process.sharedMemory.register)
                                                .build())
                                        .build();
                                plDeliverParams(msgWriteReturn, message.getToAbstractionId(), message.getToAbstractionId() + ".pl", message.getPlDeliver().getSender());
                            }
                        }
                    }
                    return true;
                }
        }
        return false;
    }

    private void bebBroadcastParams(Message message, String from, String to) {
        Message msg = Message.newBuilder()
                .setType(Message.Type.BEB_BROADCAST)
                .setBebBroadcast(BebBroadcast.newBuilder()
                        .setMessage(message)
                        .build())
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .build();
        process.messages.add(msg);
    }

    public void plSendParams(Message message, String from, String to, ProcessId pid) {
        process.messages.add(Message.newBuilder()
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

    private void plDeliverParams(Message message, String from, String to, ProcessId pid) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_DELIVER)
                .setPlDeliver(PlDeliver.newBuilder()
                        .setSender(pid)
                        .setMessage(message).build())
                .setFromAbstractionId(from)
                .setToAbstractionId(to)
                .setSystemId("sys-1")
                .build());
    }

    private int readListOccupied() {
        int c = 0;
        for (int i = 0; i < process.sharedMemory.readList.length; i++) {
            if (process.sharedMemory.readList[i] != null) {
                c++;
            }
        }
        return c;
    }

    private NnarInternalValue maxTimestamp() {
        int maxTsOccurrence = 0;
        NnarInternalValue maxTs = process.sharedMemory.readList[0];
        for (int i = 1; i < process.sharedMemory.readList.length; i++) {
            if (maxTs != null && process.sharedMemory.readList[i] != null &&
                    (process.sharedMemory.readList[i].getTimestamp() > maxTs.getTimestamp()) &&
                    (process.sharedMemory.readList[i].getValue().getV() != -1)) {

                maxTs = process.sharedMemory.readList[i];
                maxTsOccurrence++;
            }
        }
        if (maxTsOccurrence > 1) {
            for (int i = 1; i < process.sharedMemory.readList.length; i++) {
                if (process.sharedMemory.readList[i] == maxTs && process.sharedMemory.readList[i].getWriterRank() > maxTs.getWriterRank()
                        && process.sharedMemory.readList[i].getValue().getV() != -1
                ) {
                    maxTs = process.sharedMemory.readList[i];
                }
            }
        }
        return maxTs;
    }
}