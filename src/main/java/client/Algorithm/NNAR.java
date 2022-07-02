package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public class NNAR implements AbstractionInterface {

    private Proc process;

    @Override
    public void init(Proc process) {

        this.process = process;
        this.process.sharedMemory.rid = 0;
        this.process.sharedMemory.ts = 0;
        this.process.sharedMemory.value = -1;
        this.process.sharedMemory.acks = 0;
        this.process.sharedMemory.writeVal = 0;
        this.process.sharedMemory.readVal = 0;
        this.process.sharedMemory.readList = new NnarInternalValue[6];
        this.process.sharedMemory.reading = false;
    }

    @Override
    public boolean handle(Message message) {

        switch (message.getType()) {
            case NNAR_WRITE:
                this.process.sharedMemory.rid = this.process.sharedMemory.rid + 1;
                this.process.sharedMemory.writeVal = message.getNnarWrite().getValue().getV();

                this.process.sharedMemory.acks = 0;
                this.process.sharedMemory.readList = new NnarInternalValue[6];

                Message nnarWrite = Message.newBuilder()
                        .setType(Message.Type.NNAR_INTERNAL_READ)
                        .setFromAbstractionId(this.process.sharedMemory.appNnar)
                        .setToAbstractionId(this.process.sharedMemory.appNnar)
                        .setSystemId("sys-1")
                        .setNnarInternalRead(NnarInternalRead.newBuilder()
                                .setReadId(this.process.sharedMemory.rid)
                                .build())
                        .build();
                this.bebBroadcastParams(nnarWrite, this.process.sharedMemory.appNnar, this.process.sharedMemory.appNnar + ".beb");
                return true;

            case NNAR_READ:

                this.process.sharedMemory.rid = this.process.sharedMemory.rid + 1;
                this.process.sharedMemory.writeVal = message.getNnarWrite().getValue().getV();
                this.process.sharedMemory.acks = 0;
                this.process.sharedMemory.readList = new NnarInternalValue[6];
                this.process.sharedMemory.reading = true;
                Message nnarRead = Message.newBuilder()
                        .setType(Message.Type.NNAR_INTERNAL_READ)
                        .setFromAbstractionId(this.process.sharedMemory.appNnar)
                        .setToAbstractionId(this.process.sharedMemory.appNnar)
                        .setSystemId("sys-1")
                        .setNnarInternalRead(NnarInternalRead.newBuilder()
                                .setReadId(this.process.sharedMemory.rid)
                                .build())
                        .build();
                this.bebBroadcastParams(nnarRead, this.process.sharedMemory.appNnar, this.process.sharedMemory.appNnar + ".beb");
                return true;
            case BEB_DELIVER:
                if (message.getBebDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    Message nnarInternalValue = Message.newBuilder()
                            .setType(Message.Type.NNAR_INTERNAL_VALUE)
                            .setFromAbstractionId(this.process.sharedMemory.appNnar)
                            .setToAbstractionId(this.process.sharedMemory.appNnar)
                            .setNnarInternalValue(NnarInternalValue.newBuilder()
                                    .setTimestamp(this.process.sharedMemory.ts)
                                    .setReadId(message.getBebDeliver().getMessage().getNnarInternalRead().getReadId())
                                    .setValue(Value.newBuilder()
                                            .setDefined(true)
                                            .setV(this.process.sharedMemory.value)
                                            .build())
                                    .setWriterRank(this.process.sharedMemory.writerRank)
                                    .build())
                            .build();
                    this.plSendParams(nnarInternalValue, message.getToAbstractionId(), message.getToAbstractionId() + ".pl", message.getBebDeliver().getSender());

                    return true;
                }

            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)) {
                    if (message.getPlDeliver().getMessage().getNnarInternalValue().getReadId() == this.process.sharedMemory.rid) {
                        int processIndex = message.getPlDeliver().getSender().getPort() - 5001;

                        this.process.sharedMemory.readList[processIndex] = message.getPlDeliver().getMessage().getNnarInternalValue();
                        if (readListOccupied() > 3) {
                            NnarInternalValue maxTimestamp = this.maxTimestamp();
                            this.process.sharedMemory.readVal = maxTimestamp.getValue().getV();
                            this.process.sharedMemory.readList = new NnarInternalValue[6];
                            Message nnarInternalWrite = null;
                            if (this.process.sharedMemory.reading) {
                                nnarInternalWrite = Message.newBuilder()
                                        .setType(Message.Type.NNAR_INTERNAL_WRITE)
                                        .setToAbstractionId(this.process.sharedMemory.appNnar)
                                        .setFromAbstractionId(this.process.sharedMemory.appNnar)
                                        .setNnarInternalWrite(NnarInternalWrite.newBuilder()
                                                .setTimestamp(maxTimestamp.getTimestamp())
                                                .setValue(Value.newBuilder()
                                                        .setV(this.process.sharedMemory.readVal)
                                                        .setDefined(true)
                                                        .build())
                                                .setWriterRank(maxTimestamp.getWriterRank())
                                                .setReadId(this.process.sharedMemory.rid)
                                                .build())
                                        .build();
                            } else {
                                nnarInternalWrite = Message.newBuilder()
                                        .setType(Message.Type.NNAR_INTERNAL_WRITE)
                                        .setToAbstractionId(this.process.sharedMemory.appNnar)
                                        .setFromAbstractionId(this.process.sharedMemory.appNnar)
                                        .setNnarInternalWrite(NnarInternalWrite.newBuilder()
                                                .setTimestamp(maxTimestamp.getTimestamp() + 1)
                                                .setValue(Value.newBuilder()
                                                        .setDefined(true)
                                                        .setV(this.process.sharedMemory.writeVal).build())
                                                .setWriterRank(this.process.rank)
                                                .setReadId(this.process.sharedMemory.rid)
                                                .build())
                                        .build();
                            }

                            this.bebBroadcastParams(nnarInternalWrite, this.process.sharedMemory.appNnar, this.process.sharedMemory.appNnar + ".beb");
                        }
                    }
                    return true;
                }

                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)) {

                    if (message.getPlDeliver().getMessage().getNnarInternalAck().getReadId() == this.process.sharedMemory.rid) {
                        this.process.sharedMemory.acks += 1;

                        if (this.process.sharedMemory.acks > 3) {
                            this.process.sharedMemory.acks = 0;
                            if (this.process.sharedMemory.reading) {
                                this.process.sharedMemory.reading = false;
                                Message appReadReturn = Message.newBuilder()
                                        .setType(Message.Type.APP_READ_RETURN)
                                        .setToAbstractionId("hub")
                                        .setFromAbstractionId("app")
                                        .setAppReadReturn(AppReadReturn.newBuilder()
                                                .setRegister(this.process.sharedMemory.register)
                                                .setValue(Value.newBuilder()
                                                        .setDefined(true)
                                                        .setV(this.process.sharedMemory.readVal)
                                                        .build())
                                                .build())
                                        .build();
                                this.plDeliverParams(appReadReturn, message.getToAbstractionId(), message.getToAbstractionId() + ".pl", message.getPlDeliver().getSender());
                            } else {
                                Message appWriteReturn = Message.newBuilder()
                                        .setType(Message.Type.APP_WRITE_RETURN)
                                        .setToAbstractionId("hub")
                                        .setFromAbstractionId("app")
                                        .setAppWriteReturn(AppWriteReturn.newBuilder()
                                                .setRegister(this.process.sharedMemory.register)
                                                .build())
                                        .build();
                                this.plDeliverParams(appWriteReturn, message.getToAbstractionId(), message.getToAbstractionId() + ".pl", message.getPlDeliver().getSender());
                            }
                        }
                    }
                    return true;
                }
        }
        return false;
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

    private void plDeliverParams(Message message, String from, String to, ProcessId pid) {
        this.process.messages.add(Message.newBuilder()
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
        for (int i = 0; i < this.process.sharedMemory.readList.length; i++) {
            if (this.process.sharedMemory.readList[i] != null) {
                c++;
            }
        }
        return c;
    }

    private NnarInternalValue maxTimestamp() {
        int maxTimestampOccurrence = 0;
        NnarInternalValue maxTimestamp = this.process.sharedMemory.readList[0];
        for (int i = 1; i < this.process.sharedMemory.readList.length; i++) {
            if (maxTimestamp != null && this.process.sharedMemory.readList[i] != null &&
                    (this.process.sharedMemory.readList[i].getTimestamp() > maxTimestamp.getTimestamp()) &&
                    (this.process.sharedMemory.readList[i].getValue().getV() != -1)) {

                maxTimestamp = this.process.sharedMemory.readList[i];
                maxTimestampOccurrence++;
            }
        }
        if (maxTimestampOccurrence > 1) {
            for (int i = 1; i < this.process.sharedMemory.readList.length; i++) {
                if (this.process.sharedMemory.readList[i] == maxTimestamp && this.process.sharedMemory.readList[i].getWriterRank() > maxTimestamp.getWriterRank()
                        && this.process.sharedMemory.readList[i].getValue().getV() != -1
                ) {
                    maxTimestamp = this.process.sharedMemory.readList[i];
                }
            }
        }
        return maxTimestamp;
    }
}