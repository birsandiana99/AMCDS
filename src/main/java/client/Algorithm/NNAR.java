package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public class NNAR implements AbstractionInterface {

    private Proc process;

    @Override
    public void init(Proc p) {

        process = p;
        process.rid = 0;
        process.ts = 0;
        process.value = -1;
        process.acks = 0;
        process.writeVal = 0;
        process.readVal = 0;
        process.readList = new NnarInternalValue[6];
        process.reading = false;
    }

    @Override
    public boolean handle(Message message) {

        switch (message.getType()) {
            case NNAR_WRITE:
                process.rid = process.rid + 1;
                process.writeVal = message.getNnarWrite().getValue().getV();

                process.acks = 0;
                process.readList = new NnarInternalValue[6];

                Message intRead = Message.newBuilder()
                        .setType(Message.Type.NNAR_INTERNAL_READ)
                        .setFromAbstractionId("app.nnar[" + process.register + "]")
                        .setToAbstractionId("app.nnar[" + process.register + "]")
                        .setSystemId("sys-1")
                        .setNnarInternalRead(NnarInternalRead.newBuilder()
                                .setReadId(process.rid)
                                .build())
                        .build();
                Message msg = Message.newBuilder()
                        .setFromAbstractionId("app.nnar[" + process.register + "]")
                        .setToAbstractionId("app.nnar[" + process.register + "].beb")
                        .setType(Message.Type.BEB_BROADCAST)
                        .setBebBroadcast(BebBroadcast.newBuilder()
                                .setMessage(intRead)
                                .build())
                        .build();
                process.messages.add(msg);
                return true;

            case NNAR_READ:

                process.rid = process.rid + 1;
                process.writeVal = message.getNnarWrite().getValue().getV();
                process.acks = 0;
                process.readList = new NnarInternalValue[6];
                process.reading = true;
                Message intRead2 = Message.newBuilder()
                        .setType(Message.Type.NNAR_INTERNAL_READ)
                        .setFromAbstractionId("app.nnar[" + process.register + "]")
                        .setToAbstractionId("app.nnar[" + process.register + "]")
                        .setSystemId("sys-1")
                        .setNnarInternalRead(NnarInternalRead.newBuilder()
                                .setReadId(process.rid)
                                .build())
                        .build();
                Message msg2 = Message.newBuilder()
                        .setFromAbstractionId("app.nnar[" + process.register + "]")
                        .setToAbstractionId("app.nnar[" + process.register + "].beb")
                        .setType(Message.Type.BEB_BROADCAST)
                        .setBebBroadcast(BebBroadcast.newBuilder()
                                .setMessage(intRead2)
                                .build())
                        .build();
                process.messages.add(msg2);
                return true;
            case BEB_DELIVER:
                if (message.getBebDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_READ)) {
                    Message msg1 = Message.newBuilder()
                            .setType(Message.Type.PL_SEND)
                            .setFromAbstractionId(message.getToAbstractionId())
                            .setToAbstractionId(message.getToAbstractionId() + ".pl")
                            .setSystemId("sys-1")
                            .setPlSend(PlSend.newBuilder()
                                    .setMessage(Message.newBuilder()
                                            .setType(Message.Type.NNAR_INTERNAL_VALUE)
                                            .setFromAbstractionId("app.nnar[" + process.register + "]")
                                            .setToAbstractionId("app.nnar[" + process.register + "]")
                                            .setNnarInternalValue(NnarInternalValue.newBuilder()
                                                    .setTimestamp(process.ts)
                                                    .setReadId(message.getBebDeliver().getMessage().getNnarInternalRead().getReadId())
                                                    .setValue(Value.newBuilder()
                                                            .setDefined(true)
                                                            .setV(process.value)
                                                            .build())
                                                    .setWriterRank(process.writerRank)
                                                    .build())
                                            .build())
                                    .setDestination(message.getBebDeliver().getSender())
                                    .build())
                            .build();
                    process.messages.add(msg1);

                    return true;
                }

            case PL_DELIVER:
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_VALUE)) {
                    //ultima parte din read
                    if (message.getPlDeliver().getMessage().getNnarInternalValue().getReadId() == process.rid) {
                        int processIndex = message.getPlDeliver().getSender().getPort() - 5001;

                        process.readList[processIndex] = message.getPlDeliver().getMessage().getNnarInternalValue();
                        if (readListOccupied() > 3) {
                            NnarInternalValue maxts = maxTimestamp();
                            process.readVal = maxts.getValue().getV();
                            process.readList = new NnarInternalValue[6];
                            Message msgNm;
                            if (process.reading) {
                                msgNm = Message.newBuilder()
                                        .setType(Message.Type.NNAR_INTERNAL_WRITE)
                                        .setToAbstractionId("app.nnar[" + process.register + "]")
                                        .setFromAbstractionId("app.nnar[" + process.register + "]")
                                        .setNnarInternalWrite(NnarInternalWrite.newBuilder()
                                                .setTimestamp(maxts.getTimestamp())
                                                .setValue(Value.newBuilder()
                                                        .setV(process.readVal)
                                                        .setDefined(true)
                                                        .build())
                                                .setWriterRank(maxts.getWriterRank())
                                                .setReadId(process.rid)
                                                .build())
                                        .build();
                            } else {
                                msgNm = Message.newBuilder()
                                        .setType(Message.Type.NNAR_INTERNAL_WRITE)
                                        .setToAbstractionId("app.nnar[" + process.register + "]")
                                        .setFromAbstractionId("app.nnar[" + process.register + "]")
                                        .setNnarInternalWrite(NnarInternalWrite.newBuilder()
                                                .setTimestamp(maxts.getTimestamp() + 1)
                                                .setValue(Value.newBuilder()
                                                        .setDefined(true)
                                                        .setV(process.writeVal).build())
                                                .setWriterRank(process.rank)
                                                .setReadId(process.rid)
                                                .build())
                                        .build();
                            }

                            Message msg1 = Message.newBuilder()
                                    .setType(Message.Type.BEB_BROADCAST)
                                    .setFromAbstractionId("app.nnar[" + process.register + "]")
                                    .setToAbstractionId("app.nnar[" + process.register + "].beb")
                                    .setSystemId("sys-1")
                                    .setBebBroadcast(BebBroadcast.newBuilder()
                                            .setMessage(msgNm)
                                            .build())
                                    .build();
                            process.messages.add(msg1);
                        }
                    }
                    return true;
                }

                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.NNAR_INTERNAL_ACK)) {

                    if (message.getPlDeliver().getMessage().getNnarInternalAck().getReadId() == process.rid) {
                        process.acks += 1;

                        if (process.acks > 3) {
                            process.acks = 0;
                            if (process.reading) {
                                process.reading = false;
                                Message msgRead = Message.newBuilder()
                                        .setType(Message.Type.PL_DELIVER)
                                        .setFromAbstractionId(message.getToAbstractionId())
                                        .setToAbstractionId(message.getToAbstractionId() + ".pl")
                                        .setSystemId("sys-1")
                                        .setPlDeliver(PlDeliver.newBuilder()
                                                .setSender(message.getPlDeliver().getSender())
                                                .setMessage(Message.newBuilder()
                                                        .setType(Message.Type.APP_READ_RETURN)
                                                        .setToAbstractionId("hub")
                                                        .setFromAbstractionId("app")
                                                        .setAppReadReturn(AppReadReturn.newBuilder()
                                                                .setRegister(process.register)
                                                                .setValue(Value.newBuilder()
                                                                        .setDefined(true)
                                                                        .setV(process.readVal)
                                                                        .build())
                                                                .build())
                                                        .build())
                                                .build())
                                        .build();
                                process.messages.add(msgRead);
                            } else {
                                Message msgWrite = Message.newBuilder()
                                        .setType(Message.Type.PL_DELIVER)
                                        .setFromAbstractionId(message.getToAbstractionId())
                                        .setToAbstractionId(message.getToAbstractionId() + ".pl")
                                        .setSystemId("sys-1")
                                        .setPlDeliver(PlDeliver.newBuilder()
                                                .setSender(message.getPlDeliver().getSender())
                                                .setMessage(Message.newBuilder()
                                                        .setType(Message.Type.APP_WRITE_RETURN)
                                                        .setToAbstractionId("hub")
                                                        .setFromAbstractionId("app")
                                                        .setAppWriteReturn(AppWriteReturn.newBuilder()
                                                                .setRegister(process.register)
                                                                .build())
                                                        .build())
                                                .build())
                                        .build();
                                process.messages.add(msgWrite);
                            }
                        }
                    }
                    return true;
                }
        }
        return false;
    }

    private int readListOccupied() {
        int c = 0;
        for (int i = 0; i < process.readList.length; i++) {
            if (process.readList[i] != null) {
                c++;
            }
        }
        return c;
    }

    private NnarInternalValue maxTimestamp() {
        int mtsOccurrence = 0;
        NnarInternalValue mts = process.readList[0];
        for (int i = 1; i < process.readList.length; i++) {
            if (mts != null && process.readList[i] != null &&
                    (process.readList[i].getTimestamp() > mts.getTimestamp()) &&
                    (process.readList[i].getValue().getV() != -1)) {

                mts = process.readList[i];
                mtsOccurrence++;
            }
        }
        if (mtsOccurrence > 1) {
            for (int i = 1; i < process.readList.length; i++) {
                if (process.readList[i] == mts && process.readList[i].getWriterRank() > mts.getWriterRank()
                        && process.readList[i].getValue().getV() != -1
                ) {
                    mts = process.readList[i];
                }
            }
        }
        return mts;
    }
}