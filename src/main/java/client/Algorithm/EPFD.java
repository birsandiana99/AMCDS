package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;
import client.Utilities.EPFDState;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

public class EPFD implements AbstractionInterface{

    private Proc process;

    @Override
    public void init(Proc process) {
        this.process = process;
        this.process.epfdState = new EPFDState(this.process);
        this.process.epfdState.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                EPFD.this.process.messages.add(
                        Message.newBuilder()
                                .setType(Message.Type.EPFD_TIMEOUT)
                                .setSystemId("sys-1")
                                .setFromAbstractionId(EPFD.this.process.ucTopic + ".ec.eld.epfd")
                                .setToAbstractionId(EPFD.this.process.ucTopic + ".ec.eld.epfd")
                                .setEpfdTimeout(EpfdTimeout.newBuilder()
                                        .build())
                                .build()
                );
            }
        }, this.process.epfdState.delay);
    }

    public List<ProcessId> processIntersection(List<ProcessId> processesList1, List<ProcessId> processesList2){
        List<ProcessId> res = new ArrayList<>();
        for(ProcessId pid : processesList1){
            if(processesList2.contains(pid)) {
                res.add(pid);
            }
        }
        return res;
    }

    public void timeout() {
        if (processIntersection(this.process.epfdState.alive, this.process.epfdState.suspected).size() != 0) {
            this.process.epfdState.delay += 100;
        }
        for (ProcessId pid: this.process.processes) {
            if (!this.process.epfdState.alive.contains(pid) && !this.process.epfdState.suspected.contains(pid)) {
                this.process.epfdState.suspected.add(pid);
                this.process.messages.add(Message.newBuilder()
                        .setType(Message.Type.EPFD_SUSPECT)
                        .setFromAbstractionId(this.process.ucTopic + ".ec.eld.epfd")
                        .setToAbstractionId(this.process.ucTopic + ".ec.eld")
                        .setSystemId("sys-1")
                        .setEpfdSuspect(EpfdSuspect.newBuilder()
                                .setProcess(pid)
                                .build())
                        .build());
            }
            else if (this.process.epfdState.alive.contains(pid) && this.process.epfdState.suspected.contains(pid)) {
                this.process.epfdState.suspected.remove(pid);
                this.process.messages.add(Message.newBuilder()
                        .setType(Message.Type.EPFD_RESTORE)
                        .setFromAbstractionId(this.process.ucTopic + ".ec.eld.epfd")
                        .setToAbstractionId(this.process.ucTopic + ".ec.eld")
                        .setSystemId("sys-1")
                        .setEpfdRestore(EpfdRestore.newBuilder().setProcess(pid).build())
                        .build());
            }

            this.process.messages.add(Message.newBuilder()
                    .setType(Message.Type.PL_SEND)
                    .setFromAbstractionId(this.process.ucTopic + ".ec.eld.epfd")
                    .setToAbstractionId(this.process.ucTopic + ".ec.eld.epfd.pl")
                    .setSystemId("sys-1")
                    .setPlSend(PlSend.newBuilder()
                            .setDestination(pid)
                            .setMessage(Message.newBuilder()
                                    .setFromAbstractionId(this.process.ucTopic + ".ec.eld.epfd")
                                    .setToAbstractionId(this.process.ucTopic + ".ec.eld.epfd")
                                    .setSystemId("sys-1")
                                    .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                                    .setEpfdInternalHeartbeatRequest(EpfdInternalHeartbeatRequest.newBuilder()
                                            .build())
                                    .build())
                            .build())
                    .build());
        }

        this.process.epfdState.alive = new ArrayList<>();
        this.process.epfdState.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                process.messages.add(
                        Message.newBuilder()
                                .setSystemId("sys-1")
                                .setType(Message.Type.EPFD_TIMEOUT)
                                .setFromAbstractionId(process.ucTopic + ".ec.eld.epfd")
                                .setToAbstractionId(process.ucTopic + ".ec.eld.epfd")
                                .setEpfdTimeout(EpfdTimeout.newBuilder()
                                        .build())
                                .build()
                );
            }
        }, this.process.epfdState.delay);
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case EPFD_TIMEOUT:
                timeout();
                return true;
            case PL_DELIVER:
                Message heartbeat = message.getPlDeliver().getMessage();
                if (heartbeat.getType().equals(Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)) {
                    Message heartbeatReplySend = Message.newBuilder()
                            .setSystemId("sys-1")
                            .setFromAbstractionId(this.process.ucTopic + ".ec.eld.epfd")
                            .setToAbstractionId(this.process.ucTopic + ".ec.eld.epfd")
                            .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
                            .build();
                    plSend(heartbeatReplySend, this.process.ucTopic + ".ec.eld.epfd", this.process.ucTopic + ".ec.eld.epfd.pl", message.getPlDeliver().getSender());
                    return true;
                }
                else if (heartbeat.getType().equals(Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)) {
                    this.process.epfdState.alive.add(message.getPlDeliver().getSender());
                    return true;
                }
                return false;
        }
        return false;
    }

    public void plSend(Message message, String from, String to, ProcessId pid) {
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
