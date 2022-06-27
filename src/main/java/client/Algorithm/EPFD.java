package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;
import client.Utilities.EPFDState;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class EPFD implements AbstractionInterface{

    private Proc process;

    @Override
    public void init(Proc p) {
        process = p;
        process.epfdState = new EPFDState(process);
        process.epfdState.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                process.messages.add(
                        Message.newBuilder()
                                .setType(Message.Type.EPFD_TIMEOUT)
                                .setSystemId("sys-1")
                                .setFromAbstractionId(process.ucTopic+".ec.eld.epfd")
                                .setToAbstractionId(process.ucTopic+".ec.eld.epfd")
                                .setEpfdTimeout(EpfdTimeout.newBuilder()
                                        .build())
                                .build()
                );
            }
        }, process.epfdState.delay);
    }

    public List<ProcessId> processIntersection(List<ProcessId> processes1, List<ProcessId> processes2){
        List<ProcessId> res = new ArrayList<>();
        for(ProcessId pid : processes1){
            if(processes2.contains(pid)) {
                res.add(pid);
            }
        }
        return res;
    }

    public void timeout() {
        if(processIntersection(process.epfdState.alive, process.epfdState.suspected).size() != 0) {
            process.epfdState.delay += 100;
        }
        for(ProcessId pid:process.processes) {
            if(!process.epfdState.alive.contains(pid) && !process.epfdState.suspected.contains(pid)) {
                process.epfdState.suspected.add(pid);
                process.messages.add(Message.newBuilder()
                        .setType(Message.Type.EPFD_SUSPECT)
                        .setFromAbstractionId(process.ucTopic+".ec.eld.epfd")
                        .setToAbstractionId(process.ucTopic+".ec.eld")
                        .setSystemId("sys-1")
                        .setEpfdSuspect(EpfdSuspect.newBuilder()
                                .setProcess(pid)
                                .build())
                        .build());
            }
            else if(process.epfdState.alive.contains(pid) && process.epfdState.suspected.contains(pid)) {
                process.epfdState.suspected.remove(pid);
                process.messages.add(Message.newBuilder()
                        .setType(Message.Type.EPFD_RESTORE)
                        .setFromAbstractionId(process.ucTopic+".ec.eld.epfd")
                        .setToAbstractionId(process.ucTopic+".ec.eld")
                        .setSystemId("sys-1")
                        .setEpfdRestore(EpfdRestore.newBuilder().setProcess(pid).build())
                        .build());
            }

            process.messages.add(Message.newBuilder()
                    .setType(Message.Type.PL_SEND)
                    .setFromAbstractionId(process.ucTopic+".ec.eld.epfd")
                    .setToAbstractionId(process.ucTopic+".ec.eld.epfd.pl")
                    .setSystemId("sys-1")
                    .setPlSend(PlSend.newBuilder()
                            .setDestination(pid)
                            .setMessage(Message.newBuilder()
                                    .setFromAbstractionId(process.ucTopic+".ec.eld.epfd")
                                    .setToAbstractionId(process.ucTopic+".ec.eld.epfd")
                                    .setSystemId("sys-1")
                                    .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                                    .setEpfdInternalHeartbeatRequest(EpfdInternalHeartbeatRequest.newBuilder()
                                            .build())
                                    .build())
                            .build())
                    .build());
        }

        process.epfdState.alive = new ArrayList<>();
        process.epfdState.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                process.messages.add(
                        Message.newBuilder()
                                .setSystemId("sys-1")
                                .setType(Message.Type.EPFD_TIMEOUT)
                                .setFromAbstractionId(process.ucTopic+".ec.eld.epfd")
                                .setToAbstractionId(process.ucTopic+".ec.eld.epfd")
                                .setEpfdTimeout(EpfdTimeout.newBuilder()
                                        .build())
                                .build()
                );
            }
        }, process.epfdState.delay);
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case EPFD_TIMEOUT:
                timeout();
                return true;
            case PL_DELIVER:
                Message m = message.getPlDeliver().getMessage();
                if(m.getType().equals(Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST))
                {
                    Message msg = Message.newBuilder()
                            .setSystemId("sys-1")
                            .setFromAbstractionId(process.ucTopic+".ec.eld.epfd")
                            .setToAbstractionId(process.ucTopic+".ec.eld.epfd")
                            .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
                            .build();
                    plSend(msg, process.ucTopic+".ec.eld.epfd", process.ucTopic+".ec.eld.epfd.pl", message.getPlDeliver().getSender());
                    return true;
                }
                else if(m.getType().equals(Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY))
                {
                    process.epfdState.alive.add(message.getPlDeliver().getSender());
                    return true;
                }
                return false;
        }
        return false;
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
