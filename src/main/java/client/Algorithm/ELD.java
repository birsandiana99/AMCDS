package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;
import client.Utilities.ELDState;
import client.Utilities.ProcUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

public class ELD implements AbstractionInterface {

    private Proc process;

    @Override
    public void init(Proc p) {
        process = p;
        process.eldState = new ELDState();
        process.eldState.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                process.messages.add(
                        Message.newBuilder()
                                .setSystemId("sys-1")
                                .setType(Message.Type.ELD_TIMEOUT)
                                .setFromAbstractionId(process.ucTopic+".ec.eld")
                                .setToAbstractionId(process.ucTopic+".ec.eld")
                                .setEldTimeout(EldTimeout.newBuilder()
                                        .build())
                                .build()
                );
            }
        }, 100);
    }

    public List<ProcessId> getUnsuspected(List<ProcessId> processes, List<ProcessId> suspected){
        List<ProcessId> list = new ArrayList<>();
        for(ProcessId pid : processes) {
            if(!suspected.contains(pid)) {
                list.add(pid);
            }
        }
        return list;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case EPFD_SUSPECT:
                process.eldState.suspected.add(message.getEpfdSuspect().getProcess());
                return true;
            case EPFD_RESTORE:
                process.eldState.suspected.remove(message.getEpfdRestore().getProcess());
                return true;
            case ELD_TIMEOUT:
                List<ProcessId> unsuspectedProcs = getUnsuspected(process.processes,process.eldState.suspected);
                ProcessId unsuspLeader = null;
                if(!unsuspectedProcs.isEmpty()){
                    unsuspLeader = ProcUtil.maxRank(unsuspectedProcs);
                }

                if(unsuspLeader != null && process.leader.getRank() != unsuspLeader.getRank()) {

                    process.leader = unsuspLeader;
                    process.messages.add(Message.newBuilder()
                            .setSystemId("sys-1")
                            .setType(Message.Type.ELD_TRUST)
                            .setFromAbstractionId(process.ucTopic+".ec.eld")
                            .setToAbstractionId(process.ucTopic+".ec")
                            .setEldTrust(EldTrust.newBuilder()
                                    .setProcess(process.leader)
                                    .build())
                            .build());
                }
                return true;
        }
        return false;
    }
}
