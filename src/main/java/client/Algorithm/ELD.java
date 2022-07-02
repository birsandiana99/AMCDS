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
    public void init(Proc process) {
        this.process = process;
        this.process.eldState = new ELDState();
        this.process.eldState.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                ELD.this.process.messages.add(
                        Message.newBuilder()
                                .setSystemId("sys-1")
                                .setType(Message.Type.ELD_TIMEOUT)
                                .setFromAbstractionId(ELD.this.process.ucTopic + ".ec.eld")
                                .setToAbstractionId(ELD.this.process.ucTopic + ".ec.eld")
                                .setEldTimeout(EldTimeout.newBuilder()
                                        .build())
                                .build()
                );
            }
        }, 100);
    }

    public List<ProcessId> getUnsuspected(List<ProcessId> processes, List<ProcessId> suspected){
        List<ProcessId> list = new ArrayList<>();
        for (ProcessId pid : processes) {
            if (!suspected.contains(pid)) {
                list.add(pid);
            }
        }
        return list;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case EPFD_SUSPECT:
                this.process.eldState.suspected.add(message.getEpfdSuspect().getProcess());
                return true;
            case EPFD_RESTORE:
                this.process.eldState.suspected.remove(message.getEpfdRestore().getProcess());
                return true;
            case ELD_TIMEOUT:
                List<ProcessId> unsuspectedProcesses = this.getUnsuspected(this.process.processes,process.eldState.suspected);
                ProcessId unsuspectedLeader = null;
                if (!unsuspectedProcesses.isEmpty()) {
                    unsuspectedLeader = ProcUtil.maxRank(unsuspectedProcesses);
                }

                if (unsuspectedLeader != null && this.process.leader.getRank() != unsuspectedLeader.getRank()) {

                    this.process.leader = unsuspectedLeader;
                    this.process.messages.add(Message.newBuilder()
                            .setSystemId("sys-1")
                            .setType(Message.Type.ELD_TRUST)
                            .setFromAbstractionId(this.process.ucTopic + ".ec.eld")
                            .setToAbstractionId(this.process.ucTopic + ".ec")
                            .setEldTrust(EldTrust.newBuilder()
                                    .setProcess(this.process.leader)
                                    .build())
                            .build());
                }
                return true;
        }
        return false;
    }
}
