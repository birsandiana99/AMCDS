package client.Utilities;

import client.Infrastructure.Proc;
import main.CommunicationProtocol;

public class ECState {
    public CommunicationProtocol.ProcessId trusted;
    public int lastTimestamp;
    public int timestamp;

    public ECState(Proc process) {
        trusted = process.leader;
        lastTimestamp = 0;
        timestamp = ProcUtil.getCurrentProcess(process).getRank();
    }
}
