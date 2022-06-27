package client.Utilities;

import client.Infrastructure.Proc;
import main.CommunicationProtocol;

public class ECState {
    public CommunicationProtocol.ProcessId trusted;
    public int lastTimestamp;
    public int timestamp;

    public ECState(Proc p){
        trusted = p.leader;
        lastTimestamp = 0;
        timestamp = ProcUtil.getCurrentProcess(p).getRank();
    }
}
