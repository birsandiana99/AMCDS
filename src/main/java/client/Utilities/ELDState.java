package client.Utilities;

import main.CommunicationProtocol.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;

public class ELDState {
    public List<ProcessId> suspected;
//    public ProcessId leader;
    public Timer timer;

    public ELDState(){
        timer = new Timer();
        suspected = new ArrayList<>();
//        leader = null;
    }
}
