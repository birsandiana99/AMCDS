package client.Utilities;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;

public class EPFDState {
    public List<ProcessId> alive;
    public List<ProcessId> suspected;
    public long delay;
    public Timer timer;

    public EPFDState(Proc p){
        alive = p.processes;
        suspected = new ArrayList<>();
        delay = 100;
        timer = new Timer();
    }
}
