package client.Infrastructure;

import client.Algorithm.AbstractionInterface;
import client.Utilities.*;
import main.CommunicationProtocol.*;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Proc {
    public Socket socketHub;
    public static final String ADDR_HUB = "127.0.0.1";
    public static final int PORT_HUB = 5000;

    public String owner;
    public String addr;
    public String debugName;
    public int port;
    public int index;
    public int rank;

    public int refPort;
    public ProcessId pid;

    public List<ProcessId> processes = new ArrayList<>();
    public Map<String, AbstractionInterface> abstractionInterfaceMap = new HashMap<String, AbstractionInterface>();
    public int value;
    public List<Message> messages = new ArrayList<>();

    public SharedMemory sharedMemory;


    //UC = leader
    public ProcessId leader;
    public UCState ucState;
    public EPState epState;
    public ECState ecState;
    public ELDState eldState;
    public EPFDState epfdState;

    public String ucTopic;


    public Proc(int p, int i, int rp) throws UnknownHostException, IOException {
        socketHub = new Socket(ADDR_HUB, PORT_HUB);
        owner = "abc";
        addr = "127.0.0.1";
        port = p;
        index = i;
        refPort = rp;
        debugName = owner + "-" + index;
        sharedMemory = new SharedMemory();
    }

    public ProcessId getProcByPort(int port) {
        for (ProcessId pid: processes) {
            if (pid.getPort() == port) {
                return pid;
            }
        }
        return null;
    }

    public void setProcesses(List<ProcessId> processes) {
        this.processes = processes;
        this.pid = getProcByPort(this.port);
    }

    public ProcessId getLeader() {
        int maxRank = 0;
        ProcessId leader = this.processes.get(0);
        for (ProcessId pid: this.processes) {
            if (pid.getRank() > maxRank) {
                maxRank = pid.getRank();
                leader = pid;
            }
        }
        return leader;
    }
}
