package client.Infrastructure;

import client.Algorithm.AbstractionInterface;
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

    public List<Message> messages = new ArrayList<>();

    public int value;
    public int rid;
    public int writerRank;
    public int ts;
    public int acks;
    public int writeVal;
    public int readVal;

    public NnarInternalValue[] readList;
    public boolean reading;
    public String register;

    public Proc(int p, int i, int rp) throws UnknownHostException, IOException {
        socketHub = new Socket(ADDR_HUB, PORT_HUB);
        owner = "abc";
        addr = "127.0.0.1";
        port = p;
        index = i;
        refPort = rp;
        debugName = owner + "-" + index;
    }

    public ProcessId getProcByPort(int port) {
        for (ProcessId pid: processes) {
            if (pid.getPort() == port) {
                return pid;
            }
        }
        return null;
    }

    public void setProcesses(List<ProcessId> procs) {
        processes = procs;
        pid = getProcByPort(port);
    }
}
