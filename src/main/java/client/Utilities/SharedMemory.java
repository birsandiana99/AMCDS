package client.Utilities;

import main.CommunicationProtocol;

public class SharedMemory {
    public int value;
    public int rid;
    public int writerRank;
    public int ts;
    public int acks;
    public int writeVal;
    public int readVal;

    public CommunicationProtocol.NnarInternalValue[] readList;
    public boolean reading;
    public String register;
    public String appNnar;
}
