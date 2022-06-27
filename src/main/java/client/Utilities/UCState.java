package client.Utilities;

import main.CommunicationProtocol.*;

public class UCState {

    public Value val;
    public boolean proposed;
    public boolean decided;
    public int ets;
    public int newts;
    public ProcessId newl;

    public UCState(){
        val = Value.newBuilder().setDefined(false).build();
        proposed = false;
        decided = false;
        ets = 0;
        newts = 0;
        newl = null;
    }
}
