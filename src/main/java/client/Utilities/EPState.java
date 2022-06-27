package client.Utilities;

import main.CommunicationProtocol.*;

import java.util.HashMap;

public class EPState {
    public EpInternalState st;
    public Value tmpval;
    public HashMap<Integer, EpInternalState> states;
    public int accepted;
    public ProcessId leader;
    public int ets;
    public boolean halted;

    public EPState(EpInternalState state, int ets, ProcessId l){
        st = state;
        this.ets = ets;
        leader = l;
        tmpval = Value.newBuilder().setDefined(false).build();
        states = new HashMap<>();
        accepted = 0;
        halted = false;
    }
}
