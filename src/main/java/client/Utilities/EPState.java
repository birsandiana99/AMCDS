package client.Utilities;

import main.CommunicationProtocol.*;

import java.util.HashMap;

public class EPState {
    public EpInternalState st;
    public Value tmpval;
    public HashMap<Integer, EpInternalState> states;
    public int accepted;
    public int ets;
    public boolean halted;

    public EPState(EpInternalState state, int ets, ProcessId l) {
        this.st = state;
        this.ets = ets;
        this.tmpval = Value.newBuilder().setDefined(false).build();
        this.states = new HashMap<>();
        this.accepted = 0;
        this.halted = false;
    }
}
