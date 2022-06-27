package client.Utilities;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

import java.util.List;

public class ProcUtil {
    public static ProcessId maxRank(List<ProcessId> processes) {
        if(!processes.isEmpty()){
            int maxRank = 0;
            ProcessId leader = processes.get(0);
            for (ProcessId pid : processes) {
                if (pid.getRank() > maxRank) {
                    maxRank = pid.getRank();
                    leader = pid;
                }

            }
            return leader;
        }
        return null;
    }

    public static ProcessId getProcessByAddress(List<ProcessId> processes, String host, int port) {
        ProcessId process = null;

        for (ProcessId pid : processes) {
            String p_host = pid.getHost();
            int p_port = pid.getPort();

            if (p_host.equals(host) && p_port == port) {
                process = pid;
            }
        }
        return process;
    }

    public static ProcessId getCurrentProcess(Proc process) {
        ProcessId p = null;
        for (ProcessId pid : process.processes) {
            if (pid.getPort() == process.port) {
                p = pid;
            }
        }
        return p;
    }

}
