package client.Utilities;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

import java.util.List;

public class ProcUtil {
    public static ProcessId maxRank(List<ProcessId> processes) {
        if (!processes.isEmpty()) {
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
            String processHost = pid.getHost();
            int processPort = pid.getPort();

            if (processHost.equals(host) && processPort == port) {
                process = pid;
            }
        }
        return process;
    }

    public static ProcessId getCurrentProcess(Proc process) {
        ProcessId proc = null;
        for (ProcessId pid : process.processes) {
            if (pid.getPort() == process.port) {
                proc = pid;
            }
        }
        return proc;
    }

}
