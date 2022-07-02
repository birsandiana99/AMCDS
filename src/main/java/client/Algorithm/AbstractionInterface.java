package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public interface AbstractionInterface {
    void init(Proc process);
    boolean handle(Message message);
}
