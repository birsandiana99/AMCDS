package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public interface AbstractionInterface {
    void init(Proc p);
    boolean handle(Message message);
}
