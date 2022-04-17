package client.Algorithm;

import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public interface AbstractionInterface {

    public void init(Proc p);
    public boolean handle(Message message);
}
