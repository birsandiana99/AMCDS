package client.Algorithm;
import client.Infrastructure.Proc;
import main.CommunicationProtocol.*;

public class APP  implements AbstractionInterface{

    private Proc process;


    @Override
    public void init(Proc p) {
        process = p;
    }

    @Override
    public boolean handle(Message message) {
        switch (message.getType()) {
            case PL_DELIVER:
             //TODO:
             return true;
        }
        return false;
    }
}
