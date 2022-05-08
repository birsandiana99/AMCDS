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
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_BROADCAST)) {
                    bebBroadcast(message.getPlDeliver().getMessage());
                    return true;
                }
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_WRITE)) {
                    String register = message.getPlDeliver().getMessage().getAppWrite().getRegister();

                    registerAbstraction(register);

                    Message msg = Message.newBuilder()
                            .setType(Message.Type.NNAR_WRITE)
                            .setFromAbstractionId("app")
                            .setToAbstractionId("app.nnar[" + register + "]")
                            .setSystemId("sys-1")
                            .setNnarWrite(NnarWrite.newBuilder()
                                    .setValue(message.getPlDeliver().getMessage().getAppWrite().getValue())
                                    .build())
                            .build();
                    process.messages.add(msg);
                    return true;
                }
                if (message.getPlDeliver().getMessage().getType().equals(Message.Type.APP_READ)) {
                    String register = message.getPlDeliver().getMessage().getAppRead().getRegister();

                    registerAbstraction(register);

                    Message msg = Message.newBuilder()
                            .setType(Message.Type.NNAR_READ)
                            .setFromAbstractionId("app")
                            .setToAbstractionId("app.nnar[" + register + "]")
                            .setSystemId("sys-1")
                            .build();
                    process.messages.add(msg);
                    return true;
                }

            case BEB_DELIVER:
                plSend(message.getBebDeliver().getMessage());
                return true;
        }
        return false;
    }


    private void bebBroadcast(Message message) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.BEB_BROADCAST)
                .setBebBroadcast(BebBroadcast.newBuilder()
                        .setMessage(Message.newBuilder()
                                .setType(Message.Type.APP_VALUE)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.beb")
                                .setSystemId("sys-1")
                                .setAppValue(AppValue.newBuilder()
                                        .setValue(message.getAppBroadcast().getValue())
                                        .build())
                                .build())
                        .build())
                .setFromAbstractionId("app")
                .setToAbstractionId("app.beb")
                .setSystemId("sys-1")
                .build());

    }

    private void plSend(Message message) {
        process.messages.add(Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setMessage(Message.newBuilder()
                                .setType(Message.Type.APP_VALUE)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.pl")
                                .setSystemId("sys-1")
                                .setAppValue(message.getAppValue())
                                .build())
                        .build())
                .setFromAbstractionId("app")
                .setToAbstractionId("app.pl")
                .setSystemId("sys-1")
                .build());
    }

    private void registerAbstraction(String register) {
        if (process.abstractionInterfaceMap.get("app.nnar[" + register + "]") == null) {
            process.abstractionInterfaceMap.put("app.nnar[" + register + "]", new NNAR());
            process.abstractionInterfaceMap.get("app.nnar[" + register + "]").init(process);

            process.abstractionInterfaceMap.put("app.nnar[" + register + "].pl", new PL());
            process.abstractionInterfaceMap.get("app.nnar[" + register + "].pl").init(process);

            process.abstractionInterfaceMap.put("app.nnar[" + register + "].beb", new BEB());
            process.abstractionInterfaceMap.get("app.nnar[" + register + "].beb").init(process);

            process.abstractionInterfaceMap.put("app.nnar[" + register + "].beb.pl", new PL());
            process.abstractionInterfaceMap.get("app.nnar[" + register + "].beb.pl").init(process);

            process.register = register;

            System.out.println("Registered NNAR on " + process.debugName);
        }
    }
}
