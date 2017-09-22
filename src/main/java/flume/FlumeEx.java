package flume;

/**
 * Created by yachao on 17/9/10.
 */
public class FlumeEx {

    public static void main(String[] args) {
        MyRpcClientFacade clientFacade = new MyRpcClientFacade();
        clientFacade.init("bigData", 41414);

        String msg = "Hello Flume!";

        for (int i = 0; i < 10; i++) {
            clientFacade.sendDataToFlume(msg);
        }
        clientFacade.cleanUp();
    }

}
