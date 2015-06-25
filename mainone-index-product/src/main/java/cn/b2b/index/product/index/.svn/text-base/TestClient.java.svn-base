package cn.b2b.index.product.index;

import java.net.InetSocketAddress;

import cn.b2b.common.rpc.ipc.Client;
import cn.b2b.common.search.bean.IndexRegisterInfo;
import cn.b2b.common.search.bean.IndexServerInfo;
import cn.b2b.common.search.controller.bean.ControlParam;
import cn.b2b.common.search.controller.bean.ControlResult;
import cn.b2b.common.search.util.Constants;

public class TestClient {

    public static void main(String[] args) throws Exception {
        IndexServerInfo serverInfo = new IndexServerInfo("", "192.168.3.71", 9000, 0,
                0, "", "", 1, 1, new String[]{""}, 0);
        IndexRegisterInfo registInfo = new IndexRegisterInfo("", 0, 0, 0, 3,
                serverInfo);
        Client controlClient = new Client(ControlResult.class);
        InetSocketAddress controllerAddr = new InetSocketAddress("192.168.3.62", 8000);
        ControlParam param = new ControlParam(Constants.OP_REGISTER, registInfo);
        ControlResult result = (ControlResult) controlClient.call(param, controllerAddr);
        System.out.println(result);

    }
}
