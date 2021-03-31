using System;
using NATS.Client;

namespace PipServices3.Nats.Connect
{
    public interface INatsMessageListener
    {
        void OnMessage(object sender, MsgHandlerEventArgs e);
    }
}
