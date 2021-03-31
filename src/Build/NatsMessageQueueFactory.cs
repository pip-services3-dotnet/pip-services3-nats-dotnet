using PipServices3.Commons.Refer;
using PipServices3.Messaging.Build;
using PipServices3.Messaging.Queues;
using PipServices3.Nats.Queues;

namespace PipServices3.Nats.Build
{
    public class NatsMessageQueueFactory : MessageQueueFactory
    {
        private static readonly Descriptor NatsMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "nats", "*", "1.0");

        public NatsMessageQueueFactory()
        {
            Register(NatsMessageQueueDescriptor, (locator) => {
                Descriptor descriptor = locator as Descriptor;
                var name = descriptor != null ? descriptor.Name : null;
                return CreateQueue(name);
            });
        }

        public override IMessageQueue CreateQueue(string name)
        {
            var queue = new NatsMessageQueue(name);
            if (_config != null)
            {
                queue.Configure(_config);
            }
            if (_references != null)
            {
                queue.SetReferences(_references);
            }
            return queue;
        }
    }
}
