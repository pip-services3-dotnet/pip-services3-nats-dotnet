using PipServices3.Nats.Queues;
using PipServices3.Components.Build;
using PipServices3.Commons.Refer;

namespace PipServices3.Nats.Build
{
    /// <summary>
    /// Creates NatsMessageQueue components by their descriptors.
    /// </summary>
    public class DefaultNatsFactory: Factory
    {
        private static Descriptor NatsMessageQueueFactoryDescriptor = new Descriptor("pip-services", "queue-factory", "nats", "*", "1.0");
        private static Descriptor NatsMessageQueueDescriptor = new Descriptor("pip-services", "message-queue", "nats", "*", "1.0");

        /// <summary>
        /// Create a new instance of the factory.
        /// </summary>
        public DefaultNatsFactory()
        {
            RegisterAsType(NatsMessageQueueFactoryDescriptor, typeof(NatsMessageQueueFactory));
            Register(NatsMessageQueueDescriptor, (locator) => {
                Descriptor descriptor = locator as Descriptor;
                var name = descriptor != null ? descriptor.Name : null;
                return new NatsMessageQueue(name);
            });
        }
    }
}
