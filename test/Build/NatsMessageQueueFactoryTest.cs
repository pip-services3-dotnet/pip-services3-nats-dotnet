using System;
using PipServices3.Commons.Refer;
using PipServices3.Nats.Queues;
using Xunit;

namespace PipServices3.Nats.Build
{
    public class NatsMessageQueueFactoryTest : IDisposable
	{

		public void Dispose()
		{ }

		[Fact]
		public void TestFactoryCreateMessageQueue()
		{
			var factory = new NatsMessageQueueFactory();
			var descriptor = new Descriptor("pip-services", "message-queue", "nats", "test", "1.0");

			var canResult = factory.CanCreate(descriptor);
			Assert.NotNull(canResult);

			var queue = factory.Create(descriptor) as NatsMessageQueue;
			Assert.NotNull(queue);
			Assert.Equal("test", queue.Name);
		}

	}
}
