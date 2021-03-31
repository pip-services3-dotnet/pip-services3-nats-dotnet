using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using System;
using Xunit;

namespace PipServices3.Nats.Queues
{
    public class NatsMessageQueueTest: IDisposable
    {
        private bool _enabled;
        private NatsMessageQueue _queue;
        private MessageQueueFixture _fixture;

        public NatsMessageQueueTest()
        {
            var NATS_SERVICE_URI = Environment.GetEnvironmentVariable("NATS_SERVICE_URI");
            var NATS_SERVICE_HOST = Environment.GetEnvironmentVariable("NATS_SERVICE_HOST") ?? "localhost";
            var NATS_SERVICE_PORT = Environment.GetEnvironmentVariable("NATS_SERVICE_PORT") ?? "4222";
            var NATS_QUEUE = Environment.GetEnvironmentVariable("NATS_QUEUE") ?? "test";
            var NATS_USER = Environment.GetEnvironmentVariable("NATS_USER"); // ?? "user";
            var NATS_PASS = Environment.GetEnvironmentVariable("NATS_PASS"); // ?? "pass123";
            var NATS_TOKEN = Environment.GetEnvironmentVariable("NATS_TOKEN");

            _enabled = !string.IsNullOrEmpty(NATS_SERVICE_URI) || !string.IsNullOrEmpty(NATS_SERVICE_HOST);
            if (!_enabled)
            {
                return;
            }

            _queue = new NatsMessageQueue();
            _queue.Configure(ConfigParams.FromTuples(
                "queue", NATS_QUEUE,
                "connection.protocol", "nats",
                "connection.uri", NATS_SERVICE_URI,
                "connection.host", NATS_SERVICE_HOST,
                "connection.port", NATS_SERVICE_PORT,
                "credential.username", NATS_USER,
                "credential.password", NATS_PASS,
                "credential.token", NATS_TOKEN,
                "options.autosubscribe", true
            ));

            _queue.OpenAsync(null).Wait();
            _queue.ClearAsync(null).Wait();

            _fixture = new MessageQueueFixture(_queue);
        }

        public void Dispose()
        {
            if (_queue != null)
            {
                _queue.CloseAsync(null).Wait();
            }
        }

        [Fact]
        public void TestQueueSendReceiveMessage()
        {
            if (_enabled)
            {
                _fixture.TestSendReceiveMessageAsync().Wait();
            }
        }

        [Fact]
        public void TestQueueReceiveSendMessage()
        {
            if (_enabled)
            {
                _fixture.TestReceiveSendMessageAsync().Wait();
            }
        }

        [Fact]
        public void TestQueueSendPeekMessage()
        {
            if (_enabled)
                _fixture.TestSendPeekMessageAsync().Wait();
        }

        [Fact]
        public void TestQueuePeekNoMessage()
        {
            if (_enabled)
                _fixture.TestPeekNoMessageAsync().Wait();
        }

        [Fact]
        public void TestQueueOnMessage()
        {
            if (_enabled)
            {
                _fixture.TestOnMessageAsync().Wait();
            }
        }

    }
}
