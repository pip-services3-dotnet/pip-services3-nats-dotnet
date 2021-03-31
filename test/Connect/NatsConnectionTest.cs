using System;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using Xunit;

namespace PipServices3.Nats.Connect
{
    public class NatsConnectionTest : IDisposable
    {
        private bool _enabled;
        private NatsConnection _connection;

        public NatsConnectionTest()
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

            _connection = new NatsConnection();
            _connection.Configure(ConfigParams.FromTuples(
                "queue", NATS_QUEUE,
                "connection.uri", NATS_SERVICE_URI,
                "connection.protocol", "nats",
                "connection.host", NATS_SERVICE_HOST,
                "connection.port", NATS_SERVICE_PORT,
                "credential.username", NATS_USER,
                "credential.password", NATS_PASS,
                "credential.token", NATS_TOKEN
            ));
        }

        public void Dispose()
        {
        }

        [Fact]
        public async Task TestConnectionOpenCloseAsync()
        {
            if (_enabled)
            {
                await _connection.OpenAsync(null);

                Assert.True(_connection.IsOpen());
                Assert.NotNull(_connection.GetConnection());

                await _connection.CloseAsync(null);

                Assert.False(_connection.IsOpen());
                Assert.Null(_connection.GetConnection());
            }
        }

    }
}
