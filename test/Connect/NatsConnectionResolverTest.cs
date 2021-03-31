using System;
using System.Threading.Tasks;
using PipServices3.Commons.Config;
using Xunit;

namespace PipServices3.Nats.Connect
{
    public class NatsConnectionResolverTest: IDisposable
    {

		public void Dispose()
		{ }

        [Fact]
        public async Task TestSingleConnectionAsync()
        {
			var resolver = new NatsConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connection.protocol", "nats",
				"connection.host", "localhost",
				"connection.port", 4222
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("nats://localhost:4222", connection.GetAsString("uri"));
			Assert.Null(connection.GetAsString("username"));
			Assert.Null(connection.GetAsString("password"));
			Assert.Null(connection.GetAsString("token"));
		}

		[Fact]
		public async Task TestSingleConnectionWithAuthAsync()
        {
			var resolver = new NatsConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connection.protocol", "nats",
				"connection.host", "localhost",
				"connection.port", 4222,
				"credential.username", "test",
				"credential.password", "pass123",
				"credential.token", "ABC"
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("nats://localhost:4222", connection.GetAsString("uri"));
			Assert.Equal("test", connection.GetAsString("username"));
			Assert.Equal("pass123", connection.GetAsString("password"));
			Assert.Equal("ABC", connection.GetAsString("token"));
		}

		[Fact]
		public async Task TestClusterConnectionAsync()
        {
			var resolver = new NatsConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connections.0.protocol", "nats",
				"connections.0.host", "server1",
				"connections.0.port", 4222,
				"connections.1.protocol", "nats",
				"connections.1.host", "server2",
				"connections.1.port", 4222,
				"connections.2.protocol", "nats",
				"connections.2.host", "server3",
				"connections.2.port", 4222
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("nats://server1:4222,nats://server2:4222,nats://server3:4222", connection.GetAsString("uri"));
			Assert.Null(connection.GetAsString("username"));
			Assert.Null(connection.GetAsString("password"));
			Assert.Null(connection.GetAsString("token"));
		}

		[Fact]
		public async Task TestClusterConnectionWithAuthAsync()
		{
			var resolver = new NatsConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connections.0.protocol", "nats",
				"connections.0.host", "server1",
				"connections.0.port", 4222,
				"connections.1.protocol", "nats",
				"connections.1.host", "server2",
				"connections.1.port", 4222,
				"connections.2.protocol", "nats",
				"connections.2.host", "server3",
				"connections.2.port", 4222,
				"credential.username", "test",
				"credential.password", "pass123",
				"credential.token", "ABC"
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("nats://server1:4222,nats://server2:4222,nats://server3:4222", connection.GetAsString("uri"));
			Assert.Equal("test", connection.GetAsString("username"));
			Assert.Equal("pass123", connection.GetAsString("password"));
			Assert.Equal("ABC", connection.GetAsString("token"));
		}

		[Fact]
		public async Task TestUriConnectionWithAuthAsync()
		{
			var resolver = new NatsConnectionResolver();
			resolver.Configure(ConfigParams.FromTuples(
				"connection.uri", "nats://server1:4222,nats://server2:4222,nats://server3:4222",
				"credential.username", "test",
				"credential.password", "pass123",
				"credential.token", "ABC"
			));

			var connection = await resolver.ResolveAsync(null);
			Assert.Equal("nats://server1:4222,nats://server2:4222,nats://server3:4222", connection.GetAsString("uri"));
			Assert.Equal("test", connection.GetAsString("username"));
			Assert.Equal("pass123", connection.GetAsString("password"));
			Assert.Equal("ABC", connection.GetAsString("token"));
		}
	}
}
