using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NATSnet;
using NATSnet.Client;
using NATSnet.Client.Disconnecting;
using NATSnet.Client.Options;
using NATSnet.Extensions.ManagedClient;
using NATSnet.Protocol;
using PipServices3.Commons.Config;
using PipServices3.Commons.Errors;
using PipServices3.Commons.Refer;
using PipServices3.Commons.Run;
using PipServices3.Components.Log;
using PipServices3.Messaging.Connect;

namespace PipServices3.Nats.Connect
{
    /// <summary>
    ///  NATS connection using plain driver.
    ///  By defining a connection and sharing it through multiple message queues
    ///  you can reduce number of used connections.

    ///  ### Configuration parameters ###
    ///    - client_id:               (optional) name of the client id
    ///    - connection(s):
    ///      - discovery_key:               (optional) a key to retrieve the connection from [[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/connect.idiscovery.html IDiscovery]]
    ///      - host:                        host name or IP address
    ///      - port:                        port number
    ///      - uri:                         resource URI or connection string with all parameters in it
    ///    - credential(s):
    ///      - store_key:                   (optional) a key to retrieve the credentials from[[https://pip-services3-node.github.io/pip-services3-components-node/interfaces/auth.icredentialstore.html ICredentialStore]]
    ///      -username:                    user name
    ///      - password:                    user password
    ///    - options:
    ///      -retry_connect:        (optional)turns on / off automated reconnect when connection is log(default: true)
    ///      - connect_timeout:      (optional)number of milliseconds to wait for connection(default: 30000)
    ///      - reconnect_timeout:    (optional)number of milliseconds to wait on each reconnection attempt(default: 1000)
    ///      - keepalive_timeout:    (optional)number of milliseconds to ping broker while inactive(default: 3000)
    ///  
    ///  ### References ###
    ///   - \*:logger:\*:\*:1.0(optional) ILogger components to pass log messages
    ///   - \*:discovery:\*:\*:1.0(optional) IDiscovery services
    ///   - \*:credential - store:\*:\*:1.0(optional) Credential stores to resolve credentials
    /// </summary>
    public class NatsConnection: IMessageQueueConnection, IConfigurable, IReferenceable, IOpenable
    {
        private static ConfigParams _defaultConfig = ConfigParams.FromTuples(
            "options.retry_connect", true,
            "options.connect_timeout", 30000,
            "options.reconnect_timeout", 1000,
            "options.keepalive_timeout", 60000
        );
        protected CompositeLogger _logger = new CompositeLogger();
        protected NatsConnectionResolver _connectionResolver = new NatsConnectionResolver();
        protected ConfigParams _options = new ConfigParams();

        // NATS connection object
        private INatsClientOptions _clientOptions;
        protected INatsClient _connection;

        // Topic subscriptions
        protected List<NatsSubscription> _subscriptions = new List<NatsSubscription>();
        protected object _lock = new object();

        // Connection options
        private string _clientId;
        private bool _retryConnect = true;
        private int _connectTimeout = 30000;
        private int _reconnectTimeout = 60000;
        private int _keepAliveTimeout = 1000;

        public NatsConnection()
        {
        }

        /// <summary>
        /// Configure are configures component by passing configuration parameters.
        /// </summary>
        /// <param name="config">Configuration parameters to be set</param>
        public void Configure(ConfigParams config)
        {
            config = config.SetDefaults(_defaultConfig);
            _connectionResolver.Configure(config);
            _logger.Configure(config);

            _options = _options.Override(config.GetSection("options"));

            _clientId = config.GetAsStringWithDefault("client_id", _clientId);
            _retryConnect = config.GetAsBooleanWithDefault("options.retry_connect", _retryConnect);
            _connectTimeout = config.GetAsIntegerWithDefault("options.connect_timeout", _connectTimeout);
            _reconnectTimeout = config.GetAsIntegerWithDefault("options.reconnect_timeout", _reconnectTimeout);
            _keepAliveTimeout = config.GetAsIntegerWithDefault("options.keepalive_timeout", _keepAliveTimeout);
        }

        /// <summary>
        /// SetReferences are sets references to dependent components.
        /// </summary>
        /// <param name="references">References to be set</param>
        public void SetReferences(IReferences references)
        {
            _connectionResolver.SetReferences(references);
            _logger.SetReferences(references);
        }

        /// <summary>
        /// Checks if the component is opened.
        /// </summary>
        /// <returns>true if the component has been opened and false otherwise.</returns>
        public bool IsOpen()
        {
            return _connection != null;
        }

        /// <summary>
        /// Opens the component.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public async Task OpenAsync(string correlationId)
        {
            var options = await _connectionResolver.ResolveAsync(correlationId);

            var opts = new NatsClientOptionsBuilder()
                .WithClientId(_clientId);

            if (_keepAliveTimeout > 0)
            {
                opts.WithKeepAlivePeriod(TimeSpan.FromMilliseconds(_keepAliveTimeout));
            }
            else
            {
                opts.WithNoKeepAlive();
            }

            var uri = options.GetAsString("servers") ?? "";
            var servers = uri.Split(',');
            foreach (var server in servers) {
                var host = server;
                var port = 4222;

                var pos = server.IndexOf(":");
                if (pos > 0)
                {
                    host = server.Substring(0, pos);
                    Int32.TryParse(server.Substring(pos + 1), out port);
                }

                opts.WithTcpServer(host, port);
            }

            var username = options.GetAsString("username");
            if (!string.IsNullOrEmpty(username))
            {
                var password = options.GetAsString("password");
                opts.WithCredentials(username, password);
            }

            //opts.SetAutoReconnect(c.retryConnect)
            //opts.SetConnectTimeout(time.Millisecond * time.Duration(c.connectTimeout))
            //opts.SetConnectRetryInterval(time.Millisecond * time.Duration(c.reconnectTimeout))

            var client = new NatsFactory().CreateNatsClient();
            client.UseDisconnectedHandler(DisconnectedHandlerAsync);
            client.UseApplicationMessageReceivedHandler(MessageReceiveHandlerAsync);

            try
            {
                await client.ConnectAsync(opts.Build());
            }
            catch (Exception ex)
            {
                _logger.Error(correlationId, ex, "Failed to connect to NATS broker at " + uri);
                throw ex;
            }

            _connection = client;
            _clientOptions = opts.Build();

            _logger.Debug(correlationId, "Connected to NATS broker at " + uri);
        }

        /// <summary>
        /// Closes component and frees used resources.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public async Task CloseAsync(string correlationId)
        {
            if (_connection == null)
            {
                return;
            }

            try
            {
                await _connection.DisconnectAsync();
            }
            finally
            {
                _connection = null;
                _subscriptions.Clear();

                _logger.Debug(correlationId, "Disconnected from NATS broker");
            }
        }

        private async Task DisconnectedHandlerAsync(NatsClientDisconnectedEventArgs e)
        {
            //_logger.Debug(null, "Connection failed");

            await Task.Delay(TimeSpan.FromMilliseconds(_reconnectTimeout));

            try
            {
                if (_connection != null)
                {
                    await _connection.ConnectAsync(_clientOptions);
                }
            }
            catch
            {
                // Skip...
            }
        }

        private async Task MessageReceiveHandlerAsync(NatsApplicationMessageReceivedEventArgs e)
        {
            var message = e.ApplicationMessage;
            var topic = message.Topic;

            // Get subscriptions
            IEnumerable<NatsSubscription> subscriptions;
            lock (_lock)
            {
                subscriptions = _subscriptions.ToArray();
            }

            // Forward messages
            foreach (var subscription in subscriptions)
            {
                // Todo: Implement proper filtering by wildcards?
                if (subscription.Filter && topic != subscription.Topic)
                {
                    continue;
                }

                subscription.Listener.OnMessage(message);
            }

            await Task.Delay(0);
        }

        public INatsClient GetConnection()
        {
            return _connection;
        }

        /// <summary>
        /// Reads names of available queues or topics.
        /// </summary>
        /// <returns>A list with queue names</returns>
        public async Task<List<string>> ReadQueueNamesAsync()
        {
            return await Task.FromResult(new List<string>());
        }

        /// <summary>
        /// Create a queue or topic with a specified name.
        /// </summary>
        /// <param name="name">A name of the queue to be created</param>
        public async Task CreateQueueAsync(string name)
        {
            await Task.Delay(0);
        }

        /// <summary>
        /// Delete a queue or topic with a specified name.
        /// </summary>
        /// <param name="name">A name of the queue to be deleted</param>
        public async Task DeleteQueueAsync(string name)
        {
            await Task.Delay(0);
        }

        private void CheckOpen()
        {
            if (_connection != null)
            {
                return;
            }

            throw new InvalidStateException(
                null,
                "NOT_OPEN",
                "Connection was not opened"
            );
        }

        public async Task PublishAsync(string topic, NatsApplicationMessage message)
        {
            CheckOpen();

            message.Topic = topic;

            await _connection.PublishAsync(message);
        }

        public async Task SubscribeAsync(string topic, NatsQualityOfServiceLevel qos, INatsMessageListener listener)
        {
            CheckOpen();

            // Subscribe to the topic
            // Todo: Shall we skip if similar subscription already exist?
            await _connection.SubscribeAsync(topic, qos);

            lock (_lock)
            {
                var filter = topic.IndexOf("*") < 0;

                // Add subscription to the list
                var subscription = new NatsSubscription
                {
                    Topic = topic,
                    Listener = listener,
                    Filter = filter,
                    Qos = qos
                };
                _subscriptions.Add(subscription);
            }
        }

        public async Task UnsubscribeAsync(string topic, INatsMessageListener listener)
        {
            CheckOpen();

            NatsSubscription deletedSubscription = null;
            var hasMoreSubscriptions = false;

            lock (_lock)
            {
                // Find subscription
                for (var index = 0; index < _subscriptions.Count; index++)
                {
                    var subscription = _subscriptions[index];
                    if (subscription.Topic == topic && subscription.Listener == listener)
                    {
                        deletedSubscription = subscription;
                        _subscriptions.RemoveAt(index);
                        break;
                    }
                }

                // Find subscriptions to the same topic
                foreach (var subscription in _subscriptions)
                {
                    if (subscription.Topic == topic)
                    {
                        hasMoreSubscriptions = true;
                    }
                }
            }

            // Unsubscribe if there are no more subscriptions
            if (!hasMoreSubscriptions)
            {
                await _connection.UnsubscribeAsync(topic);
            }
        }
    }
}
