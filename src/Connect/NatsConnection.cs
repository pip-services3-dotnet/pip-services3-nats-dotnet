using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NATS.Client;
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
    ///      - retry_connect:        (optional)turns on / off automated reconnect when connection is log(default: true)
    ///      - max_reconnect:        (optional) maximum reconnection attempts (default: 3)
    ///      - reconnect_timeout:    (optional)number of milliseconds to wait on each reconnection attempt(default: 1000)
    ///      - flush_timeout:        (optional) number of milliseconds to wait on flushing messages (default: 3000)
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
            "options.connect_timeout", 0,
            "options.reconnect_timeout", 3000,
            "options.max_reconnect", 3,
            "options.flush_timeout", 3000
        );
        protected CompositeLogger _logger = new CompositeLogger();
        protected NatsConnectionResolver _connectionResolver = new NatsConnectionResolver();
        protected ConfigParams _options = new ConfigParams();

        // NATS connection object
        protected IConnection _connection;

        // Topic subscriptions
        protected List<NatsSubscription> _subscriptions = new List<NatsSubscription>();
        protected object _lock = new object();

        // Connection options
        private string _clientId;
        private bool _retryConnect = true;
        private int _maxReconnect = 3;
        private int _reconnectTimeout = 3000;
        private int _flushTimeout = 3000;

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
            _maxReconnect = config.GetAsIntegerWithDefault("options.max_reconnect", _maxReconnect);
            _reconnectTimeout = config.GetAsIntegerWithDefault("options.reconnect_timeout", _reconnectTimeout);
            _flushTimeout = config.GetAsIntegerWithDefault("options.flush_timeout", _flushTimeout);
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

            var opts = ConnectionFactory.GetDefaultOptions();
            opts.AllowReconnect = _retryConnect;
            opts.MaxReconnect = _maxReconnect;
            opts.ReconnectWait = _reconnectTimeout;

            var uri = options.GetAsString("uri");
            opts.Servers = uri.Split(',');

            var username = options.GetAsString("username");
            if (!string.IsNullOrEmpty(username))
            {
                opts.User = username;
                var password = options.GetAsString("password");
                opts.Password = password;
            }

            var token = options.GetAsString("token");
            if (!string.IsNullOrEmpty(token))
            {
                opts.Token = token;
            }

            try
            {
                var connection = new ConnectionFactory().CreateConnection(opts);
                _connection = connection;
            }
            catch (Exception ex)
            {
                _logger.Error(correlationId, ex, "Failed to connect to NATS broker at " + uri);
                throw ex;
            }

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
                _connection.Close();
            }
            finally
            {
                _connection = null;
                _subscriptions.Clear();

                _logger.Debug(correlationId, "Disconnected from NATS broker");
            }

            await Task.Delay(0);
        }

        public IConnection GetConnection()
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

        public async Task PublishAsync(string subject, Msg message)
        {
            CheckOpen();

            message.Subject = subject;
            _connection.Publish(message);

            await Task.Delay(0);
        }

        public async Task SubscribeAsync(string subject, string queue, INatsMessageListener listener)
        {
            CheckOpen();

            // Subscribe to the topic
            // Todo: Shall we skip if similar subscription already exist?
            var handler = _connection.SubscribeAsync(subject, queue, listener.OnMessage);

            lock (_lock)
            {
                // Add subscription to the list
                var subscription = new NatsSubscription
                {
                    Subject = subject,
                    Listener = listener,
                    Queue = queue,
                    Handler = handler
                };
                _subscriptions.Add(subscription);
            }

            await Task.Delay(0);
        }

        public async Task UnsubscribeAsync(string subject, string queue, INatsMessageListener listener)
        {
            CheckOpen();

            NatsSubscription deletedSubscription = null;

            lock (_lock)
            {
                // Find subscription
                for (var index = 0; index < _subscriptions.Count; index++)
                {
                    var subscription = _subscriptions[index];
                    if (subscription.Subject == subject && subscription.Queue == queue && subscription.Listener == listener)
                    {
                        deletedSubscription = subscription;
                        _subscriptions.RemoveAt(index);
                        break;
                    }
                }
            }

            // Unsubscribe if there are no more subscriptions
            if (deletedSubscription != null)
            {
                deletedSubscription.Handler.Unsubscribe();
            }

            await Task.Delay(0);
        }
    }
}
