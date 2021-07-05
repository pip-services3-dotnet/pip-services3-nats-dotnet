using System;
using System.Text;
using System.Threading.Tasks;
using NATS.Client;
using PipServices3.Commons.Config;
using PipServices3.Commons.Convert;
using PipServices3.Commons.Errors;
using PipServices3.Commons.Refer;
using PipServices3.Messaging.Queues;
using PipServices3.Nats.Connect;

namespace PipServices3.Nats.Queues
{
    /// <summary>
    /// NatsMessageQueue are message queue that sends and receives messages via NATS message broker.
    ///
    /// Configuration parameters:
    ///
    /// - subject:                       name of NATS topic (subject) to subscribe
    /// - queue_group:                   name of NATS queue group
    /// - connection(s) :
    ///  - discovery_key:               (optional) a key to retrieve the connection from  IDiscovery
    ///  - host:                        host name or IP address
    ///  - port:                        port number
    ///  - uri:                         resource URI or connection string with all parameters in it
    /// - credential(s) :
    ///  - store_key:                   (optional) a key to retrieve the credentials from  ICredentialStore
    ///  - username:                    user name
    ///  - password:                    user password
    /// - options:
    ///  - serialize_message:    (optional) true to serialize entire message as JSON, false to send only message payload(default: true)
    ///  - autosubscribe:        (optional) true to automatically subscribe on option(default: false)
    ///  - retry_connect:        (optional) turns on/off automated reconnect when connection is log(default: true)
    ///  - max_reconnect:        (optional) maximum reconnection attempts (default: 3)
    ///  - reconnect_timeout:    (optional) number of milliseconds to wait on each reconnection attempt (default: 3000)
    ///  - flush_timeout:        (optional) number of milliseconds to wait on flushing messages (default: 3000)
    ///
    ///
    /// References:
    ///
    /// - *:logger:*:*:1.0             (optional) ILogger components to pass log messages
    /// - *:counters:*:*:1.0           (optional) ICounters components to pass collected measurements
    /// - *:discovery:*:*:1.0          (optional) IDiscovery services to resolve connections
    /// - *:credential-store:*:*:1.0   (optional) Credential stores to resolve credentials
    /// - *:connection:nats:*:1.0      (optional) Shared connection to NATS service
    ///
    /// See MessageQueue
    /// See MessagingCapabilities
    ///
    /// Example:
    ///
    ///    var queue = new NatsMessageQueue("myqueue");
    ///    queue.Configure(ConfigParams.FromTuples(
    ///      "subject", "mytopic",
    ///      "connection.protocol", "nats"
    ///      "connection.host", "localhost"
    ///      "connection.port", 4222
    ///    ));
    ///
    ///    await queue.OpenAsync("123");
    ///
    ///    await queue.SendAsync("123", new MessageEnvelope("", "mymessage", "ABC"));
    ///
    ///    var message = await queue.ReceiveAsync("123");
    ///    if (message != null) {
    ///		...
    ///		await queue.CompleteAsync("123", message);
    ///    }
    /// </summary>
    public class NatsMessageQueue : CachedMessageQueue, INatsMessageListener
    {
        private ConfigParams _config;
        private IReferences _references;
        private bool _opened;
        private bool _localConnection;
        private DependencyResolver _dependencyResolver = new DependencyResolver();
        private NatsConnection _connection;
        private bool _subscribed;

        private string _subject;
        private string _queueGroup;

        public NatsMessageQueue(string name = null)
            : base(name, new MessagingCapabilities(false, true, true, true, true, false, false, false, true))
        {
            _dependencyResolver.Put("connection", new Descriptor("pip-services", "connection", "nats", "*", "1.0"));
        }

        /// <summary>
        /// Configure are configures component by passing configuration parameters.
        /// </summary>
        /// <param name="config">Configuration parameters to be set</param>
        public override void Configure(ConfigParams config)
        {
            base.Configure(config);
            _dependencyResolver.Configure(config);

            _config = config;

            _subject = config.GetAsStringWithDefault("topic", _subject);
            _subject = config.GetAsStringWithDefault("subject", _subject);
            _queueGroup = config.GetAsStringWithDefault("group", _queueGroup);
            _queueGroup = config.GetAsStringWithDefault("queue_group", _queueGroup);
        }

        /// <summary>
        /// SetReferences are sets references to dependent components.
        /// </summary>
        /// <param name="references">References to be set</param>
        public override void SetReferences(IReferences references)
        {
            base.SetReferences(references);

            _references = references;

            _dependencyResolver.SetReferences(references);
            // Get connection
            _connection = _dependencyResolver.GetOneOptional<NatsConnection>("connection");

            // Or create a local one
            if (_connection == null)
            {
                _connection = CreateConnection();
                _localConnection = true;
            }
            else
            {
                _localConnection = false;
            }
        }

        private NatsConnection CreateConnection()
        {
            var connection = new NatsConnection();
            if (_config != null)
            {
                connection.Configure(_config);
            }
            if (_references != null)
            {
                connection.SetReferences(_references);
            }
            return connection;
        }

        /// <summary>
        /// Checks if the component is opened.
        /// </summary>
        /// <returns>true if the component has been opened and false otherwise.</returns>
        public override bool IsOpen()
        {
            return _opened;
        }

        /// <summary>
        /// Opens the component with given connection and credential parameters.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public async override Task OpenAsync(string correlationId)
        {
            if (IsOpen())
            {
                return;
            }

            if (_connection == null)
            {
                _connection = CreateConnection();
                _localConnection = true;
            }

            if (_localConnection)
            {
                await _connection.OpenAsync(correlationId);
            }

            if (!_connection.IsOpen())
            {
                throw new InvalidStateException(correlationId, "CONNECT_FAILED", "NATS connection is not opened");
            }

            await base.OpenAsync(correlationId);

            _opened = true;
        }

        /// <summary>
        /// Closes component and frees used resources.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        public override async Task CloseAsync(string correlationId)
        {
            if (!IsOpen())
            {
                return;
            }

            if (_connection == null)
            {
                throw new InvalidStateException(correlationId, "NO_CONNECTION", "NATS connection is missing");
            }

            if (_localConnection)
            {
                await _connection.CloseAsync(correlationId);
            }

            _opened = false;

            await base.CloseAsync(correlationId);
        }

        private string GetSubject()
        {
            return !string.IsNullOrEmpty(_subject) ? _subject : Name;
        }

        protected override async Task SubscribeAsync(string correlationId)
        {
            // Check if already were subscribed
            if (_subscribed)
            {
                return;
            }

            // Subscribe to the subject
            var subject = GetSubject();
            await _connection.SubscribeAsync(subject, _queueGroup, this);

            _subscribed = true;
        }

        protected override async Task UnsubscribeAsync(string correlationId)
        {
            // Check if already were unsubscribed
            if (!_subscribed)
            {
                return;
            }

            // Unsubscribe from the subject
            var subject = GetSubject();
            await _connection.UnsubscribeAsync(subject, _queueGroup, this);

            _subscribed = false;
        }

        private Msg FromMessage(MessageEnvelope message)
        {
            var headers = new MsgHeader();
            headers.Add("message_id", message.MessageId);
            headers.Add("correlation_id", message.CorrelationId);
            headers.Add("message_type", message.MessageType);
            headers.Add("sent_time", StringConverter.ToNullableString(DateTime.UtcNow));

            var subject = !string.IsNullOrEmpty(Name) ? Name : _subject;

            var msg = new Msg(subject, null, headers, message.MessageBuffer);
            return msg;
        }

        private MessageEnvelope ToMessage(Msg msg)
        {
            if (msg == null)
            {
                return null;
            }

            var correlationId = msg.HasHeaders ? msg.Header["correlation_id"] : null;
            var messageType = msg.HasHeaders ? msg.Header["message_type"] : null;
            var messageId = msg.HasHeaders ? msg.Header["message_id"] : null;
            var sentTime = msg.HasHeaders ? DateTimeConverter.ToDateTime(msg.Header["send_time"]) : DateTime.UtcNow;

            MessageEnvelope message = new MessageEnvelope(correlationId, messageType, msg.Data);
            message.MessageId = messageId;
            message.SentTime = sentTime;
            message.Reference = msg;

            return message;
        }

        public void OnMessage(object sender, MsgHandlerEventArgs e)
        {
            // Deserialize message
            var message = ToMessage(e.Message);
            if (message == null)
            {
                _logger.Error(null, null, "Failed to read received message");
                return;
            }

            _counters.IncrementOne("queue." + Name + ".received_messages");
            _logger.Debug(message.CorrelationId, "Received message {0} via {1}", message, Name);

            if (_receiver != null)
            {
                SendMessageToReceiver(_receiver, message);
            }
            else
            {
                lock (_lock)
                {
                    _messages.Enqueue(message);
                }

                _receiveEvent.Set();
            }
        }

        private void SendMessageToReceiver(IMessageReceiver receiver, MessageEnvelope message)
        {
            var correlationId = message?.CorrelationId;

            try
            {
                receiver.ReceiveMessageAsync(message, this).Wait();
            }
            catch (Exception ex)
            {
                _logger.Error(correlationId, ex, "Failed to process the message");
            }
        }

        /// <summary>
        /// Sends a message into the queue.
        /// </summary>
        /// <param name="correlationId">(optional) transaction id to trace execution through call chain.</param>
        /// <param name="message">a message envelop to be sent.</param>
        public override async Task SendAsync(string correlationId, MessageEnvelope message)
        {
            CheckOpen(correlationId);

            _counters.IncrementOne("queue." + Name + ".sent_messages");
            _logger.Debug(message.CorrelationId, "Sent message {0} via %s", message, Name);

            var msg = FromMessage(message);

            var subject = !string.IsNullOrEmpty(Name) ? Name : _subject;

            await _connection.PublishAsync(subject, msg);
        }

        /// <summary>
        /// Renews a lock on a message that makes it invisible from other receivers in
        /// the queue.This method is usually used to extend the message processing time.
        /// </summary>
        /// <param name="message">a message to extend its lock.</param>
        /// <param name="lockTimeout">a locking timeout in milliseconds.</param>
        public override async Task RenewLockAsync(MessageEnvelope message, long lockTimeout)
        {
            // Not supported
            await Task.Delay(0);
        }

        /// <summary>
        /// Returns message into the queue and makes it available for all subscribers to
        /// receive it again.This method is usually used to return a message which could
        /// not be processed at the moment to repeat the attempt.Messages that cause
        /// unrecoverable errors shall be removed permanently or/and send to dead letter queue.
        /// </summary>
        /// <param name="message">a message to return.</param>
        public override async Task AbandonAsync(MessageEnvelope message)
        {
            // Not supported
            await Task.Delay(0);
        }

        /// <summary>
        /// Permanently removes a message from the queue. This method is usually used to
        /// remove the message after successful processing.
        /// </summary>
        /// <param name="message">a message to remove.</param>
        public override async Task CompleteAsync(MessageEnvelope message)
        {
            // Not supported
            await Task.Delay(0);
        }

        /// <summary>
        /// Permanently removes a message from the queue and sends it to dead letter queue.
        /// </summary>
        /// <param name="message">a message to be removed.</param>
        public override async Task MoveToDeadLetterAsync(MessageEnvelope message)
        {
            // Not supported
            await Task.Delay(0);
        }


    }
}
