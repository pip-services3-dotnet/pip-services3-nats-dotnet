using System.Threading;
using System.Threading.Tasks;
using Xunit;
using PipServices3.Messaging.Queues;

namespace PipServices3.Nats.Queues
{
    public class MessageQueueFixture
    {
        private IMessageQueue _queue;

        public MessageQueueFixture(IMessageQueue queue)
        {
            _queue = queue;
        }

        public async Task TestSendReceiveMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queue.SendAsync(null, envelope1);

            //var count = await _queue.ReadMessageCountAsync();
            //Assert.True(count > 0);

            var envelope2 = await _queue.ReceiveAsync(null, 10000);

            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);
        }

        public async Task TestMoveToDeadMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queue.SendAsync(null, envelope1);

            var envelope2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);

            await _queue.MoveToDeadLetterAsync(envelope2);
        }

        public async Task TestReceiveSendMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");

            ThreadPool.QueueUserWorkItem(async delegate {
                Thread.Sleep(500);
                await _queue.SendAsync(null, envelope1);
            });

            var envelope2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);
        }

        public async Task TestReceiveAndCompleteMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queue.SendAsync(null, envelope1);
            var envelope2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);

            await _queue.CompleteAsync(envelope2);
            //envelope2 = await _queue.PeekAsync();
            //Assert.IsNull(envelope2);
        }

        public async Task TestReceiveAndAbandonMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queue.SendAsync(null, envelope1);
            var envelope2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);

            await _queue.AbandonAsync(envelope2);
            envelope2 = await _queue.ReceiveAsync(null, 10000);
            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);
        }

        public async Task TestSendPeekMessageAsync()
        {
            if (_queue.Capabilities.CanPeek)
            {
                var envelope1 = new MessageEnvelope("123", "Test", "Test message");
                await _queue.SendAsync(null, envelope1);
                await Task.Delay(500);
                var envelope2 = await _queue.PeekAsync(null);
                Assert.NotNull(envelope2);
                Assert.Equal(envelope1.MessageType, envelope2.MessageType);
                Assert.Equal(envelope1.Message, envelope2.Message);
                Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);
            }
        }

        public async Task TestPeekNoMessageAsync()
        {
            if (_queue.Capabilities.CanPeek)
            {
                var envelope = await _queue.PeekAsync(null);
                Assert.Null(envelope);
            }
        }

        public async Task TestMessageCountAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            await _queue.SendAsync(null, envelope1);
            await Task.Delay(1000);
            var count = await _queue.ReadMessageCountAsync();
            Assert.True(count > 0);
        }

        public async Task TestOnMessageAsync()
        {
            var envelope1 = new MessageEnvelope("123", "Test", "Test message");
            MessageEnvelope envelope2 = null;

            _queue.BeginListen(null, async (envelope, queue) => {
                envelope2 = envelope;
                await Task.Delay(0);

                _queue.EndListen(null);
            });

            await _queue.SendAsync(null, envelope1);
            await Task.Delay(1000);

            Assert.NotNull(envelope2);
            Assert.Equal(envelope1.MessageType, envelope2.MessageType);
            Assert.Equal(envelope1.Message, envelope2.Message);
            Assert.Equal(envelope1.CorrelationId, envelope2.CorrelationId);

            await _queue.CloseAsync(null);
        }

    }
}
