using System;
using System.Threading;
using System.Transactions;

namespace NServiceBus.Transports.RavenDB
{
    using Pipeline;
    using Raven.Client;
    using System.Collections.Generic;
    using Unicast;

    class RavenDBMessageSender : ISendMessages
    {
        public IDocumentStore DocumentStore { get; set; }
        public string DefaultConnectionString { get; set; }
        public Dictionary<string, string> ConnectionStringCollection { get; set; }
        public PipelineExecutor PipelineExecutor { get; set; }
        
        public void Send(TransportMessage message, SendOptions sendOptions)
        {
            //if (sendOptions.Destination.Queue == "audit") return; //invokes dtc; push this to the side for a moment
            
            using (var ts1 = new TransactionScope(TransactionScopeOption.Suppress))
            using (var session = DocumentStore.OpenSession(sendOptions.Destination.Queue))
            {
                session.Store(new RavenTransportMessage(message, sendOptions));
                session.SaveChanges();
            
                ts1.Complete();
            }
        }
    }

    class RavenTransportMessage
    {
        private static int _staticSeed = Environment.TickCount;
        private static readonly ThreadLocal<Random> _rng = new ThreadLocal<Random>(() =>
        {
            int seed = Interlocked.Increment(ref _staticSeed) & 0x7FFFFFFF;
            return new Random(seed);
        });

        public string Id { get; set; }
        public string CorrelationId { get; set; }
        public Address ReplyToAddress { get; set; }
        public bool Recoverable { get; set; }
        public MessageIntentEnum MessageIntent { get; set; }
        public TimeSpan TimeToBeReceived { get; set; }
        public Dictionary<string, string> Headers { get; set; }
        public byte[] Body { get; set; }
        public byte Ticket { get; set; }
        public DateTime When { get; set; }

        public RavenTransportMessage()
        {
                             
        }

        public RavenTransportMessage(TransportMessage message, SendOptions sendOptions)
        {
            Id = message.Id;
            CorrelationId = message.CorrelationId;
            MessageIntent = message.MessageIntent;
            Recoverable = message.Recoverable;
            TimeToBeReceived = message.TimeToBeReceived;
            Headers = message.Headers;
            Headers[NServiceBus.Headers.ReplyToAddress] = sendOptions.ReplyToAddress.Queue;
            Body = message.Body;
            When = DateTime.UtcNow; //meh

            var bytes = new byte[1];
            _rng.Value.NextBytes(bytes);
            Ticket = bytes[0];
        }
    }
}