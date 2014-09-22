using System;
using System.Threading;

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
            using (var session = DocumentStore.OpenSession(sendOptions.Destination.Queue))
            {
                session.Store(new RavenTransportMessage(message));
                session.SaveChanges();
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

        public TransportMessage Message { get; set; } //prob need to to break into primitives to bypass versioning issues
        public byte Ticket { get; set; }

        public RavenTransportMessage()
        {
                             
        }

        public RavenTransportMessage(TransportMessage message)
        {
            Message = message;

            var bytes = new byte[1];
            _rng.Value.NextBytes(bytes);
            Ticket = bytes[0];
        }
    }
}