using System;
using System.Collections.Generic;
using System.Threading;
using NServiceBus.Unicast;

namespace NServiceBus.Transports.RavenDB
{
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
        
        public string Destination { get; set; }
        public bool Outbound { get; set; }

        //we're going to use this to get stable ordering for message consumption
        public long SequenceNumber { get; set; } 
        public byte ClaimTicket { get; set; }
        

        public RavenTransportMessage(TransportMessage message, SendOptions sendOptions, long sequenceNumber)
        {
            Id = string.Format("RavenTransportMessages/{0}/{1}", sendOptions.Destination.Queue, message.Id);
            CorrelationId = message.CorrelationId;
            MessageIntent = message.MessageIntent;
            Recoverable = message.Recoverable;
            TimeToBeReceived = message.TimeToBeReceived;
            
            Headers = message.Headers;
            Headers[NServiceBus.Headers.ReplyToAddress] = sendOptions.ReplyToAddress.Queue;
            
            Body = message.Body;
            
            Destination = sendOptions.Destination.Queue;

            var localMessage = Destination == sendOptions.ReplyToAddress.Queue ||
                               Destination.StartsWith(sendOptions.ReplyToAddress.Queue) && 
                               (Destination.EndsWith("Retries") || 
                                Destination.EndsWith("Timeouts") || 
                                Destination.EndsWith("TimeoutsDispatcher")); //clean up after Address refactor?

            Outbound = !localMessage;

            SequenceNumber = sequenceNumber;
            ClaimTicket = (byte)(sequenceNumber % byte.MaxValue);
        }

        public RavenTransportMessage()
        {

        }
    }
}