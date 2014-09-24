using System.Threading;
using System.Transactions;
using NServiceBus.Features;

namespace NServiceBus.Transports.RavenDB
{
    using Pipeline;
    using Unicast;

    class RavenDBMessageSender : ISendMessages
    {
        public RavenFactory RavenFactory { get; set; }
        public PipelineExecutor PipelineExecutor { get; set; }
        private static long _messageCount = 0;

        public void Send(TransportMessage message, SendOptions sendOptions)
        {
            Interlocked.Increment(ref _messageCount);
            var ravenTransportMessage = new RavenTransportMessage(message, sendOptions, _messageCount);
            
            //push all outbound messages to the endpoint db
            RavenFactory.UsingSession(message.CorrelationId, session => session.Store(ravenTransportMessage));


            //TODO: find a way to reliably attach to the same session from Dequeue
            //there's an unexpected transacion happening in _tryProcessMessage
            //using (var ts = new TransactionScope(TransactionScopeOption.Suppress))
            /*using (var session = RavenFactory.OpenSession())
            {
                session.Store(transportMessage);
                session.SaveChanges();
            
          //      ts.Complete();
            }*/
        }
    }
}