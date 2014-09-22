using Raven.Client;

namespace NServiceBus.Transports.RavenDB
{
    using System;
    using Unicast.Transport;

    class RavenDBDequeueStrategy : IDequeueMessages, IDisposable
    {
        private Func<TransportMessage, bool> _tryProcessMessage;
        private Action<TransportMessage, Exception> _endProcessMessage;
        
        public string ConnectionString { get; set; }
        public IDocumentStore DocumentStore { get; set; }

        public void Init(Address address, 
            TransactionSettings transactionSettings, 
            Func<TransportMessage, bool> tryProcessMessage, 
            Action<TransportMessage, Exception> endProcessMessage)
        {
            _endProcessMessage = endProcessMessage;
            _tryProcessMessage = tryProcessMessage;
        }

        public void Start(int maximumConcurrencyLevel)
        {
            
        }

        public void Stop()
        {
            
        }

        public void Dispose()
        {
            
        }
    }
}