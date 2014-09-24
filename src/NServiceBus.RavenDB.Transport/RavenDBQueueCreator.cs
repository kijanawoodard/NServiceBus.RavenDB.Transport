using NServiceBus.Features;
using Raven.Client;
using Raven.Client.Extensions;

namespace NServiceBus.Transports.RavenDB
{
    class RavenDBQueueCreator : ICreateQueues
    {
        public RavenFactory RavenFactory { get; set; }
        public string EndpointName { get; set; }

        public void CreateQueueIfNecessary(Address address, string account)
        {
            if (address.Queue.StartsWith(EndpointName) && address.Queue != EndpointName) return; //combine all queues for an endpoint in one db

            RavenFactory
                .FindDocumentStore(address.Queue)
                .DatabaseCommands.GlobalAdmin
                .EnsureDatabaseExists(address.Queue);
            //todo: log that we are creating db
        }
    }
}