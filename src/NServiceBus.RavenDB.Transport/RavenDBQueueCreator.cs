using NServiceBus.Features;
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

            var store = RavenFactory
                .FindDocumentStore(address.Queue);

            store
                .DatabaseCommands.GlobalAdmin
                .EnsureDatabaseExists(address.Queue);

            var commands = store.DatabaseCommands.ForDatabase(address.Queue);
            new RavenTransportMessageIndex().Execute(commands, store.Conventions);

            //todo: log that we are creating db
        }
    }
}