using Raven.Client;
using Raven.Client.Extensions;

namespace NServiceBus.Transports.RavenDB
{
    class RavenDBQueueCreator : ICreateQueues
    {
        public IDocumentStore DocumentStore { get; set; }
        public void CreateQueueIfNecessary(Address address, string account)
        {
            DocumentStore.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(address.Queue);
            //todo: signal to console that we are creating db
            //question: should everything under the root be a collection instead of a db...prob need Address refactor
        }
    }
}