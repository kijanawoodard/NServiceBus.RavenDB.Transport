
using System;
using NServiceBus.Persistence;

namespace Tryout.Server
{
    using NServiceBus;

    /*
		This class configures this endpoint as a Server. More information about how to configure the NServiceBus host
		can be found here: http://particular.net/articles/the-nservicebus-host
	*/
    public class EndpointConfig : IConfigureThisEndpoint, AsA_Server, UsingTransport<RavenDB>
    {
        public void Customize(BusConfiguration configuration)
        {
            configuration.Conventions()
                .DefiningCommandsAs(t => t.Namespace != null && t.Namespace.EndsWith("Commands"))
                .DefiningEventsAs(t => t.Namespace != null && t.Namespace.EndsWith("Events"));
            configuration.UsePersistence<InMemoryPersistence>();
            configuration.EnableSLAPerformanceCounter(TimeSpan.FromDays(1));
        }
    }


}
