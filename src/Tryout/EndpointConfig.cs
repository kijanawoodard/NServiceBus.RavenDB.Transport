
using System;
using NServiceBus.Persistence;
using Tryout.Server.Messages.Commands;

namespace Tryout
{
    using NServiceBus;

    /*
		This class configures this endpoint as a Server. More information about how to configure the NServiceBus host
		can be found here: http://particular.net/articles/the-nservicebus-host
	*/
    public class EndpointConfig : IConfigureThisEndpoint, AsA_Server, UsingTransport<RavenDB>
    {
        /*public void Customize(ConfigurationBuilder builder)
        {
            builder.UsePersistence<PLEASE_SELECT_ONE>();
        }*/

        public void Customize(BusConfiguration configuration)
        {
            configuration.Conventions()
                .DefiningCommandsAs(t => t.Namespace != null && t.Namespace.EndsWith("Commands"))
                .DefiningEventsAs(t => t.Namespace != null && t.Namespace.EndsWith("Events"));
            
            configuration.UsePersistence<InMemoryPersistence>();
            configuration.EnableSLAPerformanceCounter(TimeSpan.FromDays(1));
        }
    }

    public class Ok : IWantToRunWhenBusStartsAndStops
    {
        public IBus Bus { get; set; }

        public void Start()
        {
            Bus.Send(new Foo());
            Bus.Send(new Foo());
        }

        public void Stop()
        {
            
        }
    }
}
