
using System;
using Tryout.Server.Messages.Commands;
using Tryout.Server.Messages.Events;

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
            while (true)
            {
                var read = Console.ReadLine();
                if (read == null || read == "q") break;
             
                Bus.Send(new DoSomething());
            }
        }

        public void Stop()
        {
            
        }
    }

    public class What : IHandleMessages<IThinkSomethingHappened>
    {
        public void Handle(IThinkSomethingHappened message)
        {
            Console.WriteLine("Huzzaaaahhhhh: " + message.What);
        }
    }
}
