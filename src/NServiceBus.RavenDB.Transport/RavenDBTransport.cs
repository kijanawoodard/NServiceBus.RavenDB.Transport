namespace NServiceBus.Features
{
    using System;
    using System.Linq;
    using Transports;
    using Transports.RavenDB;
    using System.Configuration;


    class RavenDBTransport : ConfigureTransport
    {
        protected override string ExampleConnectionStringForErrorMessage
        {
            get { return "Url=http://localhost:8080"; }
        }
        
        protected override void Configure(FeatureConfigurationContext context, string connectionString)
        {
            if (String.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("RavenDB Transport connection string cannot be empty or null.");
            }
            
            //Until we refactor the whole address system
            Address.IgnoreMachineName();

            //Load all connectionstrings 
            var collection =
                ConfigurationManager
                    .ConnectionStrings
                    .Cast<ConnectionStringSettings>()
                    .Where(x => x.Name.StartsWith("NServiceBus/Transport/"))
                    .ToDictionary(x => x.Name.Replace("NServiceBus/Transport/", String.Empty), y => y.ConnectionString);

            var factory = new RavenFactory(connectionString, collection, context.Settings.EndpointName());
            var transporter = new RavenRemoteQueueTransporter(factory, context.Settings.EndpointName());
            transporter.Start();
            //TODO: call Stop

            var container = context.Container;
            container.ConfigureComponent<RavenDBQueueCreator>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.RavenFactory, factory)
                .ConfigureProperty(p => p.EndpointName, context.Settings.EndpointName());

            container.ConfigureComponent<RavenDBMessageSender>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.RavenFactory, factory);

            container.ConfigureComponent<RavenDBDequeueStrategy>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.RavenFactory, factory);

            //context.Container.ConfigureComponent(b => new SqlServerStorageContext(b.Build<PipelineExecutor>(), connectionString), DependencyLifecycle.InstancePerUnitOfWork);
        }
    }
}