using System.IO;
using System.IO.IsolatedStorage;

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

            var endpointName = context.Settings.EndpointName();
            var identity = GetIdentity(endpointName);//Guid.NewGuid().ToString();
            var factory = new RavenFactory(connectionString, collection, endpointName);
            var transporter = new RavenRemoteQueueTransporter(factory, endpointName, identity);
            transporter.Start();
            //TODO: call Stop

            
            var container = context.Container;
            container.ConfigureComponent<RavenDBQueueCreator>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.RavenFactory, factory)
                .ConfigureProperty(p => p.EndpointName, endpointName);

            container.ConfigureComponent<RavenDBMessageSender>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.RavenFactory, factory);

            container.ConfigureComponent<RavenDBDequeueStrategy>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.RavenFactory, factory)
                .ConfigureProperty(p => p.EndpointName, endpointName)
                .ConfigureProperty(p => p.ProcessIdentity, identity);

            //context.Container.ConfigureComponent(b => new SqlServerStorageContext(b.Build<PipelineExecutor>(), connectionString), DependencyLifecycle.InstancePerUnitOfWork);
        }

        private IsolatedStorageFileStream _stream;
        private string GetIdentity(string endpointName)
        {
            var store = IsolatedStorageFile.GetMachineStoreForAssembly();

            for (var i = 0; i < int.MaxValue; i++)
            {
                try
                {
                    var file = string.Format("NServiceBus.Transport.{0}.lock", i);
                    _stream = store.OpenFile(file, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None);
                    var result = string.Format("{0}/{1}/{2}", Environment.MachineName, endpointName, i);
                    return result;
                }
                catch (IOException)
                {
                    
                    
                }
            }

            return Guid.NewGuid().ToString();
        }
    }
}