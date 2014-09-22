namespace NServiceBus
{
    using Configuration.AdvanceExtensibility;
    using Features;
    using Transports;

    public class RavenDB : TransportDefinition
    {
        internal RavenDB()
        {
            RequireOutboxConsent = true;
        }

        protected override void Configure(BusConfiguration config)
        {
            config.EnableFeature<RavenDBTransport>();
            config.EnableFeature<MessageDrivenSubscriptions>();
            config.EnableFeature<TimeoutManagerBasedDeferral>();
            config.GetSettings().EnableFeatureByDefault<StorageDrivenPublishing>();
            config.GetSettings().EnableFeatureByDefault<TimeoutManager>();
        }
    }
}