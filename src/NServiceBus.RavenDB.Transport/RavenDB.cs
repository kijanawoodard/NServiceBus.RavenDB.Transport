namespace NServiceBus
{
    using Configuration.AdvanceExtensibility;
    using Features;
    using Transports;

    public class RavenDB : TransportDefinition
    {
        internal RavenDB()
        {
            RequireOutboxConsent = false;
        }

        protected override void Configure(BusConfiguration config)
        {
            config.UseSerialization<JsonSerializer>(); //not strictly necessary, but....xml sucks :-)
            config.EnableFeature<RavenDBTransport>();
            config.EnableFeature<MessageDrivenSubscriptions>();
            config.EnableFeature<TimeoutManagerBasedDeferral>();
            config.GetSettings().EnableFeatureByDefault<StorageDrivenPublishing>();
            config.GetSettings().EnableFeatureByDefault<TimeoutManager>();

            config.Transactions().Disable();//.DisableDistributedTransactions().DoNotWrapHandlersExecutionInATransactionScope(); //hmmm; not working get transaction conflicts in dequeue
            //config.DisableDurableMessages();
        }
    }
}