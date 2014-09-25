using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Features;

namespace NServiceBus.Transports.RavenDB
{
    class RavenRemoteQueueTransporter
    {
        private readonly RavenFactory _ravenFactory;
        private readonly string _endpointName;
        private CancellationTokenSource _tokenSource;

        public RavenRemoteQueueTransporter(RavenFactory ravenFactory, string endpointName)
        {
            _ravenFactory = ravenFactory;
            _endpointName = endpointName;
        }

        public void Start()
        {
            _tokenSource = new CancellationTokenSource();
            StartWorker();
        }

        void StartWorker()
        {
            var token = _tokenSource.Token;

            Task.Factory
                .StartNew(Loop, token, token, TaskCreationOptions.LongRunning, TaskScheduler.Default)
                .ContinueWith(t =>
                {
                    t.Exception.Handle(ex =>
                    {
                        //Logger.Warn("An exception occurred when connecting to the configured SqlServer", ex);
                        //circuitBreaker.Failure(ex);
                        return true;
                    });

                    if (!_tokenSource.IsCancellationRequested)
                    {
                        //if (countdownEvent.TryAddCount())
                        {
                            StartWorker();
                        }
                    }
                }, TaskContinuationOptions.OnlyOnFaulted);
        }

        void Loop(object o)
        {
            var cancellationToken = (CancellationToken)o;
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    Work();
                    Thread.Sleep(150); //bleh; temporary
                }
                catch (Exception)
                {
                    Thread.Sleep(2*60*1000);
                }
            }
        }

        void Work()
        {
            IEnumerable<RavenTransportMessage> outboundMessages;

            //todo: introduce leadership for competing consumer
            using (var session = _ravenFactory.OpenSession())
            {
                outboundMessages =
                    session.Query<RavenTransportMessage>()
                        .Where(x => x.Outbound)
                        .Where(x => x.Destination != _endpointName)
                        .OrderBy(x => x.SequenceNumber)
                        .Take(10) //todo: concurrency
                        .ToList();
            }

            var batches = outboundMessages.GroupBy(x => x.Destination);

            foreach (var batch in batches)
            {
                try
                {
                    var queue = batch.Key;
                    var messages = batch.Select(x => x).ToList();
                    SendMessages(queue, messages);
                    PurgeMessages(messages);
                }
                catch (Exception) //let each batch succeed or fail on its own
                {
                    //log
                    //TODO: if a destination is down, skip it "for a while"
                    Thread.Sleep(5000);
                }
            }
        }

        void SendMessages(string queue, IEnumerable<RavenTransportMessage> messages)
        {
            using (var session = _ravenFactory.OpenRemoteSession(queue))
            {
                var ticket = session.Load<RemoteTransportTicket>(RemoteTransportTicket.FormatId(_endpointName));
                if (ticket == null)
                {
                    ticket = new RemoteTransportTicket { Source = _endpointName };
                    session.Store(ticket);
                }

                var ids = new List<string>();
                var append = false;
                foreach (var message in messages)
                {
                    if (ticket.MessageIds.Contains(message.Id))
                    {
                        append = true; //we didn't get through the clean up phase, add new messages and keep these ids until we clean them up in our db
                    }
                    else
                    {
                        session.Store(message);
                        ids.Add(message.Id);
                    }
                }

                if (append)
                {
                    ticket.MessageIds.AddRange(ids);
                }
                else
                {
                    ticket.MessageIds = ids;
                }

                session.SaveChanges();
            }
        }

        void PurgeMessages(List<RavenTransportMessage> messages)
        {
            using (var session = _ravenFactory.OpenSession())
            {
                messages.ForEach(session.Store);
                messages.ForEach(session.Delete);
                session.SaveChanges();
            }
        }

        public void Stop()
        {
            if (_tokenSource == null)
            {
                return;
            }

            _tokenSource.Cancel();
        }
    }

    public class RemoteTransportTicket
    {
        public static string FormatId(string source) {  return @"NServiceBus/Transport/TransportTicket/" + source; }
        public string Id { get { return FormatId(Source); } }
        public string Source { get; set; }
        public List<string> MessageIds { get; set; }

        public RemoteTransportTicket()
        {
            MessageIds = new List<string>();
        }
    }
}