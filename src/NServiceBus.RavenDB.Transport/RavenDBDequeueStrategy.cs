﻿using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Features;
using Raven.Client;

namespace NServiceBus.Transports.RavenDB
{
    using System;
    using Unicast.Transport;

    class RavenDBDequeueStrategy : IDequeueMessages, IDisposable
    {
        private Func<TransportMessage, bool> _tryProcessMessage;
        private Action<TransportMessage, Exception> _endProcessMessage;
        private Guid _leaderIdentity; //todo: competing consumer
        private CancellationTokenSource _tokenSource;
        private Address _address;

        public RavenFactory RavenFactory { get; set; }

        public void Init(Address address, 
            TransactionSettings transactionSettings, 
            Func<TransportMessage, bool> tryProcessMessage, 
            Action<TransportMessage, Exception> endProcessMessage)
        {
            //todo: handle all local queues through one loop?
            _address = address;
            _endProcessMessage = endProcessMessage;
            _tryProcessMessage = tryProcessMessage;

            _leaderIdentity = Guid.NewGuid();
        }

        public void Start(int maximumConcurrencyLevel)
        {
            //todo: handle concurrency - prob not threads, just a buffer/queue to keep work supplied
            _tokenSource = new CancellationTokenSource();
            StartWorker();
        }

        void StartWorker()
         {
            var token = _tokenSource.Token;

            Task.Factory
                .StartNew(Work, token, token, TaskCreationOptions.LongRunning, TaskScheduler.Default)
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

        void Work(object o)
        {
            var cancellationToken = (CancellationToken)o;
            while (!cancellationToken.IsCancellationRequested)
            {
                IEnumerable<RavenTransportMessage> messages;

                //todo: introduce leadership for competing consumer
                using (var session = RavenFactory.OpenSession())
                {
                    messages =
                        session.Query<RavenTransportMessage>()
                            .Where(x => x.Destination == _address.Queue)
                            .OrderBy(x => x.SequenceNumber)
                            .Take(10) //todo: concurrency
                            .ToList();
                }

                foreach (var message in messages)
                {
                    using (var session = RavenFactory.OpenSession())
                    {
                        Exception exception = null;
                        session.Store(message); //attach message to session

                        var transportMessage = new TransportMessage(message.Id, message.Headers);
                        transportMessage.Body = message.Body;
                        transportMessage.MessageIntent = message.MessageIntent;
                        try
                        {
                            _tryProcessMessage(transportMessage);
                            session.Delete(message);
                        }
                        catch (Exception e)
                        {
                            exception = e;
                        }

                        _endProcessMessage(transportMessage, exception);
                        session.SaveChanges();
                    }
                }

                Thread.Sleep(50); //bleh; temporary
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

        public void Dispose()
        {
            //Injected todo: ask what "injected" means in sql transport project Dispose
        }
    }
}