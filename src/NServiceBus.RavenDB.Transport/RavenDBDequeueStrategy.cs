using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
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
        private string _processIdentity; //todo: competing consumer
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

            _processIdentity = Guid.NewGuid().ToString();
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
                    Thread.Sleep(2 * 60 * 1000);
                }
            }
        }

        private const int _heartbeatTimeout = 5*1000;
        private int _numberOfMessages = 255;
        private int _term;
        private bool _okToWork;

        public class Leadership
        {
            public static string Identifier = "NServiceBus/Transport/Leadership";
            public string Id { get { return Identifier; } }
            public string Leader { get; set; }
            public int Term { get; set; }
            public DateTime LastUpdate { get; set; }
            public List<FollowerAssignment> Assignments { get; set; }
            public ClusterStatus Status { get; set; }

            internal enum ClusterStatus
            {
                WaitingForNextTerm,
                Running
            }

            public class FollowerAssignment
            {
                public string FollowerId { get; set; }
                public int Lower { get; set; }
                public int Upper { get; set; }
            }

            public Leadership()
            {
                Assignments = new List<FollowerAssignment>();
            }
        }

        public class Follower
        {
            public static string IdPrefix = "NServiceBus/Transport/Follower/";
            public static string FormatId(string followerId)
            {
                return IdPrefix + followerId;
            }

            public string Id { get { return FormatId(FollowerId); } }
            public string FollowerId { get; set; }
            public int Term { get; set; }
            public DateTime LastUpdate { get; set; }
        }

        void Lead()
        {
            /*
             * Claim leadership if leader hasn't updated within heartbeat
             *  Read follower documents Load Starting With
             *      Upadate term, plus heart beat
             *      Set Ranges to work on claim tickets number
             *      Set status to Waiting
             * Wait for all Followers to acknowledge new term
             *      Set status to Running
             *  
             * If Follower is slow or missing remove from list
             *      Start new term
             * If New Follower found
             *      Start new term
             */
            using (var session = RavenFactory.OpenSession())
            {
                session.Advanced.UseOptimisticConcurrency = true;

                var l = session.Advanced.Lazily.Load<Leadership>(Leadership.Identifier);
                var f = session.Advanced.Lazily.LoadStartingWith<Follower>(Follower.IdPrefix);
                session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                var leadership = l.Value;
                var followers = f.Value
                                    .Where(follower => DateTime.UtcNow.Subtract(follower.LastUpdate).Milliseconds < _heartbeatTimeout) //filter out slow followers
                                    .ToList();

                if (leadership == null)
                {
                    leadership = new Leadership();
                    session.Store(leadership);
                }

                if (DateTime.UtcNow.Subtract(leadership.LastUpdate).Milliseconds > _heartbeatTimeout)
                {
                    leadership.Leader = _processIdentity; //claim leadership
                    leadership.Assignments.Clear();
                    leadership.Status = Leadership.ClusterStatus.WaitingForNextTerm;
                    session.SaveChanges(); //throw if someone else beats me
                    //TODO: Log Won Election!
                }

                var iAmLeader = leadership.Leader == _processIdentity;
                if (!iAmLeader) return;

                leadership.LastUpdate = DateTime.UtcNow; //heartbeat

                var difference = new HashSet<string>(followers.Select(x => x.FollowerId));
                difference.SymmetricExceptWith(leadership.Assignments.Select(x => x.FollowerId));

                if (difference.Any())
                {
                    leadership.Term++;
                    leadership.Status = Leadership.ClusterStatus.WaitingForNextTerm;

                    var range = byte.MaxValue/followers.Count; //match to TransportMessage ClaimTicket
                    var remainder = byte.MaxValue - (range*followers.Count);

                    leadership.Assignments =
                        followers
                            .Select((follower, i) => new Leadership.FollowerAssignment
                            {
                                FollowerId = follower.FollowerId,
                                Lower = i * range,
                                Upper = (i * range) + range + (i == followers.Count - 1 ? remainder : 0)
                            })
                            .ToList();
                }

                var followersInLine = followers.All(x => x.Term == leadership.Term);
                if (followersInLine)
                {
                    leadership.Status = Leadership.ClusterStatus.Running;
                }

                session.SaveChanges();
            }
        }

        void Follow()
        {
            /*
             * Read Follower document
             *      Update heartbeat
             * Look at leadership document
             *  If it says Waiting, stop taking work and empty queue
             *      Update Term
             *  If not in the list, stop taking work and empty queue
             *      A new term should start soon with you
             * 
             */

            using (var session = RavenFactory.OpenSession())
            {
                var l = session.Advanced.Lazily.Load<Leadership>(Leadership.Identifier);
                var f = session.Advanced.Lazily.Load<Follower>(Follower.FormatId(_processIdentity));
                session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                
                var leadership = l.Value ?? new Leadership();
                var me = f.Value;

                if (me == null)
                {
                    me = new Follower()
                    {
                        FollowerId = _processIdentity
                    };
                    session.Store(me);
                }

                var assignment = leadership.Assignments.FirstOrDefault(x => x.FollowerId == _processIdentity);
                _okToWork = leadership.Status == Leadership.ClusterStatus.Running
                           && assignment != null;


                me.LastUpdate = DateTime.UtcNow;
                me.Term = leadership.Term;

                session.SaveChanges();
            }
        }

        void Work()
        {
            //todo: introduce leadership for competing consumer
            if (!_okToWork) return;

            IEnumerable<RavenTransportMessage> messages;

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
                var transportMessage = new TransportMessage(message.Id, message.Headers);
                transportMessage.Body = message.Body;
                transportMessage.MessageIntent = message.MessageIntent;

                //using (var ts = new TransactionScope(TransactionScopeOption.Suppress)) //FAILED experiment - there's an unexpected transacion happening in _tryProcessMessage
                using (var session = RavenFactory.StartSession(transportMessage.Id))
                //using (var session = RavenFactory.OpenSession())
                {
                    Exception exception = null;
                    session.Store(message); //attach message to session

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
                    RavenFactory.DisposeSession(transportMessage.Id);

                    try
                    {
                        session.SaveChanges();
                    }
                    catch (Exception e)
                    {
                        exception = e;
                    }
                    //ts.Complete();
                }
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