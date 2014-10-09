using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Features;
using NServiceBus.Logging;
using Raven.Abstractions.Exceptions;

namespace NServiceBus.Transports.RavenDB
{
    using System;
    using Unicast.Transport;

    class RavenDBDequeueStrategy : IDequeueMessages, IDisposable
    {
        static readonly ILog Logger = LogManager.GetLogger(typeof(RavenDBDequeueStrategy));

        private Func<TransportMessage, bool> _tryProcessMessage;
        private Action<TransportMessage, Exception> _endProcessMessage;
        private CancellationTokenSource _tokenSource;
        private Address _address;

        private const int ConsensusHeartbeat = 250;
        private const int MaxMessagesToRead = 1024;
        private readonly BlockingCollection<RavenTransportMessage> _workQueue; 

        public RavenFactory RavenFactory { get; set; }
        public string EndpointName { get; set; }
        public string ProcessIdentity { get; set; }

        public RavenDBDequeueStrategy()
        {
            _workQueue = new BlockingCollection<RavenTransportMessage>();
        }

        public void Init(Address address, 
            TransactionSettings transactionSettings, 
            Func<TransportMessage, bool> tryProcessMessage, 
            Action<TransportMessage, Exception> endProcessMessage)
        {
            //todo: handle all local queues through one loop?
            _address = address;
            _endProcessMessage = endProcessMessage;
            _tryProcessMessage = tryProcessMessage;
        }

        public void Start(int maximumConcurrencyLevel)
        {
            maximumConcurrencyLevel = 10;
            if (_address.Queue != EndpointName) return; //TODO: how to handle retries/timeouts/etc

            _tokenSource = new CancellationTokenSource();
            StartLeader();
            StartFollower();

            for (var i = 0; i < maximumConcurrencyLevel; i++)
            {
                StartWorker();
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

        private void StartLeader()
        {
            var token = _tokenSource.Token;
            Task
                .Run(() => LeaderLoop(token), token)
                .ContinueWith(
                    Continuation(StartLeader),
                    TaskContinuationOptions.OnlyOnFaulted);
        }

        private void StartFollower()
        {
            var token = _tokenSource.Token;
            Task
                .Run(() => FollowerLoop(token), token)
                .ContinueWith(
                    Continuation(StartFollower),
                    TaskContinuationOptions.OnlyOnFaulted);
        }

        private void StartWorker()
        {
            var token = _tokenSource.Token;
            Task
                .Run(() => WorkerLoop(token), token)
                .ContinueWith(
                    Continuation(StartWorker), 
                    TaskContinuationOptions.OnlyOnFaulted);
        }

        void LeaderLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var sleep = ThreadLocalRandom.Next(ConsensusHeartbeat, ConsensusHeartbeat * 2); //add jitter to reduce conflicts
                Thread.Sleep(sleep); 
                try
                {
                    Lead();
                }
                catch (ConcurrencyException)
                {
                    //Failed bid for power
                }
            }
        }

        void FollowerLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Thread.Sleep(ConsensusHeartbeat); 
                Follow();
            }
        }

        void WorkerLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var message = _workQueue.Take(cancellationToken);
                InProgress.TryAdd(message.Id, 0);
                try
                {
                    Work(message);
                }
                finally
                {
                    byte trash;
                    InProgress.TryRemove(message.Id, out trash);   
                }
            }
        }

        private Action<Task> Continuation(Action action)
        {
            return t =>
            {
                t.Exception.Handle(ex =>
                {
                    Logger.Warn("An exception occurred when connecting to the configured RavenDB", ex);
                    //circuitBreaker.Failure(ex);
                    return true;
                });

                if (!_tokenSource.IsCancellationRequested)
                {
                    //if (countdownEvent.TryAddCount())
                    {
                        action();
                    }
                }
            };
        }

        void Lead()
        {
            using (var session = RavenFactory.OpenSession())
            {
                session.Advanced.UseOptimisticConcurrency = true;

                var l = session.Advanced.Lazily.Load<Leadership>(Leadership.Identifier);
                var f = session.Advanced.Lazily.LoadStartingWith<Followership>(Followership.IdPrefix);
                session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                var leadership = l.Value;
                
                var followers = f.Value.ToList();
                var me = followers.FirstOrDefault(x => x.FollowerId == ProcessIdentity);
                if (me == null) return;

                if (me.ConsideringCoup)
                {
                    if (leadership == null)
                    {
                        leadership = new Leadership();
                        session.Store(leadership);
                    }

                    if (leadership.IsHumbleFollower(ProcessIdentity))
                    {
                        leadership.UsurpPower(ProcessIdentity);
                        session.SaveChanges(); //throw if someone else beats me
                        //TODO: Log Won Election!
                    }
                }

                if (leadership == null) return;
                if (leadership.IsHumbleFollower(ProcessIdentity)) return;

                leadership.CommandFollowers(followers);

                var dead = followers.Where(leadership.IsDeadFollower).ToList();
                dead.ForEach(session.Delete);

                session.SaveChanges();
            }
        }

        public static Queue<string> RecentMessages = new FixedSizedQueue<string>(5 * MaxMessagesToRead);
        public static ConcurrentDictionary<string, byte> InProgress = new ConcurrentDictionary<string, byte>();
        void Follow()
        {
            using (var session = RavenFactory.OpenSession())
            {
                var l = session.Advanced.Lazily.Load<Leadership>(Leadership.Identifier);
                var m = session.Advanced.Lazily.Load<Followership>(Followership.FormatId(ProcessIdentity));
                session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                
                var leadership = l.Value ?? new Leadership();
                var me = m.Value;

                if (me == null)
                {
                    me = new Followership(ProcessIdentity);
                    session.Store(me);
                }

                if (leadership.Status == Leadership.ClusterStatus.Turmoil 
                    || leadership.DeniedAssignment(ProcessIdentity))
                {
                    while (_workQueue.Count > 0)
                    {
                        RavenTransportMessage trash;
                        _workQueue.TryTake(out trash); //clear the queue
                    }

                    Thread.Sleep(10); //make sure workers have enough time to get their work in the InProgress collection 

                    me.LastSequenceNumber = 0;
                    RecentMessages.Clear();
                }


                if (leadership.Status == Leadership.ClusterStatus.Harmony
                    && leadership.HasAssignment(ProcessIdentity))
                {
                    var assignment = leadership.GetAssignment(ProcessIdentity);
                
                    var take = MaxMessagesToRead - _workQueue.Count;
                    
                    var messages =
                        session.Query<RavenTransportMessage>()
                            .Where(x => x.Destination == _address.Queue)
                            .Where(x => x.ClaimTicket >= assignment.LowerBound && x.ClaimTicket <= assignment.UpperBound) //todo: tests
                            .Where(x => x.SequenceNumber >= me.LastSequenceNumber)
                            .OrderBy(x => x.SequenceNumber)
                            .Take(take) 
                            .ToList();

                    messages = messages.Where(x => !RecentMessages.Contains(x.Id)).ToList();
                    messages.ForEach(_workQueue.Add);
                    messages.Select(x => x.Id).ToList().ForEach(RecentMessages.Enqueue);

                    if (messages.Any())
                        me.LastSequenceNumber = messages.Last().SequenceNumber;
                }

                me.MakeReportForLeader(leadership, InProgress.Select(x => x.Key).ToList());
                session.SaveChanges();
            }
        }

        void Work(RavenTransportMessage message)
        {
            var transportMessage = new TransportMessage(message.Id, message.Headers);
            transportMessage.Body = message.Body;
            transportMessage.MessageIntent = message.MessageIntent;

            using (var session = RavenFactory.StartSession(transportMessage.Id))
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
                finally
                {
                    RavenFactory.EndSession(transportMessage.Id);    
                }

                _endProcessMessage(transportMessage, exception);
                session.SaveChanges();
            }
        }

        public void Dispose()
        {
            //Injected todo: ask what "injected" means in sql transport project Dispose
        }
    }

    /// <summary> 
    /// source: http://codeblog.jonskeet.uk/2009/11/04/revisiting-randomness/
    /// Convenience class for dealing with randomness. 
    /// </summary> 
    public static class ThreadLocalRandom
    {
        /// <summary> 
        /// Random number generator used to generate seeds, 
        /// which are then used to create new random number 
        /// generators on a per-thread basis. 
        /// </summary> 
        private static readonly Random globalRandom = new Random();
        private static readonly object globalLock = new object();

        /// <summary> 
        /// Random number generator 
        /// </summary> 
        private static readonly ThreadLocal<Random> threadRandom = new ThreadLocal<Random>(NewRandom);

        /// <summary> 
        /// Creates a new instance of Random. The seed is derived 
        /// from a global (static) instance of Random, rather 
        /// than time. 
        /// </summary> 
        public static Random NewRandom()
        {
            lock (globalLock)
            {
                return new Random(globalRandom.Next());
            }
        }

        /// <summary> 
        /// Returns an instance of Random which can be used freely 
        /// within the current thread. 
        /// </summary> 
        public static Random Instance { get { return threadRandom.Value; } }

        /// <summary>See <see cref="Random.Next()" /></summary> 
        public static int Next()
        {
            return Instance.Next();
        }

        /// <summary>See <see cref="Random.Next(int)" /></summary> 
        public static int Next(int maxValue)
        {
            return Instance.Next(maxValue);
        }

        /// <summary>See <see cref="Random.Next(int, int)" /></summary> 
        public static int Next(int minValue, int maxValue)
        {
            return Instance.Next(minValue, maxValue);
        }

        /// <summary>See <see cref="Random.NextDouble()" /></summary> 
        public static double NextDouble()
        {
            return Instance.NextDouble();
        }

        /// <summary>See <see cref="Random.NextBytes(byte[])" /></summary> 
        public static void NextBytes(byte[] buffer)
        {
            Instance.NextBytes(buffer);
        }
    }

    /// <summary>
    /// http://stackoverflow.com/a/10299662/214073
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class FixedSizedQueue<T> : Queue<T> //can probably go with Queue
    {
        public int Size { get; private set; }

        public FixedSizedQueue(int size)
        {
            Size = size;
        }

        public new void Enqueue(T obj)
        {
            base.Enqueue(obj);
            lock (this)
            {
                while (base.Count > Size)
                {
                    base.Dequeue();
                }
            }
        }
    }
}