using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Features;
using NServiceBus.Logging;

namespace NServiceBus.Transports.RavenDB
{
    using System;
    using Unicast.Transport;

    class RavenDBDequeueStrategy : IDequeueMessages, IDisposable
    {
        static readonly ILog Logger = LogManager.GetLogger(typeof(RavenDBDequeueStrategy));

        private string _processIdentity; 
        
        private Func<TransportMessage, bool> _tryProcessMessage;
        private Action<TransportMessage, Exception> _endProcessMessage;
        private CancellationTokenSource _tokenSource;
        private Address _address;

        private const int ConsensusHeartbeat = 125;
        private const int ConsensusTimeout = ConsensusHeartbeat*50; //TODO: config?
        private const int MaxMessagesToRead = 256;
        private readonly BlockingCollection<RavenTransportMessage> _workQueue; 

        public RavenFactory RavenFactory { get; set; }
        public string EndpointName { get; set; }

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

            _processIdentity = Guid.NewGuid().ToString();
        }

        public void Start(int maximumConcurrencyLevel)
        {
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
               Lead();
                var sleep = ThreadLocalRandom.Next(ConsensusHeartbeat, ConsensusHeartbeat*2); //add jitter to reduce conflicts
                Thread.Sleep(sleep); 
            }
        }

        void FollowerLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
               Follow();
               Thread.Sleep(ConsensusHeartbeat); 
            }
        }

        void WorkerLoop(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var message = _workQueue.Take(cancellationToken);
                InProgress.TryAdd(message.Id, 0);
                Work(message);
                byte trash;
                InProgress.TryRemove(message.Id, out trash);
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
                                    .Where(follower => (DateTime.UtcNow - follower.LastUpdate).TotalMilliseconds < ConsensusTimeout) //filter out slow followers
                                    .ToList();

                if (leadership == null)
                {
                    leadership = new Leadership();
                    session.Store(leadership);
                }

                if ((DateTime.UtcNow - leadership.LastUpdate).TotalMilliseconds > ConsensusTimeout)
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

                var differences = new HashSet<string>(followers.Select(x => x.FollowerId));
                differences.SymmetricExceptWith(leadership.Assignments.Select(x => x.FollowerId));

                if (differences.Any() && followers.Any())
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
                                LowerBound = i * range,
                                UpperBound = (i * range) + range + (i == followers.Count - 1 ? remainder : 0)
                            })
                            .ToList();
                }

                var followersInLine = followers.All(x => x.Term == leadership.Term);
                if (followersInLine && followers.Any())
                {
                    leadership.Status = Leadership.ClusterStatus.Running;
                }

                session.SaveChanges();

                //purge really old followers
                var old = f.Value
                            .Where(follower => (DateTime.UtcNow - follower.LastUpdate).TotalMilliseconds > ConsensusTimeout*5)
                            .ToList();
                old.ForEach(session.Delete);
                session.SaveChanges();
            }
        }

        public static Queue<string> RecentMessages = new FixedSizedQueue<string>(1000);
        public static ConcurrentDictionary<string, byte> InProgress = new ConcurrentDictionary<string, byte>();
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
                    me = new Follower(_processIdentity);
                    session.Store(me);
                }

                me.LastUpdate = DateTime.UtcNow;
                if (me.Term != leadership.Term && InProgress.Count == 0)
                {
                    me.Term = leadership.Term;
                }

                var assignment = leadership.Assignments.FirstOrDefault(x => x.FollowerId == _processIdentity);
                var ok = leadership.Status == Leadership.ClusterStatus.Running
                           && assignment != null;

                if (ok)
                {
                    var take = MaxMessagesToRead - _workQueue.Count;
                    //todo: track where we are so we don't keep querying the same ones
                    var messages =
                        session.Query<RavenTransportMessage>()
                            .Where(x => x.Destination == _address.Queue)
                            .Where(x => x.ClaimTicket > assignment.LowerBound && x.ClaimTicket <= assignment.UpperBound) //todo: tests
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
                else
                {
                    while (_workQueue.Count > 0)
                    {
                        RavenTransportMessage trash;
                        _workQueue.TryTake(out trash); //clear the queue
                    }
                    Thread.Sleep(ConsensusHeartbeat); //make sure workers have enough time to get their work in the InProgress collection //TODO: mechanism to know when all workers are done; might need to make term acceptance conditional on this.
                        
                    me.LastSequenceNumber = 0;
                    RecentMessages.Clear();
                }
                
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

                _endProcessMessage(transportMessage, exception);
                RavenFactory.DisposeSession(transportMessage.Id);

                session.SaveChanges();
            }
        }

        public void Dispose()
        {
            //Injected todo: ask what "injected" means in sql transport project Dispose
        }
    }

    public class Leadership
    {
        public static string Identifier = "NServiceBus/Transport/Leadership";
        public string Id { get { return Identifier; } }
        public string Leader { get; set; }
        public int Term { get; set; }
        public DateTime LastUpdate { get; set; }
        public List<FollowerAssignment> Assignments { get; set; }
        public ClusterStatus Status { get; set; }

        public enum ClusterStatus
        {
            WaitingForNextTerm,
            Running
        }

        public class FollowerAssignment
        {
            public string FollowerId { get; set; }
            public int LowerBound { get; set; }
            public int UpperBound { get; set; }
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
        public string FollowerId { get; private set; }
        public int Term { get; set; }
        public DateTime LastUpdate { get; set; }
        public long LastSequenceNumber { get; set; }

        public Follower(string followerId)
        {
            FollowerId = followerId;
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