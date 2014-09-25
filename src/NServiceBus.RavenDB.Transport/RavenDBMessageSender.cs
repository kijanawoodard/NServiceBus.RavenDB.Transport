using System;
using System.Collections.Generic;
using System.Linq;
using NServiceBus.Features;

namespace NServiceBus.Transports.RavenDB
{
    using Pipeline;
    using Unicast;

    class RavenDBMessageSender : ISendMessages
    {
        public RavenFactory RavenFactory { get; set; }
        public PipelineExecutor PipelineExecutor { get; set; }

        public void Send(TransportMessage message, SendOptions sendOptions)
        {
            var counter = DateTime.UtcNow.Ticks;
            var ravenTransportMessage = new RavenTransportMessage(message, sendOptions, counter);
            
            //push all outbound messages to the endpoint db
            RavenFactory.UsingSession(message.CorrelationId, session => session.Store(ravenTransportMessage));


            //TODO: find a way to reliably attach to the same session from Dequeue
            //there's an unexpected transacion happening in _tryProcessMessage
            //using (var ts = new TransactionScope(TransactionScopeOption.Suppress))
            /*using (var session = RavenFactory.OpenSession())
            {
                session.Store(transportMessage);
                session.SaveChanges();
            
          //      ts.Complete();
            }*/
        }
    }

    /// <summary>
    /// Based on http://www.siepman.nl/blog/post/2013/10/28/ID-Sequential-Guid-COMB-Vs-Int-Identity-using-Entity-Framework.aspx
    /// Here's the problem space:
    ///     When we send messages to remote queues, we can't commit a dtc transaction across both the local and remote queues
    ///     That means we wll write the messages to the remote queue in a batch
    ///         but then delete them from the local queue in a subsequent step
    ///     The process can crash before the delete step
    ///         That would mean when we restart the process, we get duplicates
    ///     That's not terrible if all the downstream systems are idempotent and this wouldn't happen "often"
    ///         but let's try and do better
    ///     If we keep a sequence number, we can order by that so that, when we restart after the crash, 
    ///         we will try to send the same messages immediately
    ///     Ok.
    ///     So, what if we store a list of the message ids we transmit in a transaction with the messages?
    ///     With stable ordering, we can look at that list and see if anything we're sneding now is on that list
    ///     If we are sending anything on that list, we know we didn't clean up properly last time
    ///         We can remove matches of the list so we don't send duplicates
    ///             We can still send new messages and append their ids to the list
    ///         Now we can cleanup knowing we haven't sent a duplicate
    ///     The next time through the loop, we'll have fresh messages
    ///         We can now overwrite our message id list
    /// 
    ///     Great.
    /// 
    ///     But what if have multiple consumers for the same logical endpoint that are sending messages to remote queues
    ///     In theory, if all consumers started at zero,
    ///         the ordering wouldn't be so stable.
    ///     A slow consumer could suddenly dump messages with older sequence numbers 
    ///         for a particular remote queue and we wouldn't detect the duplicate
    ///     Now, we are talking about this happening _and_ a crash happening between storing on the remote queue and deleting from the local queue
    ///     We could use the consumer key to sort this out, but it would require more coordination
    ///     
    ///     Instead, lets use an increasing sequence number based on time
    ///     In theory, we could have a case we might get a new message with a number we didn't see before
    ///         But our algorithm is that if there are _any_ duplicates, we will keep our entire list and dedupe
    /// 
    ///     This closes the gap significantly. 
    ///     And, this Transport eschews dtc in favor of idempotency anyway.
    ///         That means any handler will have to be aware of the possibility of duplicate messages.
    /// 
    ///     We could run monitoring on the receiving side to detect duplicates as well
    /// 
    ///     Hmmmm. I think we can just use ticks.
    ///     The only way we could run into problems is if the number of messages for a give tick was larger than our batch size.
    ///     If we read in batches of even 100, with a typical competing cluster of less than 5 nodes,
    ///         I think we'll be fine.
    /// </summary>
    public class SequentialGuid
    {

        public DateTime SequenceStartDate { get; private set; }
        public DateTime SequenceEndDate { get; private set; }

        private const int NumberOfBytes = 6;
        private const int PermutationsOfAByte = 256;
        private readonly long _maximumPermutations = (long)Math.Pow(PermutationsOfAByte, NumberOfBytes);
        private long _lastSequence;

        public SequentialGuid(DateTime sequenceStartDate, DateTime sequenceEndDate)
        {
            SequenceStartDate = sequenceStartDate;
            SequenceEndDate = sequenceEndDate;
        }

        public SequentialGuid()
            : this(new DateTime(2011, 10, 15), new DateTime(2100, 1, 1))
        {
        }

        private static readonly Lazy<SequentialGuid> InstanceField = new Lazy<SequentialGuid>(() => new SequentialGuid());
        internal static SequentialGuid Instance
        {
            get
            {
                return InstanceField.Value;
            }
        }

        public static Guid NewGuid()
        {
            return Instance.GetGuid();
        }

        public TimeSpan TimePerSequence
        {
            get
            {
                var ticksPerSequence = TotalPeriod.Ticks / _maximumPermutations;
                var result = new TimeSpan(ticksPerSequence);
                return result;
            }
        }

        public TimeSpan TotalPeriod
        {
            get
            {
                var result = SequenceEndDate - SequenceStartDate;
                return result;
            }
        }

        private long GetCurrentSequence(DateTime value)
        {
            var ticksUntilNow = value.Ticks - SequenceStartDate.Ticks;
            var result = ((decimal)ticksUntilNow / TotalPeriod.Ticks * _maximumPermutations - 1);
            return (long)result;
        }

        public Guid GetGuid()
        {
            return GetGuid(DateTime.Now);
        }

        private readonly object _synchronizationObject = new object();
        internal Guid GetGuid(DateTime now)
        {
            if (now < SequenceStartDate || now > SequenceEndDate)
            {
                return Guid.NewGuid(); // Outside the range, use regular Guid
            }

            var sequence = GetCurrentSequence(now);
            return GetGuid(sequence);
        }

        internal Guid GetGuid(long sequence)
        {
            lock (_synchronizationObject)
            {
                if (sequence <= _lastSequence)
                {
                    // Prevent double sequence on same server
                    sequence = _lastSequence + 1;
                }
                _lastSequence = sequence;
            }

            var sequenceBytes = GetSequenceBytes(sequence);
            var guidBytes = GetGuidBytes();
            var totalBytes = guidBytes.Concat(sequenceBytes).ToArray();
            var result = new Guid(totalBytes);
            return result;
        }

        private IEnumerable<byte> GetSequenceBytes(long sequence)
        {
            var sequenceBytes = BitConverter.GetBytes(sequence);
            var sequenceBytesLongEnough = sequenceBytes.Concat(new byte[NumberOfBytes]);
            var result = sequenceBytesLongEnough.Take(NumberOfBytes).Reverse();
            return result;
        }

        private IEnumerable<byte> GetGuidBytes()
        {
            var result = Guid.NewGuid().ToByteArray().Take(10).ToArray();
            return result;
        }
    }
}