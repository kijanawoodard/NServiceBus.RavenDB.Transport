using System.Collections.Generic;
using System.Linq;

namespace NServiceBus.Transports.RavenDB
{
    public class Leadership
    {
        public static string Identifier = "NServiceBus/Transport/Leadership";
        public string Id { get { return Identifier; } }
        public string Leader { get; set; }
        public long Term { get; set; }
        public long Tock { get; set; } //because "Tick" is overloaded ;-)

        public int FollowershipLeeway { get; private set; }
        public int FollowershipInsolence { get; private set; }

        public List<FollowerAssignment> InboundAssignments { get; set; }
        public string OutboundAssignment { get; set; }

        public ClusterStatus Status { get; set; }

        public enum ClusterStatus
        {
            Turmoil,
            Harmony
        }

        public class FollowerAssignment
        {
            public string FollowerId { get; set; }
            public int LowerBound { get; set; }
            public int UpperBound { get; set; }
        }

        public Leadership()
        {
            InboundAssignments = new List<FollowerAssignment>();
            FollowershipLeeway = 5;
            FollowershipInsolence = 50;
        }

        public void UsurpPower(string candidate)
        {
            Leader = candidate;
            AdvanceTerm();
        }

        public bool HasOutboundAssignment(string followerId)
        {
            return OutboundAssignment == followerId;
        }

        public bool DeniedAssignment(string followerId)
        {
            return !HasAssignment(followerId);
        }

        public bool HasAssignment(string followerId)
        {
            return GetAssignment(followerId) != null;
        }

        public FollowerAssignment GetAssignment(string followerId)
        {
            return InboundAssignments.FirstOrDefault(x => x.FollowerId == followerId);
        }

        public bool IsLeader(string id)
        {
            return Leader == id;
        }

        public bool IsHumbleFollower(string id)
        {
            return !IsLeader(id);
        }

        public bool IsObedientFollower(Followership follower)
        {
            return Tock <= follower.Tock + FollowershipLeeway;
        }

        public bool IsDeadFollower(Followership follower)
        {
            return Tock > follower.Tock + FollowershipInsolence;
        }

        public void CommandFollowers(List<Followership> followers)
        {
            Tock++;

            followers = followers
                            .Where(IsObedientFollower)
                            .OrderBy(x => x.FollowerId)
                            .ToList();

            var differences = new HashSet<string>(followers.Select(x => x.FollowerId));
            differences.SymmetricExceptWith(InboundAssignments.Select(x => x.FollowerId));

            if (differences.Any() && Status == ClusterStatus.Harmony)
            {
                AdvanceTerm();
            }

            if (!followers.Any()) return;

            var range = byte.MaxValue / followers.Count; //match MaxValue to TransportMessage ClaimTicket
            var remainder = byte.MaxValue - (range * followers.Count);

            if (differences.Any())
            {
                InboundAssignments =
                followers
                    .Select((follower, i) => new FollowerAssignment
                    {
                        FollowerId = follower.FollowerId,
                        LowerBound = i * range,
                        UpperBound = (i * range) + range + (i == followers.Count - 1 ? remainder : 0)
                    })
                    .ToList();

                OutboundAssignment = followers.First().FollowerId;
            }

            Status = followers.All(x => x.Term == Term)
                ? ClusterStatus.Harmony
                : ClusterStatus.Turmoil;
        }

        private void AdvanceTerm()
        {
            Term++;
            Status = ClusterStatus.Turmoil;
            Tock = 0;
            InboundAssignments.Clear();
        }
    }
}