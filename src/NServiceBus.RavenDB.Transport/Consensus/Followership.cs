using System.Collections.Generic;
using System.Linq;

namespace NServiceBus.Transports.RavenDB
{
    public class Followership
    {
        public static string IdPrefix = "NServiceBus/Transport/Followership/";
        public static string FormatId(string followerId)
        {
            return IdPrefix + followerId;
        }

        public string Id { get { return FormatId(FollowerId); } }
        public string FollowerId { get; private set; }
        
        public long Term { get; set; }
        public long Tock { get; set; }
        public List<string> CurrentWork { get; set; }

        public int LeadershipLeeway { get; private set; }
        public long LeaderTock { get; set; }

        public long LastSequenceNumber { get; set; }

        public Followership(string followerId)
        {
            FollowerId = followerId;
            LeadershipLeeway = 5;
        }

        public void MakeReportForLeader(Leadership leader, List<string> currentWork)
        {
            CurrentWork = currentWork;
            Tock++;            

            if (leader.Term < Term)
            {
                LeaderTock = 0;
                return;
            }
            
            if (leader.Term > Term)
            {
                if (currentWork.Any()) return;
                Term = leader.Term;
                Tock = leader.Tock; 
                LeaderTock = leader.Tock;
            }

            if (leader.Tock > LeaderTock)
            {
                Tock = leader.Tock;
                LeaderTock = leader.Tock;
            }
        }

        public bool ConsideringCoup { get { return Tock > LeaderTock + LeadershipLeeway; } }
        
    }
}