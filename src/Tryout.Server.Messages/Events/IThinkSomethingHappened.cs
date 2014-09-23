using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tryout.Server.Messages.Events
{
    public interface IThinkSomethingHappened
    {
        string What { get; set; }
    }
}
