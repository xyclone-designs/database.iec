
using System.Linq;

namespace DataProcessor.Tables
{
    [SQLite.Table("parties")]
    public class PartyIndividual : Party
    {
        public PartyIndividual() { }
        public PartyIndividual(Party party)
        {
            pk = party.pk;
            list_pkElectoralEvent = party.list_pkElectoralEvent;
            name = party.name;
            abbr = party.abbr;
            dateEstablished = party.dateEstablished;
            dateDisestablished = party.dateDisestablished;
            urlWebsite = party.urlWebsite;
            urlLogo = party.urlLogo;
            color = party.color;
        }

        [SQLite.PrimaryKey]
        public new int pk { get; set; }
    }
}