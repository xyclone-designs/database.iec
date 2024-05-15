using System;

namespace DataProcessor.Tables
{
    [SQLite.Table("electoralevents")]
    public class ElectoralEventIndividual : ElectoralEvent
    {
        public ElectoralEventIndividual() { }
        public ElectoralEventIndividual(ElectoralEvent electoralEvent)
        {
            pk = electoralEvent.pk;
            list_pkBallot = electoralEvent.list_pkBallot;
            list_pkParty_designation_nationalAllocation = electoralEvent.list_pkParty_designation_nationalAllocation;
            list_pkParty_idProvince_provincialAllocation = electoralEvent.list_pkParty_idProvince_provincialAllocation;
            date = electoralEvent.date;
            type = electoralEvent.type;
        }

        [SQLite.PrimaryKey]
        public new int pk { get; set; }
    }
}