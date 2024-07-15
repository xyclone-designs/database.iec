
namespace DataProcessor.Tables
{
    [SQLite.Table("electoralevents")]
    public class ElectoralEventIndividual : ElectoralEvent
    {
        public ElectoralEventIndividual() { }
        public ElectoralEventIndividual(ElectoralEvent electoralEvent)
        {
            pk = electoralEvent.pk;
			abbr = electoralEvent.abbr;
			date = electoralEvent.date;
			list_pkBallot = electoralEvent.list_pkBallot;
			list_pkMunicipality_pkParty = electoralEvent.list_pkMunicipality_pkParty;
            list_pkParty_designation_nationalAllocation = electoralEvent.list_pkParty_designation_nationalAllocation;
            list_pkParty_idProvince_provincialAllocation = electoralEvent.list_pkParty_idProvince_provincialAllocation;
			list_pkParty_idProvince_regionalAllocation = electoralEvent.list_pkParty_idProvince_regionalAllocation;
			name = electoralEvent.name;
            type = electoralEvent.type;
        }

        [SQLite.PrimaryKey]
        public new int pk { get; set; }
    }
}