
using DataProcessor.Tables;

namespace DataProcessor.CSVs
{
    public abstract class CSVRowNPE3 : CSVRowNPE
    {
        public CSVRowNPE3(string line) : base(line)
        {
            
            /*
             * 0 Province,
             * 1 Municipality 
             * 2 VD Number 
             * 3 VS Name 
             * 4 Registered Population 
             * 5 Spoilt Votes 
             * 6 Total Valid Votes 
             * 7 sPartyName 
             * 8 Party Votes 
             */

            string[] rows = Utils.RowsFromLine(line, 9);

            ProvincePk = Utils.RowToProvincePk(rows[0]);
            MunicipalityGeo = Utils.RowToMunicipalityGeo(rows[1]);
            MunicipalityName = Utils.RowToMunicipalityName(rows[1]);
            VotingDistrictId = Utils.RowToString(rows[2]);
            VotingStation = Utils.RowToString(rows[3]);
            RegisteredVoters = Utils.RowToInt(rows[4]);
            SpoiltVotes = Utils.RowToInt(rows[5]);
            TotalValidVotes = Utils.RowToInt(rows[6]);
            PartyName = Utils.RowToPartyName(rows[7]);
            PartyVotes = Utils.RowToInt(rows[8]);
        }

        public int? TotalValidVotes { get; set; }
        public string? VotingStation { get; set; }

        protected override Ballot GenerateBallot()
        {
            Ballot ballot = base.GenerateBallot();

            ballot.votesValid = TotalValidVotes ?? 0;
            ballot.votesTotal = TotalValidVotes ?? 0 + SpoiltVotes ?? 0;

            return ballot;
        }
    }
}