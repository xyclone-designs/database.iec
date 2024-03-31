
namespace DataProcessor.CSVs
{
    public abstract class CSVRowNPE1 : CSVRowNPE
    {
        public CSVRowNPE1(string line) : base(line)
        {
            /*
             * 0 ELECTORAL EVENT 
             * 1 PROVINCE 
             * 2 MUNICIPALITY 
             * 3 VOTING DISTRICT 
             * 4 PARTY NAME 
             * 5 REGISTERED VOTERS 
             * 6 % VOTER TURNOUT 
             * 7 VALID VOTES 
             * 8 SPOILT VOTES 
             * 9 TOTAL VOTES CAST
             */

            string[] rows = Utils.RowsFromLine(line, 10);

            ElectoralEvent = Utils.RowToString(rows[0]);
            ProvincePk = Utils.RowToProvincePk(rows[1]);
            MunicipalityGeo = Utils.RowToMunicipalityGeo(rows[2]);
            MunicipalityName = Utils.RowToMunicipalityName(rows[2]);
            VotingDistrictId = Utils.RowToString(rows[3]);
            PartyName = Utils.RowToPartyName(rows[4]);
            RegisteredVoters = Utils.RowToInt(rows[5]);
            VoterTurnout = Utils.RowToDecimal(rows[6]);
            PartyVotes = Utils.RowToInt(rows[7]);
            SpoiltVotes = Utils.RowToInt(rows[8]);
            TotalVotes = Utils.RowToInt(rows[9]);
        }
    }
}