
namespace DataProcessor.CSVs
{
    public abstract class CSVRowLGE3 : CSVRowLGE
    {
        public CSVRowLGE3(string line) : base(line)
        {
            /*
             * 00 Province 
             * 01 Municipality 
             * 02 Ward
             * 03 VotingDistrict 
             * 04 VotingStationName
             * 05 RegisteredVoters
             * 06 BallotType
             * 07 SpoiltVotes
             * 08 PartyName
             * 09 TotalValidVotes
             * 10 DateGenerated
             */

            string[] rows = Utils.RowsFromLine(line, 11);

            ProvincePk = Utils.RowToProvincePk(rows[0]);
            MunicipalityGeo = Utils.RowToMunicipalityGeo(rows[1]);
            MunicipalityName = Utils.RowToMunicipalityName(rows[1]);
            WardId = Utils.RowToWard(rows[2]);
            VotingDistrictId = Utils.RowToInt(rows[3]);
            VotingStation = Utils.RowToString(rows[4]);
            RegisteredVoters = Utils.RowToInt(rows[5]);
            BallotType = Utils.RowToBallotType(rows[6]);
            SpoiltVotes = Utils.RowToInt(rows[7]);
            PartyName = Utils.RowToPartyName(rows[8]);
            PartyVotes = Utils.RowToInt(rows[9]);
            DateGenerated = Utils.RowToDateTime(rows[10]);
        }

        public DateTime? DateGenerated { get; set; }
    }
}