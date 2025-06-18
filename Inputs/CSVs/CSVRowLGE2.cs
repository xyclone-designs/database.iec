using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public abstract class CSVRowLGE2 : CSVRowLGE
    {
        public CSVRowLGE2(string line) : base(line)
        {
            /*
             * 00 Electoral Event 
             * 01 Province 
             * 02 Municipality 
             * 03 Ward
             * 04 Voting District
             * 05 Party
             * 06 Ballot Type
             * 07 RegisteredVoters
             * 08 % Voter Turnout
             * 09 MEC7Votes
             * 10 Total Votes Cast
             * 11 Valid Votes Cast
             * 12 SpoiltVotes
             */

            string[] rows = Utils.RowsFromLine(line, 13);

            Electoralevent = Utils.RowToString(rows[0]);
            ProvincePk = Utils.RowToProvincePk(rows[1]);
            MunicipalityGeo = Utils.RowToMunicipalityGeo(rows[2]);
            MunicipalityName = Utils.RowToMunicipalityName(rows[2]);
            WardId = Utils.RowToWard(rows[3]);
            VotingDistrictId = Utils.RowToString(rows[4]);
            VotingStation = Utils.RowToString(rows[4]);
            PartyName = Utils.RowToPartyName(rows[5]);
            BallotType = Utils.RowToBallotType(rows[6]);
            RegisteredVoters = Utils.RowToInt(rows[7]);
            VoterTurnout = Utils.RowToInt(rows[8]);
            MEC7Votes = Utils.RowToInt(rows[9]);
            TotalVotes = Utils.RowToInt(rows[10]);
            PartyVotes = Utils.RowToInt(rows[11]);
            SpoiltVotes = Utils.RowToInt(rows[12]);
        }

        public decimal? VoterTurnout { get; set; }
        public int? MEC7Votes { get; set; }
        public int? TotalVotes { get; set; }

        protected override Ballot GenerateBallot()
        {
            Ballot ballot = base.GenerateBallot();

            ballot.VotesMEC7 = MEC7Votes;
            ballot.VotesTotal = TotalVotes;

            return ballot;
        }
    }
}