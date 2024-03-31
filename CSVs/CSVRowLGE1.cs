
using DataProcessor.Tables;

namespace DataProcessor.CSVs
{
    public abstract class CSVRowLGE1 : CSVRowLGE
    {
        public CSVRowLGE1(string line) : base(line)
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
             * 09 Total Votes Cast
             * 10 Valid Votes Cast
             * 11 SpoiltVotes
             */

            string[] rows = Utils.RowsFromLine(line, 12);

            ElectoralEvent = Utils.RowToString(rows[0]);
            ProvincePk = Utils.RowToProvincePk(rows[1]);
            MunicipalityGeo = Utils.RowToMunicipalityGeo(rows[2]);
            MunicipalityName = Utils.RowToMunicipalityName(rows[2]);
            WardId = Utils.RowToWard(rows[03]);
            VotingDistrictId = Utils.RowToString(rows[04]);
            PartyName = Utils.RowToPartyName(rows[05]);
            BallotType = Utils.RowToBallotType(rows[06]);
            RegisteredVoters = Utils.RowToInt(rows[07]);
            VoterTurnout = Utils.RowToInt(rows[08]);
            TotalVotes = Utils.RowToInt(rows[09]);
            PartyVotes = Utils.RowToInt(rows[10]);
            SpoiltVotes = Utils.RowToInt(rows[11]);
        }

        public decimal? VoterTurnout { get; set; }
        public int? TotalVotes { get; set; }

        protected override Ballot GenerateBallot()
        {
            Ballot ballot = base.GenerateBallot();

            ballot.votesTotal = TotalVotes;

            return ballot;
        }
    }
}