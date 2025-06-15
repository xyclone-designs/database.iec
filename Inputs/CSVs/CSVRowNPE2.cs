using XycloneDesigns.Database.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public abstract class CSVRowNPE2 : CSVRowNPE
    {
        public CSVRowNPE2(string line) : base(line)
        {
            /*
             * 00 ELECTORAL EVENT 
             * 01 PROVINCE 
             * 02 MUNICIPALITY 
             * 03 WARD 
             * 04 VOTING DISTRICT 
             * 05 PARTY NAME 
             * 06 REGISTERED VOTERS 
             * 07 % VOTER TURNOUT 
             * 08 VALID VOTES 
             * 09 SPOILT VOTES 
             * 10 TOTAL VOTES CAST 
             * 11 SECTION 24A VOTES 
             * 12 SPECIAL VOTES"
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
            RegisteredVoters = Utils.RowToInt(rows[6]);
            VoterTurnout = Utils.RowToDecimal(rows[7]);
            PartyVotes = Utils.RowToInt(rows[8]);
            SpoiltVotes = Utils.RowToInt(rows[9]);
            TotalVotes = Utils.RowToInt(rows[10]);
            Section24AVotes = Utils.RowToInt(rows[11]);
            SpecialVotes = Utils.RowToInt(rows[12]);
        }

        public string? VotingStation { get; set; }
        public string? WardId { get; set; }
        public int? Section24AVotes { get; set; }
        public int? SpecialVotes { get; set; }

        public override string? GetWardId()
        {
            return WardId ?? base.GetWardId();
        }
        protected override Ballot GenerateBallot()
        {
            Ballot ballot = base.GenerateBallot();

            ballot.VotesSection24A = Section24AVotes;
            ballot.VotesSpecial = SpecialVotes;

            return ballot;
        }
    }
}