using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public abstract class CSVRowNPE3 : CSVRowNPE
    {
        public CSVRowNPE3(string line) : base(line)
		{
			/*
             * 0 Province
             * 1 Municipality
             * 2 VD_Number
             * 3 VS_Name
             * 4 Registered_Population
             * 5 Spoilt_Votes
             * 6 Total_Valid_Votes
             * 7 sPartyName
             * 8 Party_Votes
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

            ballot.VotesValid = TotalValidVotes ?? 0;
            ballot.VotesTotal = TotalValidVotes ?? 0 + SpoiltVotes ?? 0;

            return ballot;
        }
    }
}