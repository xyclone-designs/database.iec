using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public abstract class CSVRowNPE : CSVRow
    {
        public CSVRowNPE(string line) : base(line) { }

        public int? TotalVotes { get; set; }
        public decimal? VoterTurnout { get; set; }

        public override string? GetBallotType()
        {
            return true switch
            {
                true when ElectoralEvent.IsNational(Electoralevent) => ElectoralEvent.Types.National,
                true when ElectoralEvent.IsProvincial(Electoralevent) => ElectoralEvent.Types.Provincial,

                _ => base.GetBallotType(),
            };
        }
        protected override Ballot GenerateBallot()
        {
            Ballot ballot = base.GenerateBallot();

            ballot.Type = GetBallotType();
            ballot.VotesTotal = TotalVotes;

            return ballot;
        }
    }
}