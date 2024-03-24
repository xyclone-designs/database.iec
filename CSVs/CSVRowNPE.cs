
namespace DataProcessor.CSVs
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
                true when Tables.ElectoralEvent.IsNational(ElectoralEvent) => Tables.ElectoralEvent.Types.National,
                true when Tables.ElectoralEvent.IsProvincial(ElectoralEvent) => Tables.ElectoralEvent.Types.Provincial,

                _ => base.GetBallotType(),
            };
        }
        protected override Tables.Ballot GenerateBallot()
        {
            Tables.Ballot ballot = base.GenerateBallot();

            ballot.type = GetBallotType();
            ballot.votesTotal = TotalVotes;

            return ballot;
        }
    }
}