using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public abstract class CSVRowLGE : CSVRow
    {
        public CSVRowLGE(string line) : base(line) { }

        public string? VotingStation { get; set; }
        public string? BallotType { get; set; }
        public string? WardId { get; set; }

        public override string? GetBallotType()
        {
            return BallotType ?? base.GetBallotType();
        }
        public override string? GetWardId()
        {
            return WardId ?? base.GetWardId();
        }
        protected override Ballot GenerateBallot()
        {
            Ballot ballot = base.GenerateBallot();

            ballot.Type = BallotType;

            return ballot;
        }
    }
}