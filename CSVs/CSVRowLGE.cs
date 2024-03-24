﻿
using DataProcessor.Tables;

namespace DataProcessor.CSVs
{
    public abstract class CSVRowLGE : CSVRow
    {
        public CSVRowLGE(string line) : base(line) { }

        public string? VotingStation { get; set; }
        public string? BallotType { get; set; }
        public int? WardId { get; set; }

        public override string? GetBallotType()
        {
            return BallotType ?? base.GetBallotType();
        }
        public override int? GetWardId()
        {
            return WardId ?? base.GetWardId();
        }
        protected override Ballot GenerateBallot()
        {
            Ballot ballot = base.GenerateBallot();

            ballot.type = BallotType;

            return ballot;
        }
    }
}