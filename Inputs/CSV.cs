using System;

using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs
{
    public static partial class CSV
    {
        public abstract class Row (string line)
        {
            public string? Line { get; set; } = line;
            public int? LineNumber { get; set; }
            public string? ElectoralEvent { get; set; }
            public int ElectoralEventPk
            {
                get
                {
                    return Utils.RowToElectoralEvent(ElectoralEvent ?? throw new Exception("No ElectoralEvent")) ?? throw new Exception("Errorneous ElectoralEvent");
                }
            }
            public string? MunicipalityGeo { get; set; }
            public string? MunicipalityName { get; set; }
            public int? PartyVotes { get; set; }
            public int? ProvincePk { get; set; }
            public string? PartyName { get; set; }
            public int? RegisteredVoters { get; set; }
            public int? SpoiltVotes { get; set; }
            public string? VotingDistrictId { get; set; }

            public virtual string? GetBallotType() { return null; }
            public virtual string? GetWardId() { return null; }
            public virtual int? GetTotalVotes() { return null; }

            public Ballot AsBallot(Action<Ballot>? oninit = null)
            {
                Ballot ballot = GenerateBallot();

                oninit?.Invoke(ballot);

                return ballot;
            }

            protected virtual Ballot GenerateBallot()
            {
                return new Ballot
                {
                    PkElectoralEvent = ElectoralEventPk,
                    PkProvince = ProvincePk,

                    VotersRegistered = RegisteredVoters,
                    VotesSpoilt = SpoiltVotes,
                };
            }
        }
        public abstract class RowLGE (string line) : Row(line)
        {
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
        public abstract class RowLGE1 : RowLGE
        {
            public RowLGE1(string line) : base(line)
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

                ballot.VotesTotal = TotalVotes;

                return ballot;
            }
        }
        public abstract class RowLGE2 : RowLGE
        {
            public RowLGE2(string line) : base(line)
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

                ElectoralEvent = Utils.RowToString(rows[0]);
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
        public abstract class RowLGE3 : RowLGE
        {
            public RowLGE3(string line) : base(line)
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
                VotingDistrictId = Utils.RowToString(rows[3]);
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

        public abstract class RowNPE (string line) : Row(line)
        {
            public int? TotalVotes { get; set; }
            public decimal? VoterTurnout { get; set; }

            public override string? GetBallotType()
            {
                return true switch
                {
                    true when XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsNational(ElectoralEvent) => XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.Types.National,
                    true when XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.IsProvincial(ElectoralEvent) => XycloneDesigns.Apis.IEC.Tables.ElectoralEvent.Types.Provincial,

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
        public abstract class RowNPE1 : RowNPE
        {
            public RowNPE1(string line) : base(line)
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
        public abstract class RowNPE2 : RowNPE
        {
            public RowNPE2(string line) : base(line)
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

                ElectoralEvent = Utils.RowToString(rows[0]);
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
        public abstract class RowNPE3 : RowNPE
        {
            public RowNPE3(string line) : base(line)
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
        public abstract class RowNPE4 : RowNPE
        {
            public RowNPE4(string line) : base(line)
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
                 * 9 Generated_Datetime
                 */

                string[] rows = Utils.RowsFromLine(line, 10);

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
                GeneratedDatetime = Utils.RowToDateTime(rows[9]);
            }

            public int? TotalValidVotes { get; set; }
            public string? VotingStation { get; set; }
            public DateTime? GeneratedDatetime { get; set; }

            protected override Ballot GenerateBallot()
            {
                Ballot ballot = base.GenerateBallot();

                ballot.VotesValid = TotalValidVotes ?? 0;
                ballot.VotesTotal = TotalValidVotes ?? 0 + SpoiltVotes ?? 0;

                return ballot;
            }
        }
    }
}