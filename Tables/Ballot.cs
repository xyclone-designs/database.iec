
using System.Linq;

namespace DataProcessor.Tables
{
    [SQLite.Table("ballots")]
    public class Ballot : ElectionsItem
    {
        private int? _votersRegistered;
        private int? _votesMEC7;
        private int? _votesSection24A;
        private int? _votesSpecial;
        private int? _votesSpoilt;
        private int? _votesTotal;
        private int? _votesValid;

        public int? pkElectoralEvent { get; set; }
        public int? pkMunicipality { get; set; }
        public int? pkProvince { get; set; }
        public int? pkVotingDistrict { get; set; }
        public int? pkWard { get; set; }
        public string? list_pkParty_votes { get; set; }
        public string? type { get; set; }
        public int? votersRegistered 
        {
            set => _votersRegistered = value;
            get => _votersRegistered;
        }
        public int? votesMEC7 
        { 
            set => _votesMEC7 = value;
            get => _votesMEC7;
        }
        public int? votesSection24A 
        { 
            set => _votesSection24A = value;
            get => _votesSection24A;
        }
        public int? votesSpecial 
        { 
            set => _votesSpecial = value;
            get => _votesSpecial;
        }
        public int? votesSpoilt 
        { 
            set => _votesSpoilt = value;
            get => _votesSpoilt ??= true switch
            {
                true when votesValid is not null && _votesTotal is not null => _votesTotal - votesValid,

                _ => new int?()
            };
        }
        public int? votesTotal 
        { 
            set => _votesTotal = value;
            get => _votesTotal ??= true switch
            {
                true when votesValid is not null && _votesSpoilt is null => votesValid,
                true when votesValid is not null && _votesSpoilt is not null => votesValid + _votesSpoilt,

                _ => new int?()
            };
        }
        public int? votesValid 
        { 
            set => _votesValid = value;
            get => _votesValid ??= list_pkParty_votes?
                .Split(",", StringSplitOptions.RemoveEmptyEntries)
                .Select(_ => int.Parse(_.Split(':')[1]))
                .Sum();
        }

        public void UpdateBallot(Ballot ballot)
        {
            if (votersRegistered is null) votersRegistered = ballot.votersRegistered;
            else votersRegistered += ballot.votersRegistered ?? 0;
            if (votesMEC7 is null) votesMEC7 = ballot.votesMEC7;
            else votesMEC7 += ballot.votesMEC7 ?? 0;
            if (votesSection24A is null) votesSection24A = ballot.votesSection24A;
            else votesSection24A += ballot.votesSection24A ?? 0;
            if (votesSpecial is null) votesSpecial = ballot.votesSpecial;
            else votesSpecial += ballot.votesSpecial ?? 0;
            if (votesSpoilt is null) votesSpoilt = ballot.votesSpoilt;
            else votesSpoilt += ballot.votesSpoilt ?? 0;
            if (votesTotal is null) votesTotal = ballot.votesTotal;
            else votesTotal += ballot.votesTotal ?? 0;
        }
    }
}