using System;
using System.Collections.Generic;
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
        public string? list_pkParty_seats { get; set; }
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
            if (votesValid is null) votesValid = ballot.votesValid;
            else votesValid += ballot.votesValid ?? 0;

            UpdatePartyVotes(ballot.list_pkParty_votes);
        }
        public void UpdatePartyVotes(string? listPkPartyVotes)
        {
            if (list_pkParty_votes is null) list_pkParty_votes = listPkPartyVotes;
            else if (listPkPartyVotes is not null)
            {
                Dictionary<int, long> pairs = list_pkParty_votes
                    .Split(",")
                    .Select(_pkPair => _pkPair.Split(":"))
                    .ToDictionary(_pkPair => int.Parse(_pkPair[0]), _pkPair => long.Parse(_pkPair[1]));

                foreach (string[] pkVote in listPkPartyVotes
                    .Split(",")
                    .Select(_pkVote => _pkVote.Split(":")))
                {
                    int pk = int.Parse(pkVote[0]);
                    pairs.TryAdd(pk, 0);
                    pairs[pk] += long.Parse(pkVote[1]);
                }

                list_pkParty_votes = string.Join(',', pairs.Select(pair => string.Format("{0}:{1}", pair.Key, pair.Value)));
            }
        }
    }
}