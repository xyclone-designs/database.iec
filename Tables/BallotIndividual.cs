
using System.Linq;

namespace DataProcessor.Tables
{
    [SQLite.Table("ballots")]
    public class BallotIndividual : Ballot
    {
        public BallotIndividual() { }
        public BallotIndividual(Ballot ballot)
        {
            pk = ballot.pk;
            pkElectoralEvent = ballot.pkElectoralEvent;
            pkMunicipality = ballot.pkMunicipality;
            pkProvince = ballot.pkProvince;
            pkVotingDistrict = ballot.pkVotingDistrict;
            pkWard = ballot.pkWard;
            list_pkParty_votes = ballot.list_pkParty_votes;
            type = ballot.type;
            votersRegistered = ballot.votersRegistered;
            votesMEC7 = ballot.votesMEC7;
            votesSection24A = ballot.votesSection24A;
            votesSpecial = ballot.votesSpecial;
            votesSpoilt = ballot.votesSpoilt;
            votesTotal = ballot.votesTotal;
            votesValid = ballot.votesValid;
        }

        [SQLite.PrimaryKey]
        public new int pk { get; set; }
    }
}