
namespace DataProcessor.Tables
{
    [SQLite.Table("votingdistricts")]
    public class VotingDistrictIndividual : VotingDistrict
    {
        public VotingDistrictIndividual() { }
        public VotingDistrictIndividual(VotingDistrict votingDistrict)
        {
            id = votingDistrict.id;
            pk = votingDistrict.pk;
            pkWard = votingDistrict.pkWard;
            pkProvince = votingDistrict.pkProvince;
            pkMunicipality = votingDistrict.pkMunicipality;
        }

        [SQLite.PrimaryKey]
        public new int pk { get; set; }
    }
}