
using System.Linq;

namespace DataProcessor.Tables
{
    [SQLite.Table("wards")]
    public class WardIndividual : Ward
    {
        public WardIndividual() { }
        public WardIndividual(Ward ward) 
        {
            id = ward.id;
            list_pkVotingDistrict = ward.list_pkVotingDistrict;
            pk = ward.pk;
            pkProvince = ward.pkProvince;
            pkMunicipality = ward.pkMunicipality;
        }

        [SQLite.PrimaryKey]
        public new int pk { get; set; }
    }
}