
using System.Linq;

namespace DataProcessor.Tables
{
    [SQLite.Table("provinces")]
    public class ProvinceIndividual : Province
    {
        public ProvinceIndividual() { }
        public ProvinceIndividual(Province province)
        {
            pk = province.pk;
            id = province.id;
            name = province.name;
            population = province.population;
            squareKms = province.squareKms;
            urlCoatOfArms = province.urlCoatOfArms;
            urlWebsite = province.urlWebsite;
        }

        [SQLite.PrimaryKey]
        public new int pk { get; set; }
    }
}