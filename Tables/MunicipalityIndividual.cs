
using System.Linq;

namespace DataProcessor.Tables
{
    [SQLite.Table("municipalities")]
    public class MunicipalityIndividual : Municipality
    {
        public MunicipalityIndividual() { }
        public MunicipalityIndividual(Municipality municipality)
        {
            pk = municipality.pk;
            pkProvince = municipality.pkProvince;
            list_pkWard = municipality.list_pkWard;
            name = municipality.name;
            nameLong = municipality.nameLong;
            miifCategory = municipality.miifCategory;
            category = municipality.category;
            geoLevel = municipality.geoLevel;
            geoCode = municipality.geoCode;
            isDisestablished = municipality.isDisestablished;
            addressEmail = municipality.addressEmail;
            addressPostal = municipality.addressPostal;
            addressStreet = municipality.addressStreet;
            numberPhone = municipality.numberPhone;
            numberFax = municipality.numberFax;
            urlWebsite = municipality.urlWebsite;
            urlLogo = municipality.urlLogo;
            population = municipality.population;
            squareKms = municipality.squareKms;

        }

        [SQLite.PrimaryKey]
        public new int pk { get; set; }
    }
}