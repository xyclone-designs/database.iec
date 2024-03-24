
namespace DataProcessor.CSVs
{
    public class NPE1999 : CSVRowNPE1
    {
        public NPE1999(string line) : base(line) 
        {
            MunicipalityGeo = null;
            MunicipalityName = MunicipalityName?.Trim('"', ' ');
        }

        public static bool IsElectoralEventNE(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsNational(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(1999, electoralEvent.date);
        }
        public static bool IsElectoralEventPE(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsProvincial(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(1999, electoralEvent.date);
        }
    }
}