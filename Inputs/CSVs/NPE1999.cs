using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class NPE1999 : CSVRowNPE1
    {
        public NPE1999(string line) : base(line) 
        {
            MunicipalityGeo = null;
            MunicipalityName = MunicipalityName?.Trim('"', ' ');
        }

        public static bool IsElectoralEventNE(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsNational(electoralEvent.Type) &&
                ElectoralEvent.IsYear(1999, electoralEvent.Date);
        }
        public static bool IsElectoralEventPE(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                ElectoralEvent.IsYear(1999, electoralEvent.Date);
        }
    }
}