using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class NPE2014 : CSVRowNPE2
    {
        public NPE2014(string line) : base(line) { }

        public static bool IsElectoralEventNE(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsNational(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2014, electoralEvent.Date);
        }
        public static bool IsElectoralEventPE(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2014, electoralEvent.Date);
        }
    }
}