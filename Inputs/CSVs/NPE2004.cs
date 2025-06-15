using XycloneDesigns.Database.IEC.Tables;

namespace Database.IEC.Inputs.CSVs
{
    public class NPE2004 : CSVRowNPE1
    {
        public NPE2004(string line) : base(line) { }

        public static bool IsElectoralEventNE(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsNational(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2004, electoralEvent.Date);
        }
        public static bool IsElectoralEventPE(ElectoralEvent electoralEvent)
        {
            return
                ElectoralEvent.IsProvincial(electoralEvent.Type) &&
                ElectoralEvent.IsYear(2004, electoralEvent.Date);
        }
    }
}